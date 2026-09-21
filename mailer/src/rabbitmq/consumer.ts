import amqp from "amqplib";
import { setRabbitMQHealth } from "../health/health-server";
import {
	claimEvent,
	connectIdempotencyStore,
	getEventStatus,
	markEventProcessed,
	releaseEvent
} from "../idempotency/idempotency-store";
import { error, info, warn } from "../logger/logger";
import {
	incrementAlertsProcessed,
	incrementAlertsReceived,
	incrementAlertsRetried,
	recordProcessingDuration
} from "../metrics/metrics";

const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://rabbitmq:5672";
const EXCHANGE_NAME = "alert.events";
const QUEUE_NAME = "alert.queue";
const ROUTING_KEY = "alert";

const RETRY_EXCHANGE = "alert.retry";
const RETRY_QUEUE = "alert.retry.queue";
const RETRY_ROUTING_KEY = "retry";

const DLX_EXCHANGE = "alert.dlx";
const DLQ_NAME = "alert.dlq";
const DLQ_ROUTING_KEY = "dead";

const RETRY_DELAY_MS = 5000;
const MAX_RETRIES = 3;

// RabbitMQ reconnect configuration
const RECONNECT_BASE_DELAY_MS = 1000;
const RECONNECT_MAX_DELAY_MS = 30000;

let connection: amqp.ChannelModel | null = null;
let channel: amqp.Channel | null = null;

let consumerTag: string | null = null;

let shuttingDown = false;
let reconnecting = false;

function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => {
		setTimeout(resolve, ms);
	});
}

function getReconnectDelay(attempt: number): number {
	return Math.min(RECONNECT_BASE_DELAY_MS * 2 ** (attempt - 1), RECONNECT_MAX_DELAY_MS);
}

// Start the RabbitMQ consumer
export async function startConsumer(handler: (event: unknown) => Promise<void>): Promise<void> {
	await connectIdempotencyStore();
	await connectAndStartConsumer(handler);
}

// Establish RabbitMQ connection and recreate the consumer
async function connectAndStartConsumer(handler: (event: unknown) => Promise<void>): Promise<void> {
	if (shuttingDown) {
		return;
	}

	connection = await amqp.connect(RABBITMQ_URL);

	connection.on("error", (connectionError) => {
		error("rabbitmq_connection_error", {
			error: connectionError instanceof Error ? connectionError.message : String(connectionError)
		});
	});

	// Reconnect when the connection closes unexpectedly.
	connection.on("close", () => {
		error("rabbitmq_connection_closed");
		setRabbitMQHealth(false);

		connection = null;
		channel = null;
		consumerTag = null;

		if (!shuttingDown) {
			void reconnect(handler);
		}
	});

	channel = await connection.createChannel();

	channel.on("error", (channelError) => {
		error("rabbitmq_channel_error", {
			error: channelError instanceof Error ? channelError.message : String(channelError)
		});
	});

	channel.on("close", () => {
		warn("rabbitmq_channel_closed");
	});

	await setupTopology();

	// Process one message at a time.
	await channel.prefetch(1);

	const result = await channel.consume(QUEUE_NAME, async (message) => {
		if (!message || shuttingDown) {
			return;
		}

		await processMessage(message, handler);
	});

	consumerTag = result.consumerTag;

	info("rabbitmq_consumer_connected");
	setRabbitMQHealth(true);
}

// Process a single RabbitMQ message
async function processMessage(
	message: amqp.ConsumeMessage,
	handler: (event: unknown) => Promise<void>
): Promise<void> {
	const startedAt = Date.now();
	incrementAlertsReceived();

	let eventId: string | undefined;

	try {
		const event = JSON.parse(message.content.toString());

		eventId = event.eventId;

		if (!eventId) {
			warn("rabbitmq_message_missing_event_id");

			channel?.ack(message);

			return;
		}

		const status = await getEventStatus(eventId);

		if (status === "PROCESSED") {
			info("rabbitmq_duplicate_event_ignored", {
				eventId
			});

			channel?.ack(message);

			return;
		}

		const claimed = await claimEvent(eventId);

		if (!claimed) {
			warn("rabbitmq_event_already_processing", {
				eventId
			});

			await scheduleRetry(message);

			return;
		}

		await handler(event);

		await markEventProcessed(eventId);

		incrementAlertsProcessed();
		recordProcessingDuration(Date.now() - startedAt);

		channel?.ack(message);

		info("message_processed", {
			eventId
		});
	} catch (processingError) {
		error("rabbitmq_message_processing_failed", {
			eventId,
			error: processingError instanceof Error ? processingError.message : String(processingError)
		});

		// Release the event so a future retry can claim it.
		if (eventId) {
			await releaseEvent(eventId);
		}

		try {
			await scheduleRetry(message);
		} catch (retryError) {
			error("rabbitmq_retry_schedule_failed", {
				eventId,
				error: retryError instanceof Error ? retryError.message : String(retryError)
			});

			if (channel) {
				channel.nack(message, false, true);
			}
		}
	}
}

// Setup RabbitMQ topology
async function setupTopology(): Promise<void> {
	if (!channel) {
		throw new Error("RabbitMQ channel is not initialized");
	}

	// Main exchange
	await channel.assertExchange(EXCHANGE_NAME, "direct", {
		durable: true
	});

	// Retry exchange
	await channel.assertExchange(RETRY_EXCHANGE, "direct", {
		durable: true
	});

	// Dead letter exchange
	await channel.assertExchange(DLX_EXCHANGE, "direct", {
		durable: true
	});

	// Main queue
	await channel.assertQueue(QUEUE_NAME, {
		durable: true
	});

	await channel.bindQueue(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);

	// Retry queue
	await channel.assertQueue(RETRY_QUEUE, {
		durable: true,
		arguments: {
			"x-message-ttl": RETRY_DELAY_MS,
			"x-dead-letter-exchange": EXCHANGE_NAME,
			"x-dead-letter-routing-key": ROUTING_KEY
		}
	});

	await channel.bindQueue(RETRY_QUEUE, RETRY_EXCHANGE, RETRY_ROUTING_KEY);

	// Dead letter queue
	await channel.assertQueue(DLQ_NAME, {
		durable: true
	});

	await channel.bindQueue(DLQ_NAME, DLX_EXCHANGE, DLQ_ROUTING_KEY);
}

// Schedule failed message for retry
async function scheduleRetry(message: amqp.ConsumeMessage): Promise<void> {
	if (!channel) {
		throw new Error("RabbitMQ channel is not initialized");
	}

	const currentRetryCount = Number(message.properties.headers?.["x-retry-count"] ?? 0);

	// Maximum retries reached
	if (currentRetryCount >= MAX_RETRIES) {
		warn("rabbitmq_max_retries_reached", {
			retryCount: currentRetryCount,
			maxRetries: MAX_RETRIES
		});

		channel.publish(DLX_EXCHANGE, DLQ_ROUTING_KEY, message.content, {
			persistent: true,
			contentType: "application/json",
			headers: {
				...message.properties.headers,
				"x-retry-count": currentRetryCount
			}
		});

		channel.ack(message);

		return;
	}

	const nextRetryCount = currentRetryCount + 1;

	// Publish to retry exchange
	channel.publish(RETRY_EXCHANGE, RETRY_ROUTING_KEY, message.content, {
		persistent: true,
		contentType: "application/json",
		headers: {
			...message.properties.headers,
			"x-retry-count": nextRetryCount
		}
	});

	incrementAlertsRetried();

	// Original message is safely copied into the retry queue
	channel.ack(message);

	info("rabbitmq_message_scheduled_for_retry", {
		retryCount: nextRetryCount,
		maxRetries: MAX_RETRIES
	});
}

// Reconnect after RabbitMQ connection loss
async function reconnect(handler: (event: unknown) => Promise<void>): Promise<void> {
	if (shuttingDown || reconnecting) {
		return;
	}

	reconnecting = true;

	let attempt = 1;

	while (!shuttingDown) {
		try {
			const delay = getReconnectDelay(attempt);

			info("rabbitmq_reconnect_attempt", {
				attempt,
				delayMs: delay
			});

			await sleep(delay);

			if (shuttingDown) {
				return;
			}

			await connectAndStartConsumer(handler);

			reconnecting = false;

			info("rabbitmq_reconnected");

			setRabbitMQHealth(true);

			return;
		} catch (reconnectError) {
			error("rabbitmq_reconnect_failed", {
				attempt,
				error: reconnectError instanceof Error ? reconnectError.message : String(reconnectError)
			});

			attempt++;
		}
	}

	reconnecting = false;
}

// Graceful shutdown
export async function shutdownConsumer(): Promise<void> {
	if (shuttingDown) {
		return;
	}

	shuttingDown = true;

	info("rabbitmq_graceful_shutdown_started");

	try {
		// Stop receiving new messages
		if (channel && consumerTag) {
			await channel.cancel(consumerTag);

			info("rabbitmq_consumer_stopped");
		}

		if (channel) {
			await channel.close();

			info("rabbitmq_channel_closed");
		}

		if (connection) {
			await connection.close();

			info("rabbitmq_connection_closed");
		}
	} catch (shutdownError) {
		error("rabbitmq_shutdown_error", {
			error: shutdownError instanceof Error ? shutdownError.message : String(shutdownError)
		});
	} finally {
		channel = null;
		connection = null;
		consumerTag = null;
		reconnecting = false;

		info("rabbitmq_graceful_shutdown_completed");
	}
}
