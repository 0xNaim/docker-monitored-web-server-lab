import amqp from "amqplib";
import {
	claimEvent,
	connectIdempotencyStore,
	getEventStatus,
	markEventProcessed,
	releaseEvent
} from "../idempotency/idempotency-store";

const RABBITMQ_URL = "amqp://rabbitmq:5672";

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

let connection: amqp.ChannelModel | null = null;
let channel: amqp.Channel | null = null;

export async function startConsumer(handler: (event: unknown) => Promise<void>): Promise<void> {
	await connectIdempotencyStore();

	connection = await amqp.connect(RABBITMQ_URL);

	channel = await connection.createChannel();

	await channel.assertExchange(EXCHANGE_NAME, "direct", {
		durable: true
	});

	await channel.assertExchange(RETRY_EXCHANGE, "direct", {
		durable: true
	});

	await channel.assertExchange(DLX_EXCHANGE, "direct", {
		durable: true
	});

	await channel.assertQueue(QUEUE_NAME, {
		durable: true
	});

	await channel.bindQueue(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);

	// Retry queue holds messages before sending them back.
	await channel.assertQueue(RETRY_QUEUE, {
		durable: true,
		arguments: {
			"x-message-ttl": RETRY_DELAY_MS,
			"x-dead-letter-exchange": EXCHANGE_NAME,
			"x-dead-letter-routing-key": ROUTING_KEY
		}
	});

	await channel.bindQueue(RETRY_QUEUE, RETRY_EXCHANGE, RETRY_ROUTING_KEY);

	await channel.assertQueue(DLQ_NAME, {
		durable: true
	});

	await channel.bindQueue(DLQ_NAME, DLX_EXCHANGE, DLQ_ROUTING_KEY);

	await channel.prefetch(1);

	console.log("[RabbitMQ] Consumer connected");

	await channel.consume(QUEUE_NAME, async (message) => {
		if (!message) {
			return;
		}

		let eventId: string | undefined;

		try {
			const event = JSON.parse(message.content.toString());

			eventId = event.eventId;

			if (!eventId) {
				console.error("[RabbitMQ] Message has no eventId");

				channel?.ack(message);

				return;
			}

			const status = await getEventStatus(eventId);

			if (status === "PROCESSED") {
				console.log(`[RabbitMQ] Duplicate event ignored: ${eventId}`);

				channel?.ack(message);

				return;
			}

			const claimed = await claimEvent(eventId);

			if (!claimed) {
				console.log(`[RabbitMQ] Event is already being processed: ${eventId}`);

				await scheduleRetry(message);

				return;
			}

			await handler(event);

			await markEventProcessed(eventId);

			channel?.ack(message);

			console.log(`[RabbitMQ] Event processed successfully: ${eventId}`);
		} catch (error) {
			console.error("[RabbitMQ] Message processing failed:", error);

			if (eventId) {
				await releaseEvent(eventId);
			}

			try {
				await scheduleRetry(message);
			} catch (retryError) {
				console.error("[RabbitMQ] Failed to schedule retry:", retryError);

				channel?.nack(message, false, true);
			}
		}
	});
}

async function scheduleRetry(message: amqp.ConsumeMessage): Promise<void> {
	if (!channel) {
		throw new Error("RabbitMQ channel is not initialized");
	}

	const currentRetryCount = Number(message.properties.headers?.["x-retry-count"] ?? 0);

	if (currentRetryCount >= MAX_RETRIES) {
		console.error(`[RabbitMQ] Max retries reached. Sending message to DLQ.`);

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

	channel.publish(RETRY_EXCHANGE, RETRY_ROUTING_KEY, message.content, {
		persistent: true,
		contentType: "application/json",
		headers: {
			...message.properties.headers,
			"x-retry-count": nextRetryCount
		}
	});

	channel.ack(message);

	console.log(`[RabbitMQ] Message scheduled for retry ${nextRetryCount}/${MAX_RETRIES}`);
}
