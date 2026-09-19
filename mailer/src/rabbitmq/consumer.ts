import amqp from "amqplib";

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
	connection = await amqp.connect(RABBITMQ_URL);

	channel = await connection.createChannel();

	// Main exchange
	await channel.assertExchange(EXCHANGE_NAME, "direct", {
		durable: true
	});

	// Retry exchange
	await channel.assertExchange(RETRY_EXCHANGE, "direct", {
		durable: true
	});

	// Dead Letter Exchange
	await channel.assertExchange(DLX_EXCHANGE, "direct", {
		durable: true
	});

	//  Main queue
	await channel.assertQueue(QUEUE_NAME, {
		durable: true,
		arguments: {
			"x-dead-letter-exchange": DLX_EXCHANGE,
			"x-dead-letter-routing-key": DLQ_ROUTING_KEY
		}
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

	// Dead Letter Queue
	await channel.assertQueue(DLQ_NAME, {
		durable: true
	});

	await channel.bindQueue(DLQ_NAME, DLX_EXCHANGE, DLQ_ROUTING_KEY);

	// Backpressure
	await channel.prefetch(1);

	console.log("[RabbitMQ] Consumer connected");

	await channel.consume(QUEUE_NAME, async (message) => {
		if (!message) {
			return;
		}

		try {
			const event = JSON.parse(message.content.toString());

			await handler(event);

			channel?.ack(message);

			console.log("[RabbitMQ] ACK");
		} catch (error) {
			console.error("[RabbitMQ] Message processing failed:", error);

			const retryCount = Number(message.properties.headers?.["x-retry-count"] ?? 0);

			if (retryCount >= MAX_RETRIES) {
				console.error("[RabbitMQ] Max retries reached. Sending message to DLQ.");

				channel?.sendToQueue(DLQ_NAME, message.content, {
					persistent: true,
					contentType: "application/json",
					headers: {
						...message.properties.headers,
						"x-retry-count": retryCount
					}
				});

				channel?.ack(message);

				return;
			}

			const nextRetryCount = retryCount + 1;

			channel?.publish(RETRY_EXCHANGE, RETRY_ROUTING_KEY, message.content, {
				persistent: true,
				contentType: "application/json",
				headers: {
					...message.properties.headers,
					"x-retry-count": nextRetryCount
				}
			});

			channel?.ack(message);

			console.log(`[RabbitMQ] Message scheduled for retry ${nextRetryCount}/${MAX_RETRIES}`);
		}
	});
}
