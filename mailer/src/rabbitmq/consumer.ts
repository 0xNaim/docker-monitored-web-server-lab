import amqp from "amqplib";

import { withRetry } from "../retry/retry";

const RABBITMQ_URL = "amqp://rabbitmq:5672";
const EXCHANGE_NAME = "alert.events";
const QUEUE_NAME = "alert.queue";
const ROUTING_KEY = "alert";

const CONNECT_MAX_ATTEMPTS = 10;
const CONNECT_BASE_DELAY_MS = 2000;

let connection: amqp.ChannelModel | null = null;
let channel: amqp.Channel | null = null;

export async function startConsumer(handler: (event: unknown) => Promise<void>): Promise<void> {
	connection = await withRetry(() => amqp.connect(RABBITMQ_URL), {
		maxAttempts: CONNECT_MAX_ATTEMPTS,
		baseDelayMs: CONNECT_BASE_DELAY_MS
	});

	channel = await connection.createChannel();

	await channel.assertExchange(EXCHANGE_NAME, "direct", {
		durable: true
	});

	await channel.assertQueue(QUEUE_NAME, {
		durable: true
	});

	await channel.bindQueue(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);

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

			channel?.nack(message, false, true);
		}
	});
}
