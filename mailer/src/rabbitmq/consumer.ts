import amqp from "amqplib";

const RABBITMQ_URL = "amqp://rabbitmq:5672";
const EXCHANGE_NAME = "alert.events";
const QUEUE_NAME = "alert.queue";
const ROUTING_KEY = "alert";

let connection: amqp.ChannelModel | null = null;
let channel: amqp.Channel | null = null;

export async function startConsumer(handler: (event: unknown) => Promise<void>): Promise<void> {
	connection = await amqp.connect(RABBITMQ_URL);

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
