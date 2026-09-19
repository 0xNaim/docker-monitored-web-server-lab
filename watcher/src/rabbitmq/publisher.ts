import amqp from "amqplib";

const RABBITMQ_URL = "amqp://rabbitmq:5672";
const EXCHANGE_NAME = "alert.events";

let connection: amqp.ChannelModel | null = null;
let channel: amqp.Channel | null = null;

export async function connectRabbitMQ(): Promise<void> {
	connection = await amqp.connect(RABBITMQ_URL);

	channel = await connection.createChannel();

	await channel.assertExchange(EXCHANGE_NAME, "direct", {
		durable: true
	});

	console.log("[RabbitMQ] Publisher connected");
}

export async function publishAlert(event: unknown): Promise<void> {
	if (!channel) {
		throw new Error("RabbitMQ channel is not initialized");
	}

	const message = Buffer.from(JSON.stringify(event));

	channel.publish(EXCHANGE_NAME, "alert", message, {
		persistent: true,
		contentType: "application/json"
	});

	console.log("[RabbitMQ] Alert published");
}
