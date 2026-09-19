import amqp from "amqplib";

const RABBITMQ_URL = "amqp://rabbitmq:5672";
const EXCHANGE_NAME = "alert.events";

const CONNECT_MAX_ATTEMPTS = 10;
const CONNECT_BASE_DELAY_MS = 2000;

let connection: amqp.ChannelModel | null = null;
let channel: amqp.Channel | null = null;

function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => {
		setTimeout(resolve, ms);
	});
}

async function connectWithRetry(): Promise<amqp.ChannelModel> {
	let lastError: unknown;

	for (let attempt = 1; attempt <= CONNECT_MAX_ATTEMPTS; attempt++) {
		try {
			return await amqp.connect(RABBITMQ_URL);
		} catch (error) {
			lastError = error;

			if (attempt === CONNECT_MAX_ATTEMPTS) {
				break;
			}

			const delay = CONNECT_BASE_DELAY_MS * 2 ** (attempt - 1);

			console.error(`[RabbitMQ] Connect attempt ${attempt} failed. Retrying in ${delay} ms...`);

			await sleep(delay);
		}
	}

	throw lastError;
}

export async function connectRabbitMQ(): Promise<void> {
	connection = await connectWithRetry();

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
