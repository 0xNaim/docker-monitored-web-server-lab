import { connectRedis, redis } from "./queue";

const QUEUE_NAME = "alert:queue";

interface AlertEvent {
	eventId: string;
	service: string;
	status: "DOWN" | "RECOVERED";
	timestamp: string;
	message: string;
}

async function processAlert(event: AlertEvent): Promise<void> {
	console.log("=================================");
	console.log("Processing alert");
	console.log("Event ID: ", event.eventId);
	console.log("Service: ", event.service);
	console.log("Status: ", event.status);
	console.log("Message: ", event.message);
	console.log("=================================");
}

async function start(): Promise<void> {
	try {
		await connectRedis();

		console.log("Mailer started");

		while (true) {
			const result = await redis.blPop(QUEUE_NAME, 0); // Wait for an alert event

			if (!result) {
				continue;
			}

			const event = JSON.parse(result.element) as AlertEvent;
			await processAlert(event);
		}
	} catch (error) {
		console.error("Mailer failed: ", error);

		process.exit(1);
	}
}

void start();
