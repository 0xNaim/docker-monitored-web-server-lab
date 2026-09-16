import { sendAlertEmail } from "./email/email-sender";
import { connectRedis, redis } from "./queue";
import { withRetry } from "./retry/retry";

const QUEUE_NAME = "alert:queue";

const MAX_EMAIL_ATTEMPTS = 4;
const BASE_RETRY_DELAY_MS = 1000;

interface AlertEvent {
	eventId: string;
	service: string;
	status: "DOWN" | "RECOVERED";
	timestamp: string;
	message: string;
}

async function processAlert(event: AlertEvent): Promise<void> {
	console.log(`[Mailer] Processing event: ${event.eventId}`);

	await withRetry(() => sendAlertEmail(event), {
		maxAttempts: MAX_EMAIL_ATTEMPTS,
		baseDelayMs: BASE_RETRY_DELAY_MS
	});

	console.log(`[Mailer] Email sent: ${event.eventId}`);
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
			try {
				await processAlert(event);
			} catch (error) {
				console.error(`[Mailer] Failed to send alert ${event.eventId}: `, error);
			}
		}
	} catch (error) {
		console.error("Mailer failed: ", error);

		process.exit(1);
	}
}

void start();
