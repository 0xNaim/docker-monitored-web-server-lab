import { acknowledgeAlert, claimAlert, connectRedis } from "./queue";
import { redis } from "./queue";
import { sendAlertEmail } from "./email/email-sender";
import { withRetry } from "./retry/retry";

const QUEUE_NAME = "alert:queue";
const PROCESSING_QUEUE_NAME = "alert:processing";

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

		console.log("[Mailer] Started");

		while (true) {
			const rawEvent = await claimAlert(QUEUE_NAME, PROCESSING_QUEUE_NAME);

			if (!rawEvent) {
				await new Promise((resolve) => setTimeout(resolve, 1000));

				continue;
			}

			let event: AlertEvent;

			try {
				event = JSON.parse(rawEvent) as AlertEvent;
			} catch (error) {
				console.error("[Mailer] Invalid event:", error);

				await acknowledgeAlert(PROCESSING_QUEUE_NAME, rawEvent);

				continue;
			}

			try {
				await processAlert(event);

				await acknowledgeAlert(PROCESSING_QUEUE_NAME, rawEvent);

				console.log(`[Mailer] ACK: ${event.eventId}`);
			} catch (error) {
				console.error(`[Mailer] Failed: ${event.eventId}`, error);

				console.log(`[Mailer] Event remains in processing queue`);
			}
		}
	} catch (error) {
		console.error("[Mailer] Fatal error:", error);

		await redis.quit();

		process.exit(1);
	}
}

void start();
