import { sendAlertEmail } from "./email/email-sender";
import {
	acknowledgeAlert,
	claimAlert,
	connectRedis,
	moveToDeadLetterQueue,
	redis,
	requeueAlert
} from "./queue";
import { withRetry } from "./retry/retry";

const QUEUE_NAME = "alert:queue";
const PROCESSING_QUEUE_NAME = "alert:processing";
const DLQ_NAME = "alert:dlq";
const MAX_EVENT_RETRIES = 3;
const MAX_EMAIL_ATTEMPTS = 4;
const BASE_RETRY_DELAY_MS = 1000;

interface AlertEvent {
	eventId: string;
	service: string;
	status: "DOWN" | "RECOVERED";
	timestamp: string;
	message: string;
	retryCount: number;
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

				const nextRetryCount = event.retryCount + 1;

				const updatedEvent: AlertEvent = {
					...event,
					retryCount: nextRetryCount
				};

				const updatedPayload = JSON.stringify(updatedEvent);

				if (nextRetryCount >= MAX_EVENT_RETRIES) {
					await moveToDeadLetterQueue(PROCESSING_QUEUE_NAME, DLQ_NAME, rawEvent, updatedPayload);

					console.error(`[Mailer] Moved to DLQ: ${event.eventId}`);

					continue;
				}

				await requeueAlert(PROCESSING_QUEUE_NAME, QUEUE_NAME, rawEvent, updatedPayload);

				console.log(`[Mailer] Requeued: ${event.eventId}`, `retry=${nextRetryCount}`);
			}
		}
	} catch (error) {
		console.error("[Mailer] Fatal error:", error);

		await redis.quit();

		process.exit(1);
	}
}

void start();
