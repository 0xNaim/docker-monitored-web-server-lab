import { sendAlertEmail } from "./email/email-sender";
import { shutdownConsumer, startConsumer } from "./rabbitmq/consumer";
import { withRetry } from "./retry/retry";

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
	console.log(`[Mailer] Processing ${event.eventId}`);

	await withRetry(() => sendAlertEmail(event), {
		maxAttempts: MAX_EMAIL_ATTEMPTS,
		baseDelayMs: BASE_RETRY_DELAY_MS
	});

	console.log(`[Mailer] Email sent ${event.eventId}`);
}

async function start(): Promise<void> {
	try {
		await startConsumer(async (rawEvent) => {
			const event = rawEvent as AlertEvent;

			await processAlert(event);
		});

		console.log("[Mailer] Started");
	} catch (error) {
		console.error("[Mailer] Failed to start:", error);

		process.exit(1);
	}
}

let isShuttingDown = false;

async function shutdown(signal: string): Promise<void> {
	if (isShuttingDown) {
		console.log(`[Mailer] Already shutting down, ignoring ${signal}`);
		return;
	}

	isShuttingDown = true;

	console.log(`[Mailer] ${signal} received`);

	try {
		await shutdownConsumer();

		console.log("[Mailer] Shutdown completed");
		process.exit(0);
	} catch (error) {
		console.error("[Mailer] Shutdown failed:", error);

		process.exit(1);
	}
}

process.on("SIGTERM", () => {
	void shutdown("SIGTERM");
});

process.on("SIGINT", () => {
	void shutdown("SIGINT");
});

void start();
