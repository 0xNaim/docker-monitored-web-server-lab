import { ALERT_QUEUE, PROCESSING_METADATA, PROCESSING_QUEUE, redis } from "../queue";

const VISIBILITY_TIMEOUT_MS = 30_000;
const RECOVERY_INTERVAL_MS = 5_000;

interface AlertEvent {
	eventId: string;
	service: string;
	status: "DOWN" | "RECOVERED";
	timestamp: string;
	message: string;
	retryCount: number;
}

async function recoverStuckEvents(): Promise<void> {
	const processingEvents = await redis.lRange(PROCESSING_QUEUE, 0, -1);

	const now = Date.now();

	for (const rawEvent of processingEvents) {
		let event: AlertEvent;

		try {
			event = JSON.parse(rawEvent) as AlertEvent;
		} catch {
			console.error("[Recovery] Invalid event:", rawEvent);

			continue;
		}

		const claimedAt = await redis.hGet(PROCESSING_METADATA, event.eventId);

		if (!claimedAt) {
			continue;
		}

		const age = now - Number(claimedAt);

		if (age < VISIBILITY_TIMEOUT_MS) {
			continue;
		}

		console.log(`[Recovery] Recovering event ${event.eventId}`);

		await redis.lRem(PROCESSING_QUEUE, 1, rawEvent);

		await redis.hDel(PROCESSING_METADATA, event.eventId);

		await redis.lPush(ALERT_QUEUE, rawEvent);

		console.log(`[Recovery] Requeued ${event.eventId}`);
	}
}

export function startRecoveryWorker(): void {
	console.log("[Recovery] Worker started");

	setInterval(() => {
		void recoverStuckEvents();
	}, RECOVERY_INTERVAL_MS);
}
