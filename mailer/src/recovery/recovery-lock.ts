import crypto from "node:crypto";
import { redis } from "../queue";

const LOCK_TTL_SECONDS = 30;

export interface RecoveryLock {
	key: string;
	token: string;
}

export async function acquireRecoveryLock(eventId: string): Promise<RecoveryLock | null> {
	const key = `alert:recovery-lock:${eventId}`;

	const token = crypto.randomUUID();

	const result = await redis.set(key, token, {
		NX: true,
		EX: LOCK_TTL_SECONDS
	});

	if (result !== "OK") {
		return null;
	}

	return {
		key,
		token
	};
}

export async function releaseRecoveryLock(lock: RecoveryLock): Promise<void> {
	const currentToken = await redis.get(lock.key);

	if (currentToken === lock.token) {
		await redis.del(lock.key);
	}
}
