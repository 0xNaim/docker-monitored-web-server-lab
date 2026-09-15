import { redis } from "../redis/redis-client";

const LOCK_TTL_SECONDS = 30;

export interface Lock {
	key: string;
	token: string;
}

export async function acquireLock(lockKey: string): Promise<Lock | null> {
	const key = `alert:lock:${lockKey}`;
	const token = crypto.randomUUID();

	const result = await redis.set(`alert:lock:${lockKey}`, "locked", {
		condition: "NX",
		expiration: { type: "EX", value: LOCK_TTL_SECONDS }
	});

	if (result !== "OK") {
		return null;
	}

	return { key, token };
}

export async function releaseLock(lock: Lock): Promise<void> {
	const currentToken = await redis.get(lock.key);

	if (currentToken === lock.token) {
		await redis.del(lock.key);
	}
}
