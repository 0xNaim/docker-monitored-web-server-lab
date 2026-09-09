import { redis } from "../redis/redis-client";

const COOLDOWN_SECONDS = 60 * 10; // 10 minutes

export async function isCooldownActive(alertKey: string): Promise<boolean> {
	const key = `alert:cooldown:${alertKey}`;
	const exists = await redis.exists(key);

	return exists === 1;
}

export async function startCooldown(alertKey: string): Promise<void> {
	const key = `alert:cooldown:${alertKey}`;

	await redis.set(key, "1", {
		expiration: {
			type: "EX",
			value: COOLDOWN_SECONDS
		}
	});
}
