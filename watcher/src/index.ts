import { AlertManager } from "./alert/alert-manager";
import { isCooldownActive, startCooldown } from "./alert/cooldown";
import { isEventProcessed, markEventAsProcessed } from "./alert/idempotency";
import { checkHealth } from "./monitor/health-checker";
import { connectRedis } from "./redis/redis-client";

const TARGET_URL = "http://nginx/health";

const CHECK_INTERVAL_MS = 5000;
const TIMEOUT_MS = 3000;

const FAILURE_THRESHOLD = 3;
const RECOVERY_THRESHOLD = 2;

let consecutiveFailures = 0;
let consecutiveSuccesses = 0;

const alertManager = new AlertManager("nginx");

async function monitor() {
	const result = await checkHealth(TARGET_URL, TIMEOUT_MS);

	const timestamp = new Date().toISOString();

	if (result.healthy) {
		consecutiveFailures = 0;
		consecutiveSuccesses++;

		console.log(`[${timestamp}] UP | status=${result.statusCode} latency=${result.latencyMs}ms`);

		if (consecutiveSuccesses >= RECOVERY_THRESHOLD) {
			const event = await alertManager.handleRecovery();

			if (event) {
				console.log("RECOVERY ALERT EVENT: ", event);
			}

			consecutiveSuccesses = 0;
		}

		return;
	}

	consecutiveSuccesses = 0;
	consecutiveFailures++;

	console.log(`[${timestamp}] DOWN | failure=${consecutiveFailures}/${FAILURE_THRESHOLD}`);

	if (consecutiveFailures >= FAILURE_THRESHOLD) {
		const event = await alertManager.handleDown();

		if (event) {
			const alreadyProcessed = await isEventProcessed(event.eventId);

			if (alreadyProcessed) {
				console.log(`Event already processed: ${event.eventId}`);
				return;
			}

			const cooldownActive = await isCooldownActive(`${event.service}:${event.status}`);

			if (cooldownActive) {
				console.log(`Cooldown active: ${event.service}:${event.status}`);
				return;
			}

			await markEventAsProcessed(event.eventId);
			await startCooldown(`${event.service}:${event.status}`);

			console.log("DOWN ALERT EVENT: ", event);
		}

		consecutiveFailures = 0;
	}
}

async function start() {
	await connectRedis();
	await monitor();

	setInterval(monitor, CHECK_INTERVAL_MS);
}

start();
