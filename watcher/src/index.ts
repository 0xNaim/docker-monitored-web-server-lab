import { AlertManager } from "./alert/alert-manager";
import { isCooldownActive, startCooldown } from "./alert/cooldown";
import { acquireLock, releaseLock } from "./alert/distributed-lock";
import { isEventProcessed, markEventAsProcessed } from "./alert/idempotency";
import { checkHealth } from "./monitor/health-checker";
import { connectRabbitMQ, publishAlert } from "./rabbitmq/publisher";
import { connectRedis } from "./redis/redis-client";

const TARGET_URL = "http://nginx/health";

const SERVICE_NAME = "nginx";

const CHECK_INTERVAL_MS = 5000;
const TIMEOUT_MS = 3000;

const FAILURE_THRESHOLD = 3;
const RECOVERY_THRESHOLD = 2;

let consecutiveFailures = 0;
let consecutiveSuccesses = 0;

const alertManager = new AlertManager(SERVICE_NAME);

async function monitor() {
	const result = await checkHealth(TARGET_URL, TIMEOUT_MS);

	const timestamp = new Date().toISOString();

	// Healthy
	if (result.healthy) {
		consecutiveFailures = 0;
		consecutiveSuccesses++;

		console.log(`[${timestamp}] UP | status=${result.statusCode} latency=${result.latencyMs}ms`);

		if (consecutiveSuccesses >= RECOVERY_THRESHOLD) {
			const event = await alertManager.handleRecovery();

			if (event) {
				console.log("RECOVERY ALERT EVENT: ", event);

				await publishAlert(event);
			}

			consecutiveSuccesses = 0;
		}

		return;
	}

	// Unhealthy
	consecutiveSuccesses = 0;
	consecutiveFailures++;

	console.log(`[${timestamp}] DOWN | failure=${consecutiveFailures}/${FAILURE_THRESHOLD}`);

	// Confirmed down
	if (consecutiveFailures >= FAILURE_THRESHOLD) {
		const lockKey = `${SERVICE_NAME}:DOWN`;
		const lock = await acquireLock(lockKey);

		if (!lock) {
			console.log("Another watcher is handling this alert");

			consecutiveFailures = 0;
			return;
		}

		try {
			const event = await alertManager.handleDown();

			if (event) {
				console.log("ALERT EVENT: ", event);

				const alreadyProcessed = await isEventProcessed(event.eventId);

				if (alreadyProcessed) {
					console.log(`Event already processed: ${event.eventId}`);
					consecutiveFailures = 0;
					return;
				}

				const cooldownActive = await isCooldownActive(`${event.service}:${event.status}`);

				if (cooldownActive) {
					console.log(`Cooldown active: ${event.service}:${event.status}`);
					consecutiveFailures = 0;
					return;
				}

				await markEventAsProcessed(event.eventId);
				await startCooldown(`${event.service}:${event.status}`);
				await publishAlert(event);

        console.log('DOWN ALERT EVENT PUBLISHED: ', event);
			}

			consecutiveFailures = 0;
		} catch (error) {
			console.log("Error handling alert: ", error);
		} finally {
			await releaseLock(lock);

			console.log("Alert lock released");
		}
	}
}

async function start() {
	try {
		// Connect to Redis and start monitoring
		await connectRedis();
    await connectRabbitMQ()
		await monitor();

		// Continue monitoring every 5 seconds
		setInterval(() => {
			void monitor();
		}, CHECK_INTERVAL_MS);
	} catch (error) {
		console.error("[Watcher] Failed to start watcher:", error);

		process.exit(1);
	}
}

void start();
