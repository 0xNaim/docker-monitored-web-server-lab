import {
	processingDuration,
	alertsFailed as prometheusAlertsFailed,
	alertsProcessed as prometheusAlertsProcessed,
	alertsReceived as prometheusAlertsReceived,
	alertsRetried as prometheusAlertsRetried,
	emailsFailed as prometheusEmailsFailed,
	emailsSent as prometheusEmailsSent
} from "./prometheus";

interface Metrics {
	alertsReceived: number;
	alertsProcessed: number;
	alertsFailed: number;
	alertsRetried: number;
	emailsSent: number;
	emailsFailed: number;
	processingDurationMs: number;
	processingCount: number;
}

const metrics: Metrics = {
	alertsReceived: 0,
	alertsProcessed: 0,
	alertsFailed: 0,
	alertsRetried: 0,
	emailsSent: 0,
	emailsFailed: 0,
	processingDurationMs: 0,
	processingCount: 0
};

export function incrementAlertsReceived(): void {
	metrics.alertsReceived++;

	prometheusAlertsReceived.inc();
}

export function incrementAlertsProcessed(): void {
	metrics.alertsProcessed++;

	prometheusAlertsProcessed.inc();
}

export function incrementAlertsFailed(): void {
	metrics.alertsFailed++;

	prometheusAlertsFailed.inc();
}

export function incrementAlertsRetried(): void {
	metrics.alertsRetried++;

	prometheusAlertsRetried.inc();
}

export function incrementEmailsSent(): void {
	metrics.emailsSent++;

	prometheusEmailsSent.inc();
}

export function incrementEmailsFailed(): void {
	metrics.emailsFailed++;

	prometheusEmailsFailed.inc();
}

export function recordProcessingDuration(durationMs: number): void {
	metrics.processingDurationMs += durationMs;

	metrics.processingCount++;

	processingDuration.observe(durationMs / 1000);
}

export function getMetrics(): Metrics & {
	averageProcessingDurationMs: number;
} {
	const averageProcessingDurationMs =
		metrics.processingCount === 0 ? 0 : metrics.processingDurationMs / metrics.processingCount;

	return {
		...metrics,
		averageProcessingDurationMs
	};
}
