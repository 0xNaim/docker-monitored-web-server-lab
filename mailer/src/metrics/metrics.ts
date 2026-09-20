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
}

export function incrementAlertsProcessed(): void {
	metrics.alertsProcessed++;
}

export function incrementAlertsFailed(): void {
	metrics.alertsFailed++;
}

export function incrementAlertsRetried(): void {
	metrics.alertsRetried++;
}

export function incrementEmailsSent(): void {
	metrics.emailsSent++;
}

export function incrementEmailsFailed(): void {
	metrics.emailsFailed++;
}

export function recordProcessingDuration(durationMs: number): void {
	metrics.processingDurationMs += durationMs;
	metrics.processingCount++;
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
