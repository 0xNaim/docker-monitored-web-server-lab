import { Counter, Histogram, Registry } from "prom-client";

export const registry = new Registry();

export const alertsReceived = new Counter({
	name: "mailer_alerts_received_total",
	help: "Total number of alerts received",
	registers: [registry]
});

export const alertsProcessed = new Counter({
	name: "mailer_alerts_processed_total",
	help: "Total number of alerts processed successfully",
	registers: [registry]
});

export const alertsFailed = new Counter({
	name: "mailer_alerts_failed_total",
	help: "Total number of alerts that failed",
	registers: [registry]
});

export const alertsRetried = new Counter({
	name: "mailer_alerts_retried_total",
	help: "Total number of alert retries",
	registers: [registry]
});

export const emailsSent = new Counter({
	name: "mailer_emails_sent_total",
	help: "Total number of emails sent successfully",
	registers: [registry]
});

export const emailsFailed = new Counter({
	name: "mailer_emails_failed_total",
	help: "Total number of emails that failed",
	registers: [registry]
});

export const processingDuration = new Histogram({
	name: "mailer_processing_duration_seconds",
	help: "Alert processing duration in seconds",
	registers: [registry],
	buckets: [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5]
});
