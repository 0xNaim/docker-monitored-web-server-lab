interface LogContext {
	[key: string]: unknown;
}

type LogLevel = "info" | "warn" | "error";

const SERVICE_NAME = "mailer";

function log(level: LogLevel, event: string, context: LogContext = {}): void {
	const entry = {
		timestamp: new Date().toISOString(),
		level,
		service: SERVICE_NAME,
		event,
		...context
	};

	console.log(JSON.stringify(entry));
}

export function info(event: string, context?: LogContext): void {
	log("info", event, context);
}

export function warn(event: string, context?: LogContext): void {
	log("warn", event, context);
}

export function error(event: string, context?: LogContext): void {
	log("error", event, context);
}
