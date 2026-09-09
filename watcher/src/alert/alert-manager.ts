import { AlertState } from "./alert-state";

export interface AlertEvent {
	service: string;
	status: "DOWN" | "RECOVERED";
	timestamp: string;
	message: string;
}

export class AlertManager {
	private state = AlertState.NOT_ALERTED;

	constructor(private readonly serviceName: string) {}

	handleDown(): AlertEvent | null {
		if (this.state === AlertState.ALERTED) {
			return null;
		}

		this.state = AlertState.ALERTED;

		return {
			service: this.serviceName,
			status: "DOWN",
			timestamp: new Date().toISOString(),
			message: `${this.serviceName} is DOWN`
		};
	}

	handleRecovery(): AlertEvent | null {
		if (this.state !== AlertState.ALERTED) {
			return null;
		}

		this.state = AlertState.NOT_ALERTED;

		return {
			service: this.serviceName,
			status: "RECOVERED",
			timestamp: new Date().toISOString(),
			message: `${this.serviceName} has RECOVERED`
		};
	}
}
