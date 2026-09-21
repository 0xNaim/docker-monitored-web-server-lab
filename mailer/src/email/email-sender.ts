import fs from "node:fs";
import nodemailer from "nodemailer";

function readSecret(path: string): string {
	return fs.readFileSync(path, "utf8").trim();
}

const SMTP_HOST = process.env.SMTP_HOST;
const SMTP_PORT = Number(process.env.SMTP_PORT ?? 587);
const SMTP_USER = process.env.SMTP_USER;
const SMTP_PASSWORD = process.env.SMTP_PASSWORD ?? readSecret("/run/secrets/smtp_password");
const ALERT_EMAIL = process.env.ALERT_EMAIL;

if (!SMTP_HOST || !SMTP_USER || !SMTP_PASSWORD || !ALERT_EMAIL) {
	throw new Error("SMTP configuration is missing");
}

const transporter = nodemailer.createTransport({
	host: SMTP_HOST,
	port: SMTP_PORT,
	secure: SMTP_PORT === 465,
	auth: {
		user: SMTP_USER,
		pass: SMTP_PASSWORD
	}
});

export interface AlertEmail {
	service: string;
	status: "DOWN" | "RECOVERED";
	timestamp: string;
	message: string;
}

export async function sendAlertEmail(alert: AlertEmail): Promise<void> {
	await transporter.sendMail({
		from: SMTP_USER,
		to: ALERT_EMAIL,
		subject: `[${alert.status}] ${alert.service}`,
		text: `
Service: ${alert.service}
Status: ${alert.status}
Time: ${alert.timestamp}

Message:
${alert.message}
		`.trim()
	});
}
