"""Email notification sent after a message is successfully processed (Send Email box)."""
import smtplib
from email.message import EmailMessage
from datetime import datetime

from config import SENDER_EMAIL, APP_PASSWORD, RECEIVER_EMAIL


def send_success_email(data):
    msg = EmailMessage()
    msg["Subject"] = "Task Executed Successfully"
    msg["From"] = SENDER_EMAIL
    msg["To"] = RECEIVER_EMAIL

    table = data.get("table")
    body = f"""Hello,

Your scheduled task has been executed successfully.

Table : {table}
Execution Time : {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
"""

    if table == "student":
        body += f"""
Student ID : {data.get('id')}
Name       : {data.get('name')}
Age        : {data.get('age')}
Department : {data.get('department')}
"""
    elif table == "employee":
        body += f"""
Employee ID   : {data.get('emp_id')}
Employee Name : {data.get('emp_name')}
Salary        : {data.get('salary')}
Department    : {data.get('department')}
"""

    body += "\nStatus : SUCCESS\n\nRegards,\nRedis Scheduler\n"
    msg.set_content(body)

    def _send_via_starttls():
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=10) as smtp:
            smtp.set_debuglevel(0)
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
            smtp.login(SENDER_EMAIL, APP_PASSWORD)
            smtp.send_message(msg)

    def _send_via_ssl():
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=10) as smtp:
            smtp.set_debuglevel(0)
            smtp.login(SENDER_EMAIL, APP_PASSWORD)
            smtp.send_message(msg)

    try:
        try:
            _send_via_starttls()
            print("Email Sent Successfully (STARTTLS)")
            return
        except Exception as e_start:
            print("Email Error (STARTTLS):", e_start)
            print("Retrying with SSL on port 465...")
        try:
            _send_via_ssl()
            print("Email Sent Successfully (SSL)")
            return
        except Exception as e_ssl:
            print("Email Error (SSL):", e_ssl)
    except Exception as e:
        print("Email Error (unexpected):", e)