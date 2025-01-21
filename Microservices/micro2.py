#!/usr/bin/env python3

import subprocess
import json
import time
import uuid
import random
import threading
from datetime import datetime

# Define service name
SERVICE_NAME = "Service_B"

# Endpoint URL
FLUENTD_ENDPOINT = "http://localhost:9881/script-myapp.log"

# Function to generate a random INFO log
def generate_info_log():
    return {
        "log_id": str(uuid.uuid4()),
        "node_id": "node_2",
        "log_level": "INFO",
        "message_type": "LOG",
        "message": "This is an INFO log message.",
        "service_name": SERVICE_NAME,
        "timestamp": datetime.now().isoformat()
    }

# Function to generate a random WARN log
def generate_warn_log():
    return {
        "log_id": str(uuid.uuid4()),
        "node_id": "node_2",
        "log_level": "WARN",
        "message_type": "LOG",
        "message": "This is a WARN log message.",
        "service_name": SERVICE_NAME,
        "response_time_ms": random.randint(200, 500),
        "threshold_limit_ms": random.randint(100, 200),
        "timestamp": datetime.now().isoformat()
    }

# Function to generate a random ERROR log
def generate_error_log():
    return {
        "log_id": str(uuid.uuid4()),
        "node_id": "node_2",
        "log_level": "ERROR",
        "message_type": "LOG",
        "message": "This is an ERROR log message.",
        "service_name": SERVICE_NAME,
        "error_details": {
            "error_code": random.randint(1000, 9999),
            "error_message": "An error occurred."
        },
        "timestamp": datetime.now().isoformat()
    }

# Function to generate a heartbeat log
def generate_heartbeat_log():
    return {
        "node_id": "node_2",
        "message_type": "HEARTBEAT",
        "status": "UP",
        "timestamp": datetime.now().isoformat()
    }

# Function to generate a registration log
def generate_registration_log():
    return {
        "node_id": "node_2",
        "message_type": "REGISTRATION",
        "service_name": SERVICE_NAME,
        "timestamp": datetime.now().isoformat()
    }

# Function to send a log to Fluentd
def send_log(log):
    log_json = json.dumps(log)
    command = [
        "curl", "-X", "POST",
        "-d", f"json={log_json}",
        FLUENTD_ENDPOINT
    ]
    subprocess.run(command)
    print(log_json)

# Function to send heartbeat logs on a separate thread
def send_heartbeat_logs():
    while True:
        heartbeat_log = generate_heartbeat_log()
        send_log(heartbeat_log)
        time.sleep(5)

# Function to send random logs (INFO, WARN, ERROR)
def send_logs():
    while True:
        log_type = random.choice(["INFO", "WARN", "ERROR"])

        if log_type == "INFO":
            log = generate_info_log()
        elif log_type == "WARN":
            log = generate_warn_log()
        else:  # ERROR
            log = generate_error_log()

        send_log(log)
        time.sleep(5)

if __name__ == "__main__":
    # Generate a unique node ID for this instance
    node_id = str(uuid.uuid4())

    # Send the registration log once
    registration_log = generate_registration_log()
    send_log(registration_log)

    # Start a separate thread for sending heartbeat logs
    heartbeat_thread = threading.Thread(target=send_heartbeat_logs)
    heartbeat_thread.daemon = True  # Ensures the thread exits when the main program exits
    heartbeat_thread.start()

    # Start sending other logs
    send_logs()
