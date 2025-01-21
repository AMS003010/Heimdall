import curses
from kafka import KafkaConsumer
import json
import threading
import time
from datetime import datetime, timedelta

# Configuration
KAFKA_TOPICS = ['error_logs', 'warn_logs', 'registration_logs', 'heartbeat_logs']
KAFKA_BROKERS = ['192.168.83.128:9092']
HEARTBEAT_TIMEOUT = 10  # Timeout in seconds to detect missing heartbeats

# Shared state for node monitoring
nodes_last_heartbeat = {}
alerts = []

# Helper function to format alerts
def format_alert(log_entry):
    timestamp = log_entry.get('timestamp', 'UNKNOWN')
    log_level = log_entry.get('log_level', 'UNKNOWN')
    node_id = log_entry.get('node_id', 'UNKNOWN')
    service_name = log_entry.get('service_name', 'UNKNOWN')
    message = log_entry.get('message', 'No message provided')

    alert_message = f"{timestamp} ALERT: {log_level} | Node: {node_id} | Service: {service_name} | Message: {message}"

    if log_level == "ERROR":
        error_details = log_entry.get('error_details', {})
        error_code = error_details.get('error_code', 'UNKNOWN')
        alert_message += f" | Error Code: {error_code}"

    return alert_message

# Function to process Kafka messages
def consume_logs():
    consumer = KafkaConsumer(
        *KAFKA_TOPICS,
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='alert-monitor-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    global alerts

    for message in consumer:
        topic = message.topic
        log_entry = message.value
        node_id = log_entry.get('node_id', 'UNKNOWN')

        if topic == 'heartbeat_logs':
            # Update the last heartbeat time for the node
            nodes_last_heartbeat[node_id] = datetime.now()
        elif topic in ['warn_logs', 'error_logs']:
            # Add an alert for WARN or ERROR logs
            alerts.append(format_alert(log_entry))
        elif topic == 'registration_logs':
            # Log registration events
            service_name = log_entry.get('service_name', 'UNKNOWN')
            alerts.append(f"{datetime.now().isoformat()} INFO: Node {node_id} registered with service {service_name}")

# Function to monitor heartbeat signals
def monitor_heartbeats():
    global alerts
    while True:
        now = datetime.now()
        for node_id, last_heartbeat in list(nodes_last_heartbeat.items()):
            if now - last_heartbeat > timedelta(seconds=HEARTBEAT_TIMEOUT):
                alerts.append(f"{now.isoformat()} ALERT: Missing heartbeat from Node {node_id}")
                del nodes_last_heartbeat[node_id]  # Remove the node to avoid duplicate alerts
        time.sleep(1)

# Terminal UI with colors using curses
def terminal_ui(stdscr):
    global alerts

    # Initialize curses settings
    curses.curs_set(0)
    stdscr.nodelay(1)

    # Initialize color pairs
    curses.start_color()
    curses.init_pair(1, curses.COLOR_GREEN, curses.COLOR_BLACK)  # Active nodes
    curses.init_pair(2, curses.COLOR_YELLOW, curses.COLOR_BLACK)  # WARN
    curses.init_pair(3, curses.COLOR_RED, curses.COLOR_BLACK)  # ERROR
    curses.init_pair(4, curses.COLOR_CYAN, curses.COLOR_BLACK)  # INFO
    curses.init_pair(5, curses.COLOR_MAGENTA, curses.COLOR_BLACK)  # Missing heartbeat

    while True:
        # Clear screen
        stdscr.clear()

        # Display active nodes
        stdscr.addstr(0, 0, "Active Nodes (Last Heartbeat):", curses.color_pair(4))
        for i, (node_id, last_heartbeat) in enumerate(nodes_last_heartbeat.items(), start=1):
            stdscr.addstr(i, 0, f"{node_id}: {last_heartbeat.strftime('%Y-%m-%d %H:%M:%S')}", curses.color_pair(1))

        # Display alerts
        stdscr.addstr(len(nodes_last_heartbeat) + 2, 0, "Alerts:", curses.color_pair(4))
        for i, alert in enumerate(reversed(alerts[-10:]), start=len(nodes_last_heartbeat) + 3):
            if "ERROR" in alert:
                color = curses.color_pair(3)
            elif "WARN" in alert:
                color = curses.color_pair(2)
            elif "Missing heartbeat" in alert:
                color = curses.color_pair(5)
            else:
                color = curses.color_pair(4)

            stdscr.addstr(i, 0, alert, color)

        # Refresh the screen
        stdscr.refresh()
        time.sleep(0.5)

# Main function to start threads and curses UI
if __name__ == "__main__":
    try:
        # Start the Kafka consumer thread
        consumer_thread = threading.Thread(target=consume_logs, daemon=True)
        consumer_thread.start()

        # Start the heartbeat monitor thread
        heartbeat_thread = threading.Thread(target=monitor_heartbeats, daemon=True)
        heartbeat_thread.start()

        # Start the curses terminal UI
        curses.wrapper(terminal_ui)
    except KeyboardInterrupt:
        print("Shutting down gracefully...")
