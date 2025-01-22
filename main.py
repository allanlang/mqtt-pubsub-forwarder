import paho.mqtt.client as mqtt
from google.cloud import pubsub_v1
import ssl
import logging
import json
import os
import signal
import threading

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# MQTT Configuration
MQTT_BROKER = os.getenv("MQTT_BROKER", "mqtt.example.com")
MQTT_PORT = int(os.getenv("MQTT_PORT", 8883))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "your/mqtt/topic")
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "your_username")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "your_password")
MQTT_TLS_CERT = os.getenv("MQTT_TLS_CERT", "/path/to/your/ca_certificate.pem")

# Google Pub/Sub Configuration
PROJECT_ID = os.getenv("PROJECT_ID", "your-gcp-project-id")
PUBSUB_TOPIC = os.getenv("PUBSUB_TOPIC", "your-pubsub-topic")

# Initialize Google Pub/Sub publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC)

# Thread event for graceful shutdown
shutdown_event = threading.Event()

def on_connect(client, userdata, flags, rc):
    """Callback for when the client receives a CONNACK response from the server."""
    if rc == 0:
        logging.info("Connected to MQTT broker.")
        client.subscribe(MQTT_TOPIC)
    else:
        logging.error(f"Failed to connect to MQTT broker. Return code: {rc}")

def on_message(client, userdata, msg):
    """Callback for when a PUBLISH message is received from the server."""
    try:
        logging.info(f"Message received from MQTT: {msg.topic}")
        # Publish the message to Google Pub/Sub
        future = publisher.publish(topic_path, msg.payload, mqtt_topic=msg.topic)
        future.result()  # Wait for the publish to complete
        logging.info("Message forwarded to Google Pub/Sub.")
    except Exception as e:
        logging.error(f"Failed to forward message to Pub/Sub: {e}")

def handle_signal(sig, frame):
    """Signal handler to initiate graceful shutdown."""
    logging.info("Shutdown signal received. Terminating...")
    shutdown_event.set()

def main():
    """Main function to initialize MQTT client and start the loop."""
    try:
        # Configure MQTT client
        client = mqtt.Client()
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

        # Configure TLS/SSL
        client.tls_set(ca_certs=MQTT_TLS_CERT, certfile=None, keyfile=None, cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2)

        # Set callback functions
        client.on_connect = on_connect
        client.on_message = on_message

        # Connect to MQTT broker
        logging.info("Connecting to MQTT broker...")
        client.connect(MQTT_BROKER, MQTT_PORT, 60)

        # Register signal handlers
        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)

        # Start MQTT loop in a separate thread
        mqtt_thread = threading.Thread(target=client.loop_forever)
        mqtt_thread.start()

        # Wait for shutdown signal
        shutdown_event.wait()

        # Stop MQTT client loop and disconnect
        logging.info("Stopping MQTT loop...")
        client.disconnect()
        mqtt_thread.join()

        logging.info("Service terminated gracefully.")
    except Exception as e:
        logging.error(f"Service encountered an error: {e}")

if __name__ == "__main__":
    main()
