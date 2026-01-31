"""
Main entry point for Kafka NAS to S3 Transfer Service.
Consumes Kafka messages and transfers files from NAS to S3.
"""

import os
import sys
import signal
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import json

from src.config_loader import ConfigLoader
from src.logging_config import CdpLogger
from src.kafka.consumer import KafkaMessageConsumer
from src.services.file_transfer_service import FileTransferService
from scm_utilities.Constant.technical import Status


class HealthCheckHandler(BaseHTTPRequestHandler):
    """HTTP handler for health check endpoints."""

    file_transfer_service = None

    def log_message(self, format, *args):
        """Suppress default logging."""
        pass

    def do_GET(self):
        """Handle GET requests for health checks."""
        if self.path == '/health':
            self._handle_health()
        elif self.path == '/ready':
            self._handle_ready()
        else:
            self.send_response(404)
            self.end_headers()

    def _handle_health(self):
        """Liveness probe - is the service running."""
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({"status": "healthy"}).encode())

    def _handle_ready(self):
        """Readiness probe - is the service ready to accept traffic."""
        if self.file_transfer_service:
            health = self.file_transfer_service.health_check()
            status_code = 200 if health.get("healthy", False) else 503
        else:
            status_code = 503
            health = {"healthy": False, "error": "Service not initialized"}

        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(health).encode())


def start_health_server(file_transfer_service: FileTransferService, port: int = 8080):
    """Start health check HTTP server in background thread."""
    HealthCheckHandler.file_transfer_service = file_transfer_service
    server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    return server


def main():
    """Main application entry point."""
    logger = CdpLogger.get_instance()

    logger.log_info(
        message="Starting Kafka NAS to S3 Transfer Service",
        status=Status.Running,
        source="Main"
    )

    try:
        # Load configuration
        config = ConfigLoader()

        # Initialize services
        file_transfer_service = FileTransferService()
        kafka_consumer = KafkaMessageConsumer()

        # Start health check server
        health_port = int(os.getenv("HEALTH_PORT", "8080"))
        health_server = start_health_server(file_transfer_service, health_port)
        logger.log_info(
            message=f"Health check server started on port {health_port}",
            status=Status.Running,
            source="Main"
        )

        # Determine topics to consume
        kafka_config = config.get("kafka", {})
        topic_names = kafka_config.get("topic_names", {})

        # Get all unique topics
        topics = list(set(topic_names.values()))

        if not topics:
            # Fallback to single topic if specified
            single_topic = kafka_config.get("topic_name", "")
            if single_topic:
                topics = [single_topic]

        if not topics:
            logger.log_error(
                message="No Kafka topics configured",
                status=Status.Failed,
                source="Main"
            )
            sys.exit(1)

        logger.log_info(
            message=f"Consuming from topics: {topics}",
            status=Status.Running,
            source="Main"
        )

        # Setup shutdown handler
        def shutdown_handler(signum, frame):
            logger.log_info(
                message="Received shutdown signal, cleaning up...",
                status=Status.Running,
                source="Main"
            )
            kafka_consumer.close()
            file_transfer_service.close()
            health_server.shutdown()
            sys.exit(0)

        signal.signal(signal.SIGTERM, shutdown_handler)
        signal.signal(signal.SIGINT, shutdown_handler)

        # Start consuming messages
        kafka_consumer.consume_and_process(
            topics=topics,
            handler=file_transfer_service.process_message
        )

    except KeyboardInterrupt:
        logger.log_info(
            message="Shutdown requested",
            status=Status.Completed,
            source="Main"
        )

    except Exception as e:
        logger.log_error(
            message=f"Application error: {str(e)}",
            status=Status.Failed,
            source="Main"
        )
        sys.exit(1)

    finally:
        logger.log_info(
            message="Kafka NAS to S3 Transfer Service stopped",
            status=Status.Completed,
            source="Main"
        )


if __name__ == "__main__":
    main()
