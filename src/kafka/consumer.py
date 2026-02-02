"""
Kafka consumer with manual offset commit.
Only commits offset after successful file transfer.
"""

import json
import signal
import sys
from typing import Callable, Optional, Dict, Any, List
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from src.config_loader import ConfigLoader
from src.logging_config import CdpLogger
from scm_utilities.Constant.technical import Status


class KafkaMessageConsumer:
    """
    Kafka consumer with manual offset commit support.
    Ensures messages are only committed after successful processing.
    """

    def __init__(self):
        self._config = ConfigLoader()
        self._logger = CdpLogger.get_instance()
        self._consumer: Optional[KafkaConsumer] = None
        self._running = False

        # Get Kafka configuration (Vault placeholders already resolved)
        self._kafka_config = self._config.get("kafka", {})
        self._consumer_group = f"{self._kafka_config.get('msg_key', 'nas-watcher')}-consumer-group"

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals gracefully."""
        self._logger.log_info(
            message=f"Received shutdown signal {signum}, stopping consumer...",
            status=Status.Running,
            source="Kafka"
        )
        self._running = False

    def connect(self) -> bool:
        """
        Initialize Kafka consumer with SASL_SSL authentication.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            bootstrap_servers = self._kafka_config.get("bootstrap_servers", "")
            username = self._kafka_config.get("user_name", "")
            password = self._kafka_config.get("secret_key", "")

            if not all([bootstrap_servers, username, password]):
                self._logger.log_error(
                    message="Missing required Kafka configuration",
                    status=Status.Failed,
                    source="Kafka"
                )
                return False

            self._consumer = KafkaConsumer(
                bootstrap_servers=bootstrap_servers.split(","),
                security_protocol="SASL_SSL",
                sasl_mechanism="PLAIN",
                sasl_plain_username=username,
                sasl_plain_password=password,
                group_id=self._consumer_group,
                enable_auto_commit=False,  # CRITICAL: Manual commit only
                auto_offset_reset="earliest",
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda m: m.decode('utf-8') if m else None,
                max_poll_records=10,
                max_poll_interval_ms=300000,  # 5 minutes
                session_timeout_ms=45000,
                heartbeat_interval_ms=15000,
            )

            self._logger.log_info(
                message=f"Connected to Kafka: {bootstrap_servers}",
                status=Status.Running,
                source="Kafka"
            )
            return True

        except KafkaError as e:
            self._logger.log_error(
                message=f"Failed to connect to Kafka: {e}",
                status=Status.Failed,
                source="Kafka"
            )
            return False

    def get_topic_for_message(self, is_costed: str, bom_type: str) -> Optional[str]:
        """
        Get appropriate topic based on message attributes.

        Args:
            is_costed: "COST" or "UNCOST"
            bom_type: BOM type (contains EE or ME indicator)

        Returns:
            Topic name or None if not found
        """
        topic_names = self._kafka_config.get("topic_names", {})

        # Determine key based on costed status and type
        # Assuming EE types end with "EBOM" and ME types are others
        type_indicator = "EE" if "EBOM" in bom_type.upper() else "ME"
        cost_indicator = "Costed" if is_costed.upper() == "COST" else "UnCosted"

        topic_key = f"{cost_indicator}\\{type_indicator}"
        return topic_names.get(topic_key)

    def subscribe(self, topics: List[str]):
        """
        Subscribe to specified topics.

        Args:
            topics: List of topic names to subscribe to
        """
        if self._consumer:
            self._consumer.subscribe(topics)
            self._logger.log_info(
                message=f"Subscribed to topics: {topics}",
                status=Status.Running,
                source="Kafka"
            )

    def consume_and_process(
        self,
        topics: List[str],
        handler: Callable[[Dict[str, Any], str], bool],
        poll_timeout_ms: int = 1000
    ):
        """
        Consume messages and process them with the provided handler.
        Only commits offset if handler returns True.

        Args:
            topics: List of topics to consume from
            handler: Function that processes message and topic, returns success status
            poll_timeout_ms: Timeout for polling messages
        """
        if not self._consumer:
            if not self.connect():
                raise ConnectionError("Failed to connect to Kafka")

        self.subscribe(topics)
        self._running = True

        self._logger.log_info(
            message="Starting message consumption loop - waiting for Kafka messages",
            status=Status.Running,
            source="Kafka"
        )

        while self._running:
            try:
                # Poll for messages
                message_batch = self._consumer.poll(timeout_ms=poll_timeout_ms)

                for topic_partition, messages in message_batch.items():
                    kafka_topic = topic_partition.topic  # Get the topic name

                    for message in messages:
                        message_id = None
                        try:
                            # Extract message ID for logging
                            message_value = message.value
                            message_id = message_value.get("MESSAGE_ID", "unknown")

                            self._logger.log_info(
                                message=f"Received Kafka message: {message_id} from topic: {kafka_topic}",
                                status=Status.Running,
                                correlation_id=message_id,
                                source="Kafka"
                            )

                            # Process the message with topic name
                            success = handler(message_value, kafka_topic)

                            if success:
                                # Commit offset only on success
                                self._consumer.commit()
                                self._logger.log_info(
                                    message=f"Committed offset for message: {message_id}",
                                    status=Status.Completed,
                                    correlation_id=message_id,
                                    source="Kafka"
                                )
                            else:
                                # Do NOT commit - message will be reprocessed
                                self._logger.log_warning(
                                    message=f"Handler returned failure for message: {message_id}, will retry",
                                    status=Status.Running,
                                    correlation_id=message_id,
                                    source="Kafka"
                                )

                        except json.JSONDecodeError as e:
                            # Invalid message format - commit to skip
                            self._logger.log_error(
                                message=f"Invalid JSON message: {e}",
                                status=Status.Failed,
                                correlation_id=message_id,
                                source="Kafka"
                            )
                            self._consumer.commit()

                        except Exception as e:
                            # Processing error - do NOT commit
                            self._logger.log_error(
                                message=f"Error processing message {message_id}: {e}",
                                status=Status.Failed,
                                correlation_id=message_id,
                                source="Kafka"
                            )
                            # Don't commit, will retry on next poll

            except KafkaError as e:
                self._logger.log_error(
                    message=f"Kafka error during consumption: {e}",
                    status=Status.Failed,
                    source="Kafka"
                )
                # Brief pause before retrying
                import time
                time.sleep(5)

        self._logger.log_info(
            message="Message consumption loop stopped",
            status=Status.Completed,
            source="Kafka"
        )

    def close(self):
        """Close Kafka consumer connection."""
        self._running = False
        if self._consumer:
            try:
                self._consumer.close()
                self._logger.log_info(
                    message="Kafka consumer closed",
                    status=Status.Completed,
                    source="Kafka"
                )
            except Exception as e:
                self._logger.log_error(
                    message=f"Error closing Kafka consumer: {e}",
                    status=Status.Failed,
                    source="Kafka"
                )
            finally:
                self._consumer = None
