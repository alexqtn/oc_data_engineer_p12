# ============================================================
# publish_activities.py — Redpanda Producer (Phase 2)
# Generates activities and publishes each as a JSON message
# to the sport_activities Redpanda topic.
# Cleans the topic before publishing to avoid duplicates.
# ============================================================

import json
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
import os

from src.utils.logger import get_logger
from src.generators.generate_activities import load_sport_employees, generate_all_activities


logger = get_logger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(ROOT_DIR / ".env")

REDPANDA_HOST = os.getenv("POSTGRES_EXTERNAL_HOST", "localhost")
REDPANDA_PORT = os.getenv("REDPANDA_EXTERNAL_PORT", "19092")
REDPANDA_BROKER = f"{REDPANDA_HOST}:{REDPANDA_PORT}"
TOPIC_NAME = "sport_activities"


# ============================================================
# Custom JSON serializer that handles datetime objects.
# KafkaProducer needs a function that converts Python dict → bytes.
# datetime is not JSON-serializable by default, so we convert
# it to ISO format string before encoding.
# ============================================================
def _json_serializer(data):
    def default_handler(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    return json.dumps(data, default=default_handler).encode("utf-8")


# ============================================================
# STEP 1 — Reset topic to avoid duplicates from previous runs.
# Deletes existing topic, waits, then recreates it.
# ============================================================
def reset_topic():
    logger.info(f"Resetting topic '{TOPIC_NAME}'")

    admin = KafkaAdminClient(
        bootstrap_servers=REDPANDA_BROKER,
        client_id="sport_admin",
    )

    # Delete if exists
    try:
        admin.delete_topics([TOPIC_NAME])
        logger.info(f"Deleted existing topic '{TOPIC_NAME}'")

        import time
        time.sleep(2)

    except UnknownTopicOrPartitionError:
        logger.info(f"Topic '{TOPIC_NAME}' does not exist — nothing to delete")

    # Recreate with 1 partition (sufficient for POC)
    try:
        admin.create_topics([
            NewTopic(
                name=TOPIC_NAME,
                num_partitions=1,
                replication_factor=1,
            )
        ])
        logger.info(f"Created topic '{TOPIC_NAME}'")
    except TopicAlreadyExistsError:
        logger.info(f"Topic '{TOPIC_NAME}' already exists")

    admin.close()


# ============================================================
# STEP 2 — Publish all activities to Redpanda topic.
# Each activity is a JSON message with employee_id as key.
# Async: sends all messages then flushes at the end.
# ============================================================
def publish(activities: list[dict]):
    logger.info(f"Publishing {len(activities)} activities to '{TOPIC_NAME}'")

    producer = KafkaProducer(
        bootstrap_servers=REDPANDA_BROKER,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=_json_serializer,
    )

    success_count = 0
    error_count = 0

    for activity in activities:
        try:
            producer.send(
                topic=TOPIC_NAME,
                key=activity["employee_id"],
                value=activity,
            )
            success_count += 1

        except Exception as e:
            error_count += 1
            logger.warning(f"Failed to publish activity: {e}")

    # Wait for all async messages to be confirmed by Redpanda
    producer.flush()
    producer.close()

    logger.info(
        f"Published {success_count} activities "
        f"({error_count} errors)"
    )


# ============================================================
# MAIN — Generate activities, reset topic, publish all.
# ============================================================
def main():
    logger.info("=" * 60)
    logger.info("ACTIVITY PUBLISHER — Starting")
    logger.info("=" * 60)

    employees = load_sport_employees()
    activities = generate_all_activities(employees)

    reset_topic()
    publish(activities)

    logger.info("=" * 60)
    logger.info("ACTIVITY PUBLISHER — Complete")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()