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
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
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


def _json_serializer(data):
    """Converts Python dict to JSON bytes, handling datetime objects."""
    def default_handler(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    return json.dumps(data, default=default_handler).encode("utf-8")


def reset_topic():
    """Deletes and recreates the topic to avoid duplicates."""
    logger.info(f"Resetting topic '{TOPIC_NAME}'")

    admin = AdminClient({"bootstrap.servers": REDPANDA_BROKER})

    # Delete if exists
    futures = admin.delete_topics([TOPIC_NAME])
    for topic, future in futures.items():
        try:
            future.result()
            logger.info(f"Deleted existing topic '{topic}'")
        except Exception:
            logger.info(f"Topic '{topic}' does not exist — nothing to delete")

    import time
    time.sleep(2)

    # Recreate
    new_topic = NewTopic(TOPIC_NAME, num_partitions=1, replication_factor=1)
    futures = admin.create_topics([new_topic])
    for topic, future in futures.items():
        try:
            future.result()
            logger.info(f"Created topic '{topic}'")
        except Exception as e:
            logger.info(f"Topic '{topic}' already exists: {e}")


def publish(activities: list[dict]):
    """Publishes all activities to Redpanda topic asynchronously."""
    logger.info(f"Publishing {len(activities)} activities to '{TOPIC_NAME}'")

    producer = Producer({"bootstrap.servers": REDPANDA_BROKER})

    success_count = 0
    error_count = 0

    def delivery_callback(err, msg):
        nonlocal success_count, error_count
        if err:
            error_count += 1
            logger.warning(f"Delivery failed: {err}")
        else:
            success_count += 1

    for activity in activities:
        producer.produce(
            topic=TOPIC_NAME,
            key=activity["employee_id"].encode("utf-8"),
            value=_json_serializer(activity),
            callback=delivery_callback,
        )

        # Trigger callbacks periodically to avoid buffer overflow
        producer.poll(0)

    # Wait for all messages to be delivered
    producer.flush()

    logger.info(f"Published {success_count} activities ({error_count} errors)")


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