# ============================================================
# consumer_postgres.py — Redpanda Consumer for PostgreSQL (Phase 2)
# Reads sport activities from Redpanda topic and inserts
# into sport_activities table. Uses UPSERT for idempotency.
# ============================================================

import json
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError
from sqlalchemy import text
import os

from src.utils.db import get_session
from src.utils.logger import get_logger


logger = get_logger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(ROOT_DIR / ".env")

REDPANDA_HOST = os.getenv("POSTGRES_EXTERNAL_HOST", "localhost")
REDPANDA_PORT = os.getenv("REDPANDA_EXTERNAL_PORT", "19092")
REDPANDA_BROKER = f"{REDPANDA_HOST}:{REDPANDA_PORT}"

TOPIC_NAME = "sport_activities"
CONSUMER_GROUP = "postgres_consumer"

BATCH_SIZE = 100
POLL_TIMEOUT_S = 5.0
MAX_EMPTY_POLLS = 6


UPSERT_QUERY = text("""
    INSERT INTO sport_activities (
        sp_employee_id,
        sp_activity_type,
        sp_start_date,
        sp_elapsed_time,
        sp_distance,
        sp_avg_speed,
        sp_max_speed,
        sp_climb,
        sp_comment,
        sp_data_source,
        sp_is_active,
        sp_created_at,
        sp_updated_at
    ) VALUES (
        :sp_employee_id,
        :sp_activity_type,
        :sp_start_date,
        :sp_elapsed_time,
        :sp_distance,
        :sp_avg_speed,
        :sp_max_speed,
        :sp_climb,
        :sp_comment,
        :sp_data_source,
        TRUE,
        NOW(),
        NOW()
    )
    ON CONFLICT (sp_employee_id, sp_start_date) DO UPDATE SET
        sp_activity_type = :sp_activity_type,
        sp_elapsed_time  = :sp_elapsed_time,
        sp_distance      = :sp_distance,
        sp_avg_speed     = :sp_avg_speed,
        sp_max_speed     = :sp_max_speed,
        sp_climb         = :sp_climb,
        sp_comment       = :sp_comment,
        sp_data_source   = :sp_data_source,
        sp_updated_at    = NOW()
""")


def _parse_message(raw_value: bytes) -> dict:
    """Deserializes JSON message and converts start_date back to datetime."""
    data = json.loads(raw_value.decode("utf-8"))
    data["start_date"] = datetime.fromisoformat(data["start_date"])
    return data


def consume():
    logger.info(f"Connecting to Redpanda at {REDPANDA_BROKER}")
    logger.info(f"Topic: {TOPIC_NAME}, Group: {CONSUMER_GROUP}")

    consumer = Consumer({
        "bootstrap.servers": REDPANDA_BROKER,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    consumer.subscribe([TOPIC_NAME])

    total_inserted = 0
    batch_count = 0
    empty_polls = 0

    with get_session() as session:
        while True:
            msg = consumer.poll(timeout=POLL_TIMEOUT_S)

            # No message received
            if msg is None:
                empty_polls += 1
                logger.info(
                    f"No messages received "
                    f"(empty poll {empty_polls}/{MAX_EMPTY_POLLS})"
                )
                if empty_polls >= MAX_EMPTY_POLLS:
                    logger.info("Max empty polls reached — stopping consumer")
                    break
                continue

            # Error handling
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info("Reached end of partition")
                    empty_polls += 1
                    if empty_polls >= MAX_EMPTY_POLLS:
                        break
                    continue
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

            # Reset counter on successful message
            empty_polls = 0

            try:
                activity = _parse_message(msg.value())

                session.execute(UPSERT_QUERY, {
                    "sp_employee_id":   activity["employee_id"],
                    "sp_activity_type": activity["activity_type"],
                    "sp_start_date":    activity["start_date"],
                    "sp_elapsed_time":  activity["elapsed_time"],
                    "sp_distance":      activity.get("distance"),
                    "sp_avg_speed":     activity.get("avg_speed"),
                    "sp_max_speed":     activity.get("max_speed"),
                    "sp_climb":         activity.get("climb"),
                    "sp_comment":       activity.get("comment"),
                    "sp_data_source":   activity["data_source"],
                })

                batch_count += 1
                total_inserted += 1

            except Exception as e:
                logger.warning(
                    f"Failed to process message at offset "
                    f"{msg.offset()}: {e}"
                )
                continue

            # Batch commit: DB first, then offset
            if batch_count >= BATCH_SIZE:
                session.commit()
                consumer.commit()
                logger.info(f"Committed batch — {total_inserted} total")
                batch_count = 0

        # Final commit for remaining messages
        if batch_count > 0:
            session.commit()
            consumer.commit()
            logger.info(f"Final commit — {total_inserted} total")

    consumer.close()
    logger.info(f"Consumer closed — {total_inserted} activities inserted")


def main():
    logger.info("=" * 60)
    logger.info("POSTGRES CONSUMER — Starting")
    logger.info("=" * 60)

    consume()

    logger.info("=" * 60)
    logger.info("POSTGRES CONSUMER — Complete")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()