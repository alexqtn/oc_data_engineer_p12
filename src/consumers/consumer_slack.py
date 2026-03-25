# ============================================================
# consumer_slack.py — Redpanda Consumer for Slack Notifications (Phase 2)
# Reads new sport activities from Redpanda topic and sends
# congratulatory messages to Slack. Designed for LIVE mode only.
# ============================================================

import json
import time
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError
import requests
import os

from src.utils.db import get_engine
from src.utils.encryption import ENCRYPTION_KEY
from src.utils.logger import get_logger


logger = get_logger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(ROOT_DIR / ".env")

REDPANDA_HOST = os.getenv("POSTGRES_EXTERNAL_HOST", "localhost")
REDPANDA_PORT = os.getenv("REDPANDA_EXTERNAL_PORT", "19092")
REDPANDA_BROKER = os.getenv(
    "REDPANDA_EXTERNAL_BROKER",
    os.getenv("REDPANDA_BROKERS", "localhost:19092")
)

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

if not SLACK_WEBHOOK_URL:
    raise ValueError(
        "Missing SLACK_WEBHOOK_URL. "
        "Ensure SLACK_WEBHOOK_URL is set in your .env file."
    )

TOPIC_NAME = "sport_activities"
CONSUMER_GROUP = "slack_consumer"

# Slack free tier: max 1 message per second
SLACK_RATE_LIMIT_S = 1

# Message templates per activity type (with distance)
MESSAGES_WITH_DISTANCE = {
    "running": "Tu viens de courir {distance} km en {duration} !",
    "hiking": "Une randonnée de {distance} km terminée !",
    "swimming": "Tu viens de nager {distance} km en {duration} !",
    "cycling": "Tu viens de pédaler {distance} km en {duration} !",
    "outdoor_sports": "{duration} d'activité en plein air, {distance} km parcourus !",
}

# Message templates per activity type (without distance)
MESSAGES_WITHOUT_DISTANCE = {
    "racket_sports": "{duration} de sport de raquette aujourd'hui !",
    "team_sports": "{duration} de sport collectif aujourd'hui !",
    "combat_sports": "{duration} de sport de combat aujourd'hui !",
    "outdoor_sports": "{duration} d'activité en plein air !",
    "swimming": "{duration} de natation aujourd'hui !",
    "running": "{duration} de course aujourd'hui !",
    "hiking": "{duration} de randonnée aujourd'hui !",
    "cycling": "{duration} de vélo aujourd'hui !",
}

# Congratulation prefixes — randomly varied for diversity
CONGRATS = [
    "Bravo",
    "Magnifique",
    "Super",
    "Félicitations",
    "Chapeau",
    "Impressionnant",
]


# ============================================================
# Load employee names from PostgreSQL (decrypted).
# Called once at startup. Returns dict {id: "Prénom Nom"}.
# ============================================================
def _load_employee_names() -> dict:
    import pandas as pd

    query = f"""
        SELECT rh_employee_id,
               pgp_sym_decrypt(rh_first_name, '{ENCRYPTION_KEY}') AS first_name,
               pgp_sym_decrypt(rh_last_name, '{ENCRYPTION_KEY}') AS last_name
        FROM employees
        WHERE rh_is_active = TRUE
    """

    df = pd.read_sql(query, get_engine())

    names = {}
    for _, row in df.iterrows():
        names[str(row["rh_employee_id"])] = f"{row['first_name']} {row['last_name']}"

    logger.info(f"Loaded {len(names)} employee names from PostgreSQL")
    return names


# ============================================================
# Format elapsed seconds into human-readable duration.
# 3600 → "1h00"  |  5430 → "1h30"  |  2700 → "45 min"
# ============================================================
def _format_duration(seconds: int) -> str:
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60

    if hours > 0:
        return f"{hours}h{minutes:02d}"
    return f"{minutes} min"


# ============================================================
# Build a congratulatory Slack message from an activity.
# ============================================================
def _build_slack_message(activity: dict, employee_name: str) -> str:
    import random

    congrat = random.choice(CONGRATS)
    activity_type = activity["activity_type"]
    duration = _format_duration(activity["elapsed_time"])

    # Choose template based on whether activity has distance
    if activity.get("distance") and activity["distance"] > 0:
        distance_km = round(activity["distance"] / 1000, 1)
        template = MESSAGES_WITH_DISTANCE.get(
            activity_type,
            "Tu viens de faire {distance} km en {duration} !"
        )
        body = template.format(distance=distance_km, duration=duration)
    else:
        template = MESSAGES_WITHOUT_DISTANCE.get(
            activity_type,
            "{duration} d'activité aujourd'hui !"
        )
        body = template.format(duration=duration)

    message = f"{congrat} {employee_name} ! {body}"

    # Append comment if present
    if activity.get("comment"):
        message += f' ("{activity["comment"]}")'

    return message


# ============================================================
# Send a message to Slack via webhook.
# ============================================================
def _send_to_slack(message: str) -> bool:
    try:
        response = requests.post(
            SLACK_WEBHOOK_URL,
            json={"text": message},
            timeout=10,
        )

        if response.status_code == 200:
            return True
        else:
            logger.warning(f"Slack returned {response.status_code}: {response.text}")
            return False

    except Exception as e:
        logger.error(f"Slack notification failed: {e}")
        return False


# ============================================================
# Main consume loop — reads from Redpanda, sends to Slack.
# Uses "latest" offset: only processes NEW activities.
# ============================================================
def consume():
    logger.info("Loading employee names for Slack messages...")
    employee_names = _load_employee_names()

    logger.info(f"Connecting to Redpanda at {REDPANDA_BROKER}")
    logger.info(f"Topic: {TOPIC_NAME}, Group: {CONSUMER_GROUP}")
    logger.info("Waiting for new activities (live mode)...")

    consumer = Consumer({
        "bootstrap.servers": REDPANDA_BROKER,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })

    consumer.subscribe([TOPIC_NAME])

    total_sent = 0

    try:
        while True:
            msg = consumer.poll(timeout=5.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

            try:
                activity = json.loads(msg.value().decode("utf-8"))
                employee_id = activity["employee_id"]

                # Look up employee name
                employee_name = employee_names.get(
                    employee_id,
                    f"Employé {employee_id}"
                )

                # Build and send Slack message
                slack_message = _build_slack_message(activity, employee_name)
                success = _send_to_slack(slack_message)

                if success:
                    total_sent += 1
                    logger.info(f"Slack notification sent: {employee_name}")
                else:
                    logger.warning(f"Failed to notify for {employee_name}")

                # Respect Slack rate limit
                time.sleep(SLACK_RATE_LIMIT_S)

            except Exception as e:
                logger.warning(f"Failed to process message: {e}")
                continue

    except KeyboardInterrupt:
        logger.info("Consumer stopped by user (Ctrl+C)")

    finally:
        consumer.close()
        logger.info(f"Consumer closed — {total_sent} notifications sent")


def main():
    logger.info("=" * 60)
    logger.info("SLACK CONSUMER — Starting")
    logger.info("=" * 60)

    consume()

    logger.info("=" * 60)
    logger.info("SLACK CONSUMER — Complete")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()