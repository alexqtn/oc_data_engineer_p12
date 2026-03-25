# ============================================================
# inject_live_activity.py — Live Demo Activity Injection
# Creates N activities for a given employee, publishes to
# Redpanda, and sends Slack notifications directly.
# Used by Kestra flow during live demo.
# ============================================================

import json
import sys
import random
from datetime import datetime

from confluent_kafka import Producer
import requests
import pandas as pd

from src.utils.db import get_engine
from src.utils.encryption import ENCRYPTION_KEY
from src.utils.logger import get_logger
from src.generators.generate_activities import SPORT_CONFIG, _generate_one_activity

import os
from pathlib import Path
from dotenv import load_dotenv


logger = get_logger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(ROOT_DIR / ".env")

REDPANDA_BROKER = os.getenv(
    "REDPANDA_EXTERNAL_BROKER",
    os.getenv("REDPANDA_BROKERS", "localhost:19092")
)

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

TOPIC_NAME = "sport_activities"

CONGRATS = [
    "Bravo",
    "Magnifique",
    "Super",
    "Félicitations",
    "Chapeau",
    "Impressionnant",
]

MESSAGES_WITH_DISTANCE = {
    "running": "Tu viens de courir {distance} km en {duration} !",
    "hiking": "Une randonnée de {distance} km terminée !",
    "swimming": "Tu viens de nager {distance} km en {duration} !",
    "cycling": "Tu viens de pédaler {distance} km en {duration} !",
    "outdoor_sports": "{duration} d'activité en plein air, {distance} km parcourus !",
}

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

# ============================================================
# Format duration from seconds to human-readable
# ============================================================
def _format_duration(seconds: int) -> str:
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60

    if hours > 0:
        return f"{hours}h{minutes:02d}"
    return f"{minutes} min"


# ============================================================
# Build Slack message from activity data
# ============================================================
def _build_slack_message(activity: dict, employee_name: str) -> str:
    congrat = random.choice(CONGRATS)
    activity_type = activity["activity_type"]
    duration = _format_duration(activity["elapsed_time"])

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

    if activity.get("comment"):
        message += f' ("{activity["comment"]}")'

    return message


# ============================================================
# Send message to Slack via webhook
# ============================================================
def _send_to_slack(message: str) -> bool:
    if not SLACK_WEBHOOK_URL:
        logger.warning("SLACK_WEBHOOK_URL not set — skipping notification")
        return False

    try:
        response = requests.post(
            SLACK_WEBHOOK_URL,
            json={"text": message},
            timeout=10,
        )
        return response.status_code == 200
    except Exception as e:
        logger.error(f"Slack notification failed: {e}")
        return False


# ============================================================
# JSON serializer for Redpanda (handles datetime)
# ============================================================
def _json_serializer(data):
    def default_handler(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    return json.dumps(data, default=default_handler).encode("utf-8")


# ============================================================
# Main: generate N activities, publish to Redpanda, notify Slack
# ============================================================
def inject(activity_count: int = 3):
    logger.info(f"Injecting {activity_count} activities for random employees")

    # Load all employees with a sport
    query = f"""
        SELECT
            rh_employee_id,
            pgp_sym_decrypt(rh_first_name, '{ENCRYPTION_KEY}') AS first_name,
            pgp_sym_decrypt(rh_last_name, '{ENCRYPTION_KEY}') AS last_name,
            rh_sport
        FROM employees
        WHERE rh_is_active = TRUE
        AND rh_sport IS NOT NULL
    """

    df = pd.read_sql(query, get_engine())
    logger.info(f"Found {len(df)} employees with sport")

    # Pick N random employees
    selected = df.sample(n=min(activity_count, len(df)))

    # Connect to Redpanda
    producer = Producer({"bootstrap.servers": REDPANDA_BROKER})

    for _, row in selected.iterrows():
        employee_id = str(row["rh_employee_id"])
        name = f"{row['first_name']} {row['last_name']}"
        sport = str(row["rh_sport"]).strip().lower()

        if sport not in SPORT_CONFIG:
            logger.warning(f"Unknown sport '{sport}' for {name} — skipping")
            continue

        # Generate one activity with current timestamp
        activity = _generate_one_activity(employee_id, sport, datetime.now())

        # Publish to Redpanda
        producer.produce(
            topic=TOPIC_NAME,
            key=employee_id.encode("utf-8"),
            value=_json_serializer(activity),
        )
        producer.poll(0)

        # Send Slack notification
        slack_message = _build_slack_message(activity, name)
        success = _send_to_slack(slack_message)

        if success:
            logger.info(f"Slack sent: {name} ({sport})")
        else:
            logger.warning(f"Slack failed: {name}")

    producer.flush()
    logger.info(f"Published {activity_count} activities to Redpanda")


def main():
    logger.info("=" * 60)
    logger.info("LIVE ACTIVITY INJECTION — Starting")
    logger.info("=" * 60)

    activity_count = int(sys.argv[1]) if len(sys.argv) > 1 else 3

    inject(activity_count)

    logger.info("=" * 60)
    logger.info("LIVE ACTIVITY INJECTION — Complete")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()