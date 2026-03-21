# ============================================================
# generate_activities.py — Sport Activity Generator (Phase 2)
# Creates 12 months of simulated sport activities per employee.
# Deterministic: same seed = same output every run.
# Returns a list of activity dicts ready for Redpanda or DB.
# ============================================================

import random
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

from src.utils.logger import get_logger
from src.validators.schema_activity import ActivitySchema


logger = get_logger(__name__)

# Fixed seed for deterministic generation
RANDOM_SEED = 42

# French sport names → database ENUM + generation config
# Each config defines realistic physical ranges for that sport
SPORT_CONFIG = {
    "tennis": {
        "activity_type": "racket_sports",
        "has_distance": False,
        "duration_range": (3600, 7200),
        "has_climb": False,
        "comments": [
            "Bon match aujourd'hui !",
            "Set serré, belle victoire",
            "Entrainement au service",
            "Match en double, super ambiance",
        ],
    },
    "badminton": {
        "activity_type": "racket_sports",
        "has_distance": False,
        "duration_range": (1800, 5400),
        "has_climb": False,
        "comments": [
            "Session intense !",
            "Smash qui claque",
            "Tournoi du club",
        ],
    },
    "tennis de table": {
        "activity_type": "racket_sports",
        "has_distance": False,
        "duration_range": (1800, 3600),
        "has_climb": False,
        "comments": [
            "Ping-pong du midi",
            "Revers qui progresse",
        ],
    },
    "football": {
        "activity_type": "team_sports",
        "has_distance": False,
        "duration_range": (3600, 5400),
        "has_climb": False,
        "comments": [
            "Victoire 3-1 !",
            "Beau match d'equipe",
            "Entrainement du mardi",
            "But en lucarne !",
        ],
    },
    "basketball": {
        "activity_type": "team_sports",
        "has_distance": False,
        "duration_range": (3600, 5400),
        "has_climb": False,
        "comments": [
            "Gros dunk ce soir",
            "Match serre mais on gagne",
            "Entrainement collectif",
        ],
    },
    "rugby": {
        "activity_type": "team_sports",
        "has_distance": False,
        "duration_range": (4800, 6000),
        "has_climb": False,
        "comments": [
            "Bel essai en equipe",
            "Match physique mais content",
            "Entrainement touche et melee",
        ],
    },
    "judo": {
        "activity_type": "combat_sports",
        "has_distance": False,
        "duration_range": (3600, 5400),
        "has_climb": False,
        "comments": [
            "Ippon en finale !",
            "Bonne seance de randori",
            "Travail au sol aujourd'hui",
        ],
    },
    "boxe": {
        "activity_type": "combat_sports",
        "has_distance": False,
        "duration_range": (3600, 5400),
        "has_climb": False,
        "comments": [
            "Rounds intenses",
            "Sparring avec le coach",
            "Travail de vitesse au sac",
        ],
    },
    "runing": {
        "activity_type": "running",
        "has_distance": True,
        "distance_range": (3000, 20000),
        "duration_range": (1200, 5400),
        "speed_range": (7, 15),
        "has_climb": True,
        "climb_range": (10, 200),
        "comments": [
            "Belle foulée ce matin",
            "Sorti de la zone de confort",
            "Fractionne sur la piste",
            "Reprise apres une pause",
            "Objectif semi-marathon !",
        ],
    },
    "triathlon": {
        "activity_type": "running",
        "has_distance": True,
        "distance_range": (5000, 42000),
        "duration_range": (1800, 10800),
        "speed_range": (8, 18),
        "has_climb": True,
        "climb_range": (20, 500),
        "comments": [
            "Entrainement transition",
            "Sortie longue en preparation",
            "Brick session velo-course",
        ],
    },
    "randonnée": {
        "activity_type": "hiking",
        "has_distance": True,
        "distance_range": (5000, 25000),
        "duration_range": (3600, 18000),
        "speed_range": (3, 6),
        "has_climb": True,
        "climb_range": (100, 1200),
        "comments": [
            "Pic Saint-Loup magnifique",
            "Balade au bord du Lez",
            "Randonnee de St Guilhem le desert, je vous la conseille c'est top",
            "Sentier des douaniers superbe",
            "Sortie en famille dans les Cevennes",
        ],
    },
    "natation": {
        "activity_type": "swimming",
        "has_distance": True,
        "distance_range": (500, 3000),
        "duration_range": (1800, 5400),
        "speed_range": (1.5, 4),
        "has_climb": False,
        "comments": [
            "50 longueurs ce matin",
            "Entrainement crawl",
            "Seance papillon, les epaules brulent",
            "Nage en eau libre au lac",
        ],
    },
    "escalade": {
        "activity_type": "outdoor_sports",
        "has_distance": False,
        "duration_range": (3600, 10800),
        "has_climb": True,
        "climb_range": (50, 500),
        "comments": [
            "Voie 6b envoyee !",
            "Bloc en salle, gros progres",
            "Falaise de Claret magnifique",
        ],
    },
    "équitation": {
        "activity_type": "outdoor_sports",
        "has_distance": True,
        "distance_range": (5000, 20000),
        "duration_range": (3600, 7200),
        "speed_range": (5, 15),
        "has_climb": False,
        "comments": [
            "Belle balade a cheval",
            "Cours de dressage",
            "Galop en pleine campagne",
        ],
    },
    "voile": {
        "activity_type": "outdoor_sports",
        "has_distance": True,
        "distance_range": (5000, 30000),
        "duration_range": (7200, 18000),
        "speed_range": (5, 20),
        "has_climb": False,
        "comments": [
            "Navigation vers Sete",
            "Regate du dimanche",
            "Vent parfait aujourd'hui",
        ],
    },
}

# Activities per month: random between 1 and 12
ACTIVITIES_PER_MONTH_RANGE = (1, 12)


# ============================================================
# STEP 1 — Load employees who practice a sport from PostgreSQL
# Single source of truth: database, not Excel
# ============================================================
def load_sport_employees() -> list[dict]:
    logger.info("Step 1 — Loading employees with sport from PostgreSQL")

    from src.utils.db import get_engine

    df = pd.read_sql(
        "SELECT rh_employee_id, rh_sport FROM employees "
        "WHERE rh_sport IS NOT NULL AND rh_is_active = TRUE",
        get_engine(),
    )

    employees = []
    for _, row in df.iterrows():
        sport_raw = str(row["rh_sport"]).strip().lower()

        if sport_raw not in SPORT_CONFIG:
            logger.warning(f"Unknown sport '{sport_raw}' for employee {row['rh_employee_id']}")
            continue

        employees.append({
            "employee_id": str(row["rh_employee_id"]),
            "sport": sport_raw,
        })

    logger.info(f"Loaded {len(employees)} employees with a declared sport")
    return employees


# ============================================================
# STEP 2 — Generate one activity with realistic values
# based on sport configuration
# ============================================================
def _generate_one_activity(employee_id: str, sport: str, activity_date: datetime) -> dict:
    config = SPORT_CONFIG[sport]

    # Duration from sport-specific range
    min_dur, max_dur = config["duration_range"]
    elapsed_time = random.randint(min_dur, max_dur)

    # Distance: only for sports where it's meaningful
    distance = None
    avg_speed = None
    max_speed = None

    if config["has_distance"]:
        min_dist, max_dist = config["distance_range"]
        distance = round(random.uniform(min_dist, max_dist), 0)

        # Speed derived from distance and time (physically coherent)
        avg_speed = round(distance / elapsed_time, 2)

        # Max speed is always higher than average
        max_speed = round(avg_speed * random.uniform(1.1, 1.4), 2)

    # Climb: only for relevant sports
    climb = None
    if config["has_climb"]:
        min_climb, max_climb = config["climb_range"]
        climb = round(random.uniform(min_climb, max_climb), 0)

    # Comment: 20% chance of having one
    comment = None
    if random.random() < 0.2:
        comment = random.choice(config["comments"])

    return {
        "employee_id": employee_id,
        "activity_type": config["activity_type"],
        "start_date": activity_date,
        "elapsed_time": elapsed_time,
        "distance": distance,
        "avg_speed": avg_speed,
        "max_speed": max_speed,
        "climb": climb,
        "comment": comment,
        "data_source": "simulated",
    }


# ============================================================
# STEP 3 — Generate 12 months of activities for all employees
# ============================================================
def generate_all_activities(employees: list[dict]) -> list[dict]:
    logger.info("Step 2 — Generating 12 months of activities")

    random.seed(RANDOM_SEED)

    now = datetime.now()
    all_activities = []
    validation_errors = 0

    for emp in employees:
        employee_id = emp["employee_id"]
        sport = emp["sport"]

        for months_ago in range(12, 0, -1):
            # Calculate first and last day of the target month
            target_date = now - timedelta(days=months_ago * 30)
            month_start = target_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

            if month_start.month == 12:
                month_end = month_start.replace(year=month_start.year + 1, month=1, day=1)
            else:
                month_end = month_start.replace(month=month_start.month + 1, day=1)

            # Random number of activities this month (1-12)
            min_act, max_act = ACTIVITIES_PER_MONTH_RANGE
            activity_count = random.randint(min_act, max_act)

            for _ in range(activity_count):
                # Random day and time within the month
                days_in_month = (month_end - month_start).days
                random_day = random.randint(0, days_in_month - 1)
                random_hour = random.randint(6, 20)
                random_minute = random.randint(0, 59)

                activity_date = month_start + timedelta(
                    days=random_day,
                    hours=random_hour,
                    minutes=random_minute,
                )

                # Generate the activity
                activity = _generate_one_activity(employee_id, sport, activity_date)

                # Validate with Pydantic
                try:
                    validated = ActivitySchema(**activity)
                    all_activities.append(validated.model_dump())
                except Exception as e:
                    validation_errors += 1
                    logger.warning(
                        f"Validation failed for employee {employee_id}: {e}"
                    )

    logger.info(
        f"Generated {len(all_activities)} activities "
        f"for {len(employees)} employees "
        f"({validation_errors} validation errors)"
    )

    return all_activities


# ============================================================
# MAIN — Entry point for standalone execution
# ============================================================
def main():
    logger.info("=" * 60)
    logger.info("ACTIVITY GENERATOR — Starting")
    logger.info("=" * 60)

    employees = load_sport_employees()
    activities = generate_all_activities(employees)

    logger.info(f"Total activities generated: {len(activities)}")
    logger.info("=" * 60)
    logger.info("ACTIVITY GENERATOR — Complete")
    logger.info("=" * 60)

    return activities


if __name__ == "__main__":
    main()