# ============================================================
# load_employees.py — HR Pipeline (Phase 1)
# Extracts Excel data, validates, transforms, resolves addresses,
# encrypts PII, and upserts into employees table.
# ============================================================

import pandas as pd
from pathlib import Path
from sqlalchemy import text

from src.utils.db import get_session
from src.utils.logger import get_logger
from src.utils.encryption import ENCRYPTION_KEY
from src.utils.gmaps import parse_address, calculate_distance, validate_commute
from src.validators.schema_employee import EmployeeSchema


logger = get_logger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data" / "raw"

# Excel column names → Pydantic field names
COLUMN_RENAME_EXTRACT = {
    "ID salarié":             "employee_id",
    "Nom":                    "last_name",
    "Prénom":                 "first_name",
    "Date de naissance":      "birth_date",
    "BU":                     "bu",
    "Date d'embauche":        "hire_date",
    "Salaire brut":           "gross_salary",
    "Type de contrat":        "contract_type",
    "Nombre de jours de CP":  "cp_days",
    "Adresse du domicile":    "home_address",
    "Moyen de déplacement":   "transport_mode",
    "Pratique d'un sport":    "sport",
}

# French transport modes → database ENUM values
TRANSPORT_MODE_MAPPING = {
    "marche/running":                    "walking",
    "vélo/trottinette/autres":           "cycling",
    "véhicule thermique/électrique":     "motorized",
    "transports en commun":              "motorized",
}


# ============================================================
# STEP 1 — EXTRACT
# Reads both Excel files and merges on employee ID.
# Renames columns to match Pydantic field names.
# ============================================================
def extract() -> pd.DataFrame:
    logger.info("Step 1 — Extracting data from Excel files")

    df_rh = pd.read_excel(DATA_DIR / "DonneesRH.xlsx")
    logger.info(f"DonneesRH.xlsx: {len(df_rh)} rows, {len(df_rh.columns)} columns")

    df_sport = pd.read_excel(DATA_DIR / "DonneesSportive.xlsx")
    logger.info(f"DonneesSportive.xlsx: {len(df_sport)} rows, {len(df_sport.columns)} columns")

    df = df_rh.merge(df_sport, on="ID salarié", how="left")
    df = df.rename(columns=COLUMN_RENAME_EXTRACT)

    logger.info(f"Extracted and merged: {len(df)} employees")
    return df


# ============================================================
# STEP 2 — VALIDATE
# Passes each row through Pydantic EmployeeSchema.
# Valid rows continue, invalid rows are logged and skipped.
# ============================================================
def validate(df: pd.DataFrame) -> list[dict]:
    logger.info("Step 2 — Validating with Pydantic schema")

    valid_employees = []
    rejected_count = 0

    for idx, row in df.iterrows():
        # Separate sport from validation data (not in EmployeeSchema)
        sport = row.get("sport", None)
        if pd.isna(sport):
            sport = None

        # Build dict without sport for Pydantic validation
        validation_dict = {
            k: v for k, v in row.to_dict().items()
            if k != "sport"
        }

        try:
            validated = EmployeeSchema(**validation_dict)
            employee = validated.model_dump()
            employee["home_address"] = row["home_address"]
            employee["sport"] = sport
            valid_employees.append(employee)

        except Exception as e:
            rejected_count += 1
            emp_id = row.get("employee_id", f"row_{idx}")
            logger.warning(f"Rejected employee {emp_id}: {e}")

    logger.info(
        f"Validation complete: {len(valid_employees)} valid, "
        f"{rejected_count} rejected out of {len(df)}"
    )

    if rejected_count > 0:
        logger.error(
            f"{rejected_count} employees rejected — "
            f"check transform or source data"
        )

    return valid_employees

# ============================================================
# STEP 3 — TRANSFORM
# Maps French transport modes to ENUM values.
# Renames Pydantic field names to database column names.
# ============================================================
def transform(employees: list[dict]) -> list[dict]:
    logger.info("Step 3 — Transforming to database format")

    transformed = []

    for emp in employees:
        # Map French transport mode to ENUM
        raw_mode = emp["transport_mode"].strip().lower()
        db_mode = TRANSPORT_MODE_MAPPING.get(raw_mode)

        if db_mode is None:
            logger.warning(
                f"Unknown transport mode for {emp['employee_id']}: "
                f"{emp['transport_mode']} — skipping"
            )
            continue

        transformed.append({
            "rh_employee_id":    emp["employee_id"],
            "rh_last_name":      emp["last_name"],
            "rh_first_name":     emp["first_name"],
            "rh_birth_date":     str(emp["birth_date"]),
            "rh_bu":             emp["bu"],
            "rh_hire_date":      emp["hire_date"],
            "rh_gross_salary":   str(emp["gross_salary"]),
            "rh_contract_type":  emp["contract_type"],
            "rh_cp_days":        emp["cp_days"],
            "rh_transport_mode": db_mode,
            "rh_is_active":      True,
            "home_address":      emp["home_address"],
            "sport":             emp.get("sport"),
        })

    logger.info(f"Transformed {len(transformed)} employees")
    return transformed


# ============================================================
# STEP 4 — ADDRESS PARSING + DISTANCE CALCULATION
# Calls Google Maps API to split address into components
# and calculate home-to-office distance per transport mode.
# ============================================================
def resolve_addresses(employees: list[dict]) -> list[dict]:
    logger.info("Step 4 — Resolving addresses via Google Maps")

    suspicious_count = 0

    for emp in employees:
        raw_address = emp["home_address"]
        transport_mode = emp["rh_transport_mode"]

        # Parse address into components
        parsed = parse_address(raw_address)

        if parsed:
            emp["rh_street_number"] = parsed["street_number"]
            emp["rh_street_name"]   = parsed["street_name"]
            emp["rh_postal_code"]   = parsed["postal_code"]
            emp["rh_city"]          = parsed["city"]
        else:
            emp["rh_street_number"] = ""
            emp["rh_street_name"]   = ""
            emp["rh_postal_code"]   = ""
            emp["rh_city"]          = ""

        # Calculate distance for eligible transport modes only
        if transport_mode in ("walking", "cycling"):
            distance = calculate_distance(raw_address, transport_mode)
            emp["be_declaration_valid"] = validate_commute(distance, transport_mode)

            if not emp["be_declaration_valid"]:
                suspicious_count += 1
        else:
            emp["be_declaration_valid"] = True

    logger.info(
        f"Addresses resolved: {len(employees)} processed, "
        f"{suspicious_count} suspicious declarations"
    )

    return employees


# ============================================================
# STEP 5 — ENCRYPT PII + UPSERT INTO POSTGRESQL
# Builds one INSERT per employee with pgcrypto encryption inline.
# ON CONFLICT updates existing rows. Soft deletes absent employees.
# ============================================================
UPSERT_QUERY = text("""
    INSERT INTO employees (
        rh_employee_id,
        rh_last_name,
        rh_first_name,
        rh_birth_date,
        rh_bu,
        rh_hire_date,
        rh_gross_salary,
        rh_contract_type,
        rh_cp_days,
        rh_street_number,
        rh_street_name,
        rh_postal_code,
        rh_city,
        rh_transport_mode,
        rh_is_active,
        be_declaration_valid,
        rh_created_at,
        rh_updated_at
    ) VALUES (
        :rh_employee_id,
        pgp_sym_encrypt(:rh_last_name, :key),
        pgp_sym_encrypt(:rh_first_name, :key),
        pgp_sym_encrypt(:rh_birth_date, :key),
        :rh_bu,
        :rh_hire_date,
        pgp_sym_encrypt(:rh_gross_salary, :key),
        :rh_contract_type,
        :rh_cp_days,
        pgp_sym_encrypt(:rh_street_number, :key),
        pgp_sym_encrypt(:rh_street_name, :key),
        pgp_sym_encrypt(:rh_postal_code, :key),
        pgp_sym_encrypt(:rh_city, :key),
        :rh_transport_mode,
        :rh_is_active,
        :be_declaration_valid,
        NOW(),
        NOW()
    )
    ON CONFLICT (rh_employee_id) DO UPDATE SET
        rh_last_name         = pgp_sym_encrypt(:rh_last_name, :key),
        rh_first_name        = pgp_sym_encrypt(:rh_first_name, :key),
        rh_birth_date        = pgp_sym_encrypt(:rh_birth_date, :key),
        rh_bu                = :rh_bu,
        rh_hire_date         = :rh_hire_date,
        rh_gross_salary      = pgp_sym_encrypt(:rh_gross_salary, :key),
        rh_contract_type     = :rh_contract_type,
        rh_cp_days           = :rh_cp_days,
        rh_street_number     = pgp_sym_encrypt(:rh_street_number, :key),
        rh_street_name       = pgp_sym_encrypt(:rh_street_name, :key),
        rh_postal_code       = pgp_sym_encrypt(:rh_postal_code, :key),
        rh_city              = pgp_sym_encrypt(:rh_city, :key),
        rh_transport_mode    = :rh_transport_mode,
        rh_is_active         = :rh_is_active,
        be_declaration_valid = :be_declaration_valid,
        rh_updated_at        = NOW()
""")

SOFT_DELETE_QUERY = text("""
    UPDATE employees
    SET rh_is_active = FALSE, rh_updated_at = NOW()
    WHERE rh_employee_id NOT IN :current_ids
    AND rh_is_active = TRUE
""")


def upsert_employees(employees: list[dict]) -> None:
    logger.info("Step 5 — Encrypting PII and upserting into PostgreSQL")

    current_ids = [emp["rh_employee_id"] for emp in employees]

    with get_session() as session:
        for emp in employees:
            session.execute(UPSERT_QUERY, {
                "rh_employee_id":     emp["rh_employee_id"],
                "rh_last_name":       emp["rh_last_name"],
                "rh_first_name":      emp["rh_first_name"],
                "rh_birth_date":      emp["rh_birth_date"],
                "rh_bu":              emp["rh_bu"],
                "rh_hire_date":       emp["rh_hire_date"],
                "rh_gross_salary":    emp["rh_gross_salary"],
                "rh_contract_type":   emp["rh_contract_type"],
                "rh_cp_days":         emp["rh_cp_days"],
                "rh_street_number":   emp["rh_street_number"],
                "rh_street_name":     emp["rh_street_name"],
                "rh_postal_code":     emp["rh_postal_code"],
                "rh_city":            emp["rh_city"],
                "rh_transport_mode":  emp["rh_transport_mode"],
                "rh_is_active":       emp["rh_is_active"],
                "be_declaration_valid": emp["be_declaration_valid"],
                "key":                ENCRYPTION_KEY,
            })

        # Soft delete employees no longer in Excel
        session.execute(SOFT_DELETE_QUERY, {
            "current_ids": tuple(current_ids),
        })

        session.commit()

    logger.info(f"Upserted {len(employees)} employees into PostgreSQL")


# ============================================================
# MAIN — Orchestrates all 5 steps in sequence.
# This function is the entry point called by Kestra or manually.
# ============================================================
def main():
    logger.info("=" * 60)
    logger.info("HR PIPELINE — Starting")
    logger.info("=" * 60)

    df = extract()
    valid_employees = validate(df)
    transformed = transform(valid_employees)
    resolved = resolve_addresses(transformed)
    upsert_employees(resolved)

    logger.info("=" * 60)
    logger.info("HR PIPELINE — Complete")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()