# ============================================================
# gmaps.py — Google Maps API helpers for address parsing and
# distance calculation between employee home and office
# ============================================================

from pathlib import Path

from dotenv import load_dotenv
import requests
import os

from src.utils.logger import get_logger


ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(ROOT_DIR / ".env")

GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

if not GOOGLE_MAPS_API_KEY:
    raise ValueError(
        "Missing GOOGLE_MAPS_API_KEY. "
        "Ensure GOOGLE_MAPS_API_KEY is set in your .env file."
    )

logger = get_logger(__name__)

# Office address from note de cadrage — destination for all distance calculations
OFFICE_ADDRESS = "1362 Av. des Platanes, 34970 Lattes"

# Google API endpoints
GEOCODING_URL = "https://maps.googleapis.com/maps/api/geocode/json"
DISTANCE_URL = "https://maps.googleapis.com/maps/api/distancematrix/json"

# Google uses "bicycling" not "cycling"
TRANSPORT_MODE_MAPPING = {
    "walking": "walking",
    "cycling": "bicycling",
}

# Distance thresholds in km (from benefit_rules seed values)
DISTANCE_THRESHOLDS = {
    "walking": 15,
    "cycling": 25,
}

def _extract_street_number_fallback(raw_address: str) -> str:
    """
    Fallback: extracts leading digits from raw address.
    '128 Rue du Port, 34000 Frontignan' -> '128'
    'Lieu-dit Le Mas, 34970 Lattes' -> ''
    """
    parts = raw_address.strip().split(" ", 1)
    if parts and parts[0].isdigit:
        return parts[0]
    return ""


def parse_address(raw_address: str) -> dict | None:
    """
    Calls Google Geocoding API to split a raw address into components.
    Returns dict with street_number, street_name, postal_code, city.
    Returns None if Google cannot resolve the address.
    """
    response = requests.get(
        GEOCODING_URL,
        params={
            "address": raw_address,
            "key": GOOGLE_MAPS_API_KEY,
            "language": "fr",
            "region": "fr",
        },
        timeout=10,
    )

    data = response.json()

    if data["status"] != "OK" or not data["results"]:
        logger.warning(f"Geocoding failed for: {raw_address} — status: {data['status']}")
        return None

    # Google returns a list of address components, each with a type
    # We extract the 4 fields we need by matching component types
    components = data["results"][0]["address_components"]

    parsed = {
        "street_number": _extract_component(components, "street_number")
            or _extract_street_number_fallback(raw_address),
        "street_name": _extract_component(components, "route"),
        "postal_code": _extract_component(components, "postal_code"),
        "city": _extract_component(components, "locality"),
    }

    logger.info(f"Parsed address: {raw_address} → {parsed['city']}")
    return parsed


def calculate_distance(origin: str, transport_mode: str) -> float | None:
    """
    Calls Google Distance Matrix API to calculate distance from origin to office.
    Returns distance in km. Returns None if calculation fails.
    Skips API call for motorized transport (not eligible for prime).
    """
    if transport_mode == "motorized":
        logger.info(f"Skipping distance for motorized: {origin}")
        return None

    google_mode = TRANSPORT_MODE_MAPPING.get(transport_mode)
    if not google_mode:
        logger.warning(f"Unknown transport mode: {transport_mode}")
        return None

    response = requests.get(
        DISTANCE_URL,
        params={
            "origins": origin,
            "destinations": OFFICE_ADDRESS,
            "mode": google_mode,
            "key": GOOGLE_MAPS_API_KEY,
            "language": "fr",
            "region": "fr",
        },
        timeout=10,
    )

    data = response.json()

    if data["status"] != "OK":
        logger.warning(f"Distance API failed for: {origin} — status: {data['status']}")
        return None

    element = data["rows"][0]["elements"][0]

    if element["status"] != "OK":
        logger.warning(f"No route found: {origin} → {OFFICE_ADDRESS} — {element['status']}")
        return None

    # API returns meters, we convert to km rounded to 1 decimal
    distance_km = round(element["distance"]["value"] / 1000, 1)

    logger.info(f"Distance: {origin} → office = {distance_km} km ({transport_mode})")
    return distance_km


def validate_commute(distance_km: float, transport_mode: str) -> bool:
    """
    Checks if employee commute declaration is plausible.
    Compares distance against threshold for the transport mode.
    Motorized is always valid (not eligible for prime anyway).
    """
    if transport_mode == "motorized":
        return True

    if distance_km is None:
        return False

    threshold = DISTANCE_THRESHOLDS.get(transport_mode)
    if threshold is None:
        logger.warning(f"No threshold for transport mode: {transport_mode}")
        return False

    is_valid = distance_km <= threshold

    if not is_valid:
        logger.warning(
            f"Suspicious declaration: {distance_km} km by {transport_mode} "
            f"(threshold: {threshold} km)"
        )

    return is_valid


def _extract_component(components: list, component_type: str) -> str:
    """
    Internal helper — extracts a specific field from Google's
    address_components array by matching the type.
    Returns empty string if not found.
    """
    for component in components:
        if component_type in component["types"]:
            return component["long_name"]
    return ""