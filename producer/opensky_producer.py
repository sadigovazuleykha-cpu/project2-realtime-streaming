"""
OpenSky Network -> Kafka producer.

Polls GET /states/all (optionally scoped to a bounding box), normalises each
state vector, and publishes one Kafka message per aircraft to topic
`aircraft_state_v1`, keyed by ICAO24 (preserves per-aircraft ordering
downstream, per the tech doc's partition-key choice).

Runs as a SINGLE pass on purpose — no infinite loop. Airflow (Step 4) calls
this on a schedule, which keeps us well inside OpenSky's daily credit budget
(see NOTE near the bottom).

Auth: OpenSky retired basic auth (username/password) in March 2026. This
uses the OAuth2 client-credentials flow. Credentials are picked up, in
order:
  1) OPENSKY_CREDENTIALS_FILE env var -> path to a downloaded credentials.json
  2) a credentials.json file sitting next to this script
  3) OPENSKY_CLIENT_ID / OPENSKY_CLIENT_SECRET env vars
  4) none of the above -> anonymous access (heavily rate-limited)

NEVER commit credentials.json — it's in .gitignore already, keep it that way.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

import requests
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("opensky_producer")

TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
STATES_URL = "https://opensky-network.org/api/states/all"

# Default assumes we're running INSIDE the docker-compose network (e.g. from
# Airflow). Testing from your host machine? Override to localhost:9092.
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "aircraft_state_v1")

# Optional bounding box (lamin, lamax, lomin, lomax) — leave unset to poll
# globally. A box keeps the payload smaller and costs fewer OpenSky credits.
BBOX = {
    "lamin": os.environ.get("BBOX_LAMIN"),
    "lamax": os.environ.get("BBOX_LAMAX"),
    "lomin": os.environ.get("BBOX_LOMIN"),
    "lomax": os.environ.get("BBOX_LOMAX"),
}

# Fixed field order of the OpenSky state-vector arrays. `category` (index 17)
# is only present on newer responses, so we pad defensively below.
STATE_FIELDS = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
    "spi", "position_source", "category",
]


def load_opensky_credentials() -> Tuple[Optional[str], Optional[str]]:
    """Finds client_id/client_secret from a credentials.json or env vars."""
    cred_path = os.environ.get("OPENSKY_CREDENTIALS_FILE")
    if not cred_path:
        default_path = Path(__file__).parent / "credentials.json"
        if default_path.exists():
            cred_path = str(default_path)

    if cred_path and Path(cred_path).exists():
        with open(cred_path) as f:
            data = json.load(f)
        log.info("Loaded OpenSky credentials from %s", cred_path)
        return data.get("clientId"), data.get("clientSecret")

    return os.environ.get("OPENSKY_CLIENT_ID"), os.environ.get("OPENSKY_CLIENT_SECRET")


class TokenManager:
    """Fetches and caches an OAuth2 client-credentials Bearer token."""

    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self._token: Optional[str] = None
        self._expires_at: float = 0.0

    def get_token(self) -> str:
        # 30s safety margin so we never send a token that expires mid-flight
        if self._token and time.time() < self._expires_at - 30:
            return self._token
        return self._refresh()

    def _refresh(self) -> str:
        resp = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=15,
        )
        resp.raise_for_status()
        payload = resp.json()
        self._token = payload["access_token"]
        expires_in = payload.get("expires_in", 1800)
        self._expires_at = time.time() + expires_in
        log.info("OpenSky token refreshed, valid for %ss", expires_in)
        return self._token


def fetch_states(token: Optional[str]) -> list:
    """Calls /states/all and returns a list of normalised state-vector dicts."""
    params = {k: v for k, v in BBOX.items() if v is not None}
    headers = {"Authorization": f"Bearer {token}"} if token else {}

    resp = requests.get(STATES_URL, params=params, headers=headers, timeout=20)
    if resp.status_code == 401:
        raise RuntimeError("OpenSky returned 401 — token invalid or expired")
    resp.raise_for_status()

    body = resp.json()
    server_time = body.get("time")
    raw_states = body.get("states") or []

    records = []
    for row in raw_states:
        padded = row + [None] * (len(STATE_FIELDS) - len(row))
        state = dict(zip(STATE_FIELDS, padded))
        if not state["icao24"]:
            continue  # defensive: skip malformed rows

        event_time = state["time_position"] or server_time
        records.append({
            "icao24": state["icao24"].strip(),
            "callsign": (state["callsign"] or "").strip() or None,
            "origin_country": state["origin_country"],
            "longitude": state["longitude"],
            "latitude": state["latitude"],
            "baro_altitude": state["baro_altitude"],
            "velocity": state["velocity"],
            "true_track": state["true_track"],
            "vertical_rate": state["vertical_rate"],
            "on_ground": state["on_ground"],
            "event_time": (
                datetime.fromtimestamp(event_time, tz=timezone.utc).isoformat()
                if event_time else None
            ),
        })
    return records


def run_once() -> int:
    """Single ingestion pass: fetch -> normalise -> publish. Returns count."""
    client_id, client_secret = load_opensky_credentials()
    token = None
    if client_id and client_secret:
        token = TokenManager(client_id, client_secret).get_token()
    else:
        log.warning("No OpenSky credentials found — polling anonymously (heavily rate-limited)")

    records = fetch_states(token)
    log.info("Fetched %d state vectors", len(records))

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    try:
        for record in records:
            # Keyed by ICAO24 -> all updates for one aircraft land on the
            # same partition, preserving per-aircraft order downstream.
            producer.send(KAFKA_TOPIC, key=record["icao24"], value=record)
        producer.flush()
    finally:
        producer.close()

    log.info("Published %d records to topic '%s'", len(records), KAFKA_TOPIC)
    return len(records)


# NOTE on cadence: OpenSky's credit system allows ~400 requests/day
# anonymous, ~4000/day authenticated, ~8000/day for active feeders. The doc
# says data refreshes globally every 5-10s, but polling that often would
# blow the daily budget fast. In Step 4 we'll schedule this via Airflow
# every 1-2 minutes instead — plenty for a demo/teaching pipeline.

if __name__ == "__main__":
    run_once()