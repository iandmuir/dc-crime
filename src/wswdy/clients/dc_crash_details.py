"""DC Crash Details client — fetches parties (drivers/passengers/peds/cyclists)
for a given list of crash CRIMEIDs.

Endpoint: layer 25 of the same Public_Safety_WebMercator MapServer that hosts
the main crashes layer (layer 24). Joined on CRIMEID. The parties layer has
~885k historical rows total; we only fetch parties for crashes already in
our 30-day crashes window (~1300 crashes × ~2-3 parties each).

Schema mapping (party feature attributes → crash_parties row):

  PERSONID           -> id
  CRIMEID            -> crimeid (joins to crashes.id)
  CCN                -> ccn
  PERSONTYPE         -> person_type ('Driver' / 'Passenger' / 'Pedestrian' /
                         'Bicyclist' / 'Other' / 'Unknown' / 'Streetcar ')
  AGE                -> age (integer; 0 means unknown)
  FATAL              -> fatal     (Y/N → 1/0)
  MAJORINJURY        -> major_injury
  MINORINJURY        -> minor_injury
  VEHICLEID          -> vehicle_id
  INVEHICLETYPE      -> vehicle_type (raw string; humanized at display time)
  LICENSEPLATESTATE  -> license_state
  TICKETISSUED       -> ticket_issued
  IMPAIRED           -> impaired
  SPEEDING           -> speeding
"""
import json

import httpx

DEFAULT_URL = (
    "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/"
    "Public_Safety_WebMercator/MapServer/25/query"
)
# WHERE CRIMEID IN ('a','b','c',...) — keep batches well under URL length
# limits. Each ID is ~12 chars + quotes/comma; 50 IDs ≈ 750 chars in the
# WHERE clause, comfortably within typical 2KB query limits.
DEFAULT_BATCH_SIZE = 50


def _yn(v) -> int:
    """ArcGIS Y/N strings → 0/1 ints."""
    return 1 if str(v or "").strip().upper() == "Y" else 0


def _coalesce_int(v) -> int:
    return int(v) if v is not None else 0


def _feature_to_record(f: dict) -> dict | None:
    a = f.get("attributes") or {}
    person_id = str(a.get("PERSONID") or "").strip()
    crimeid = str(a.get("CRIMEID") or "").strip()
    if not person_id or not crimeid:
        return None
    return {
        "id": person_id,
        "crimeid": crimeid,
        "ccn": a.get("CCN"),
        "person_type": a.get("PERSONTYPE"),
        "age": _coalesce_int(a.get("AGE")),
        "fatal": _yn(a.get("FATAL")),
        "major_injury": _yn(a.get("MAJORINJURY")),
        "minor_injury": _yn(a.get("MINORINJURY")),
        "vehicle_id": a.get("VEHICLEID"),
        "vehicle_type": a.get("INVEHICLETYPE"),
        "license_state": a.get("LICENSEPLATESTATE"),
        "ticket_issued": _yn(a.get("TICKETISSUED")),
        "impaired": _yn(a.get("IMPAIRED")),
        "speeding": _yn(a.get("SPEEDING")),
    }


async def fetch_parties_for_crashes(
    *,
    crimeids: list[str],
    feed_url: str = DEFAULT_URL,
    batch_size: int = DEFAULT_BATCH_SIZE,
    timeout_s: float = 30.0,
) -> list[dict]:
    """Fetch all parties whose CRIMEID is in `crimeids`. Batched to keep URLs short."""
    if not crimeids:
        return []
    out: list[dict] = []
    seen_ids: set[str] = set()
    async with httpx.AsyncClient(timeout=timeout_s) as client:
        for i in range(0, len(crimeids), batch_size):
            chunk = crimeids[i : i + batch_size]
            # ArcGIS WHERE wants single-quoted strings; CRIMEIDs are pure
            # numeric strings in DC's data, so escaping is overkill but
            # we still wrap defensively.
            in_list = ",".join(f"'{cid}'" for cid in chunk)
            params = {
                "where": f"CRIMEID IN ({in_list})",
                "outFields": "*",
                "f": "json",
                "resultRecordCount": 2000,
            }
            r = await client.get(feed_url, params=params)
            r.raise_for_status()
            data = r.json()
            for feature in data.get("features") or []:
                rec = _feature_to_record(feature)
                if rec is None or rec["id"] in seen_ids:
                    continue
                seen_ids.add(rec["id"])
                out.append(rec)
    return out


# ----- Display-time humanization -----------------------------------------

# Junk values DC has somehow populated this column with — nothing we should
# show to a subscriber. Compared lowercase.
_VEHICLE_JUNK = frozenset({
    "", "0", "none", "unknown", "other",
    "computer hardware/ software", "credit/ debit cards", "drugs/ narcotics",
    "firearms", "jewelry / precious metals / gems", "clothes",
    "childcare/daycare",
})

# Map raw vehicle strings to short, friendly labels. Compared lowercase.
# Handles all the variants ("MOTOR CYCLE" / "Motor Cycle" / "motor cycle")
# uniformly via the lower() lookup.
_VEHICLE_HUMAN = {
    # Cars
    "passenger car/station wagon/jeep": "Car",
    "passenger car/automobile": "Car",
    "other small passenger": "Car",
    # SUVs
    "sport utility vehicle": "SUV",
    "suv (sport utility vehicle)": "SUV",
    "suv (sports utility vehicle)": "SUV",
    # Trucks
    "pickup truck": "Pickup",
    "single-unit truck (2 axles)": "Truck",
    "single-unit truck (3 or more axles)": "Truck",
    "large/heavy truck": "Truck",
    "truck, axles unknown": "Truck",
    "other small/light truck": "Truck",
    "truck tractor (bobtail)": "Truck",
    # Vans
    "cargo van": "Van",
    "mini-van (personal use, up to 8 seats)": "Minivan",
    "passenger van": "Van",
    "large passenger van": "Van",
    # Motorcycles
    "2-wheeled motorcycle": "Motorcycle",
    "3-wheeled motorcycle": "Motorcycle",
    "motor cycle": "Motorcycle",
    "autocycle": "Motorcycle",
    # Mopeds / scooters
    "moped/scooter": "Moped",
    "moped or motorized bicycle": "Moped",
    # Buses
    "bus": "Bus",
    "charter/tour bus": "Bus",
    "transit bus": "Bus",
    "intercity bus": "Bus",
    "other bus": "Bus",
    "shuttle bus": "Bus",
    "school bus": "School bus",
    # Off-road / specialty
    "atv (all terrain vehicle)": "ATV",
    "all-terrain vehicle/cycle (atv/atc)": "ATV",
    "recreational off-highway vehicle (rov)": "ATV",
    "snowmobile": "Snowmobile",
    "snow mobile": "Snowmobile",
    "low speed vehicle": "LSV",
    "golf cart": "Golf cart",
    # RV / trailer
    "motorhome/camper/rv (recreational vehicle)": "RV",
    "motor home/recreational vehicle": "RV",
    "recreational vehicles": "RV",
    "trailer": "Trailer",
    "limo": "Limo",
    # Construction / farm
    "heavy construction/industrial equipment": "Construction",
    "construction/industrial equipment": "Construction",
    "construction equipment (backhoe, bulldozer, etc.)": "Construction",
    "farm equipment": "Farm equipment",
    "farm equipment (tractor, combine, harvester, etc.)": "Farm equipment",
    # Misc
    "other vehicle": "Other vehicle",
    "watercraft/boat": "Boat",
    "aircraft": "Aircraft",
}


def humanize_vehicle(raw: str | None) -> str | None:
    """Return a short friendly label for a vehicle type, or None if junk/unknown."""
    if not raw:
        return None
    key = raw.strip().lower()
    if key in _VEHICLE_JUNK:
        return None
    return _VEHICLE_HUMAN.get(key, raw.strip())


_PLATE_JUNK = frozenset({"", "0", "unknown", "uk", "ou", "am", "none"})


def humanize_plate_state(raw: str | None) -> str | None:
    """Return the license plate state code or None if unknown."""
    if not raw:
        return None
    key = raw.strip().lower()
    if key in _PLATE_JUNK:
        return None
    return raw.strip()


def party_is_interesting(party: dict) -> bool:
    """Decide whether to surface a party in the popup. We always show drivers
    (they're the actor in the crash), pedestrians, and cyclists. We hide
    passengers and 'Other' parties unless they were injured or had a
    notable factor (impaired/speeding) that other fields don't already
    expose."""
    pt = (party.get("person_type") or "").strip()
    if pt in ("Driver", "Pedestrian", "Bicyclist"):
        return True
    if (party.get("fatal") or 0) or (party.get("major_injury") or 0) \
       or (party.get("minor_injury") or 0):
        return True
    if (party.get("impaired") or 0) or (party.get("speeding") or 0):
        return True
    return False
