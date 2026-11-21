"""
AtmoSync - Step 1: IoT container simulator (prints to screen only).

Each container has a temperature and humidity that wander slowly.
Occasionally a "cooling fault" is injected so the temperature drifts up,
which is the situation AtmoSync is supposed to detect later.

Run:  python simulator.py
Stop: Ctrl+C
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone

# ---- Settings you can play with -------------------------------------------
NUM_CONTAINERS = 3        # how many containers to simulate
SETPOINT_C = 5.0          # target temperature for avocados (about 5-7 C)
FAULT_CHANCE = 0.01       # chance per reading that a cooling fault starts
INTERVAL_SECONDS = 1      # seconds between rounds of readings
# ----------------------------------------------------------------------------


def make_container(index):
    """Create the starting state of one container."""
    return {
        "container_id": f"C-A{index:02d}",
        "commodity": "avocado",
        "temp_c": SETPOINT_C,
        "humidity_pct": 90.0,
        "fault": False,          # is the cooling unit currently failing?
        # starting position (somewhere off the Tamil Nadu coast) and a slow movement
        "lat": 13.08 + random.uniform(-0.5, 0.5),
        "lon": 80.27 + random.uniform(-0.5, 0.5),
    }


def update_container(c):
    """Move the container's state forward by one time step."""
    # Randomly start a cooling fault
    if not c["fault"] and random.random() < FAULT_CHANCE:
        c["fault"] = True

    if c["fault"]:
        # Cooling is failing: temperature creeps upward
        c["temp_c"] += random.uniform(0.05, 0.3)
        # Technicians "fix" it eventually
        if c["temp_c"] > SETPOINT_C + 8 and random.random() < 0.05:
            c["fault"] = False
    else:
        # Normal: small random wiggle, gently pulled back toward the setpoint
        c["temp_c"] += random.gauss(0, 0.05) + (SETPOINT_C - c["temp_c"]) * 0.05

    # Humidity does a small random walk, kept between 60 and 100 percent
    c["humidity_pct"] += random.gauss(0, 0.3)
    c["humidity_pct"] = max(60.0, min(100.0, c["humidity_pct"]))

    # The container slowly moves
    c["lat"] += 0.001
    c["lon"] += 0.001


def make_event(c):
    """Turn the container's state into one sensor message (a JSON-ready dict)."""
    return {
        "event_id": str(uuid.uuid4()),
        "container_id": c["container_id"],
        "commodity": c["commodity"],
        "ts": datetime.now(timezone.utc).isoformat(),
        "temp_c": round(c["temp_c"], 2),
        "humidity_pct": round(c["humidity_pct"], 1),
        "vibration_g": round(abs(random.gauss(0.3, 0.1)), 2),
        "lat": round(c["lat"], 4),
        "lon": round(c["lon"], 4),
    }


def main():
    containers = [make_container(i + 1) for i in range(NUM_CONTAINERS)]
    print(f"Simulating {NUM_CONTAINERS} containers. Press Ctrl+C to stop.\n")

    try:
        while True:
            for c in containers:
                update_container(c)
                event = make_event(c)
                print(json.dumps(event))
            time.sleep(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()