"""
AtmoSync - send simulated container readings into Kafka.

This reuses the simulator from simulator_final.py (keep both files in the
same folder). Instead of printing each message, it publishes it to a Kafka topic.

Run:
  python simulator_kafka.py
  python simulator_kafka.py --duration 60
  python simulator_kafka.py --faults-per-day 500     (see faults quickly)
Stop: Ctrl+C
"""

import argparse
import json
import os
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

from simulator_final import (
    LIVE_INTERVAL,
    NUM_CONTAINERS,
    make_container,
    make_event,
    update_container,
)

TOPIC = "iot.container.telemetry"


def build_kafka_config():
    """
    Connection settings come from environment variables, so the same script
    works with local Kafka (Docker) or a cloud Kafka (e.g. Confluent Cloud).

      KAFKA_BOOTSTRAP   server address (default: localhost:9092)
      KAFKA_API_KEY     cloud API key    (leave unset for local Kafka)
      KAFKA_API_SECRET  cloud API secret (leave unset for local Kafka)
    """
    conf = {"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")}
    key = os.environ.get("KAFKA_API_KEY")
    secret = os.environ.get("KAFKA_API_SECRET")
    if key and secret:
        conf.update({
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": key,
            "sasl.password": secret,
        })
    return conf

sent_ok = 0
errors = 0


def on_delivery(err, msg):
    """Kafka calls this for every message to say whether it arrived."""
    global sent_ok, errors
    if err is not None:
        errors += 1
        if errors <= 3:   # don't flood the screen
            print(f"Delivery failed: {err}")
    else:
        sent_ok += 1


def main():
    p = argparse.ArgumentParser(description="Send simulated readings to Kafka")
    p.add_argument("--duration", type=int, default=0,
                   help="stop after this many seconds (0 = until Ctrl+C)")
    p.add_argument("--interval", type=float, default=LIVE_INTERVAL,
                   help="seconds between readings per container")
    p.add_argument("--faults-per-day", type=float, default=1.0,
                   help="average cooling faults per container per day")
    args = p.parse_args()

    fault_chance = args.faults_per_day / (86400 / args.interval)
    kafka_conf = build_kafka_config()
    producer = Producer(kafka_conf)
    containers = [make_container(i + 1) for i in range(NUM_CONTAINERS)]

    print(f"Connecting to {kafka_conf['bootstrap.servers']}")
    print(f"Sending {NUM_CONTAINERS} containers to topic '{TOPIC}' "
          f"every {args.interval}s. Press Ctrl+C to stop.")

    start = time.time()
    last_report = start
    try:
        while True:
            round_start = time.time()
            for c in containers:
                update_container(c, args.interval, fault_chance)
                event = make_event(c, datetime.now(timezone.utc))
                producer.produce(
                    TOPIC,
                    key=event["container_id"],   # same container -> same partition
                    value=json.dumps(event),
                    on_delivery=on_delivery,
                )
            producer.poll(0)   # let Kafka report delivery results

            now = time.time()
            if now - last_report >= 10:
                print(f"  delivered={sent_ok:,}  failed={errors:,}")
                last_report = now
            if args.duration and now - start >= args.duration:
                break
            time.sleep(max(0, args.interval - (time.time() - round_start)))
    except KeyboardInterrupt:
        print("\nStopping...")

    producer.flush(10)   # make sure everything queued is actually sent
    print(f"Done. delivered={sent_ok:,}  failed={errors:,}")


if __name__ == "__main__":
    main()
