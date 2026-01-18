import os
import json
import time
import uuid
import random
from datetime import datetime, timezone

from azure.eventhub import EventData
from azure.eventhub import EventHubProducerClient
from dotenv import load_dotenv


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def random_user_id() -> str:
    return f"user{random.randint(1, 50):03d}"


def random_merchant_id() -> str:
    return f"merchant{random.randint(1, 30):03d}"


def choose_country() -> str:
    # mostly CH/EU to look realistic
    return random.choices(
        population=["CH", "DE", "FR", "IT", "PL", "GB", "US"],
        weights=[50, 12, 10, 8, 8, 6, 6],
        k=1,
    )[0]


def choose_channel() -> str:
    return random.choices(
        population=["pos", "ecom", "atm"],
        weights=[60, 35, 5],
        k=1,
    )[0]


def generate_normal_amount() -> float:
    # typical payments: 5–200
    amount = random.lognormvariate(mu=3.4, sigma=0.6)  # ~ 30-60 typical
    return round(min(max(amount, 2.5), 250.0), 2)


def generate_anomalous_amount() -> float:
    # suspicious big amount
    amount = random.uniform(800, 5000)
    return round(amount, 2)


def generate_transaction(anomalous: bool) -> dict:
    user_id = random_user_id()
    base = {
        "transactionId": str(uuid.uuid4()),
        "userId": user_id,
        "merchantId": random_merchant_id(),
        "amount": generate_anomalous_amount() if anomalous else generate_normal_amount(),
        "currency": "CHF",
        "country": choose_country() if not anomalous else random.choice(["US", "GB"]),
        "channel": choose_channel() if not anomalous else random.choice(["ecom", "atm"]),
        "timestamp": utc_now_iso(),
        "deviceId": f"dev{random.randint(1, 200):04d}",
    }
    # optional fields useful later
    base["isInjectedAnomaly"] = anomalous
    return base


def main():
    load_dotenv()

    conn_str = os.getenv("EVENTHUB_CONNECTION_STRING")
    hub_name = os.getenv("EVENTHUB_NAME", "transactionsStream")

    if not conn_str:
        raise SystemExit("Missing EVENTHUB_CONNECTION_STRING in environment / .env")

    eps = float(os.getenv("EVENTS_PER_SECOND", "2"))
    anomaly_rate = float(os.getenv("ANOMALY_RATE", "0.05"))

    producer = EventHubProducerClient.from_connection_string(
        conn_str=conn_str,
        eventhub_name=hub_name,
    )

    sleep_s = 1.0 / max(eps, 0.1)

    print("Starting producer...")
    print(f"EventHub: {hub_name}")
    print(f"Events/s: {eps}")
    print(f"Anomaly rate: {anomaly_rate}")

    try:
        with producer:
            while True:
                anomalous = random.random() < anomaly_rate
                event = generate_transaction(anomalous)
                body = json.dumps(event)

                producer.send_batch([EventData(body)])

                tag = "ANOM" if anomalous else "OK"
                print(f"[{tag}] {event['timestamp']} user={event['userId']} amount={event['amount']} {event['currency']}")

                time.sleep(sleep_s)

    except KeyboardInterrupt:
        print("Stopped by user.")


if __name__ == "__main__":
    main()
