import os
import math
from typing import Iterable

import pathway as pw

# To use advanced features with Pathway Scale, set your key. Community users can comment this out.
pw.set_license_key("demo-license-key-with-telemetry")


# ----------- Ingestion -----------
# Switch between Kafka and MQTT by toggling the connector block.

BROKER_ENDPOINT = os.environ.get("KAFKA_ENDPOINT", "talented-cow-10356-eu1-kafka.upstash.io:9092")
KAFKA_USER = os.environ.get("KAFKA_USER")
KAFKA_PASS = os.environ.get("KAFKA_PASS")
INPUT_TOPIC = os.environ.get("INPUT_TOPIC", "sensors")
ALERTS_TOPIC = os.environ.get("ALERTS_TOPIC", "maintenance-alerts")


class SensorSchema(pw.Schema):
    ts: int  # unix ns
    machine_id: str
    temperature: float
    vibration: float
    pressure: float


# Kafka connector (default)
def read_stream() -> pw.Table:
    rdkafka_settings = {
        "bootstrap.servers": BROKER_ENDPOINT,
        "security.protocol": "sasl_ssl",
        "sasl.mechanism": "SCRAM-SHA-256",
        "group.id": "pw-maintenance",
        "session.timeout.ms": "6000",
        "sasl.username": KAFKA_USER,
        "sasl.password": KAFKA_PASS,
    }
    return pw.io.kafka.read(
        rdkafka_settings,
        topic=INPUT_TOPIC,
        schema=SensorSchema,
        format="json",
        autocommit_duration_ms=1000,
    )


# MQTT alternative (uncomment to use):
# def read_stream() -> pw.Table:
#     return pw.io.mqtt.read(
#         uri="mqtt://localhost:1883/?client_id=pathway-demo",
#         topic="sensors",
#         format="json",
#         schema=SensorSchema,
#     )


# ----------- Feature Engineering -----------

WINDOW_NS = 60 * 1_000_000_000  # 60 seconds sliding window


def rolling_features(stream: pw.Table) -> pw.Table:
    # Keep only the last WINDOW_NS worth of data per machine using a sliding window join pattern.
    latest_ts = stream.groupby(stream.machine_id).reduce(last_ts=pw.reducers.max(stream.ts))
    windowed = stream.join(
        latest_ts,
        pw.left.machine_id == pw.right.machine_id,
    ).filter(pw.left.ts >= pw.right.last_ts - WINDOW_NS)

    agg = windowed.groupby(windowed.machine_id).reduce(
        count=pw.reducers.count(),
        mean_temp=pw.reducers.mean(windowed.temperature),
        mean_vib=pw.reducers.mean(windowed.vibration),
        mean_press=pw.reducers.mean(windowed.pressure),
        max_vib=pw.reducers.max(windowed.vibration),
        std_vib=pw.reducers.std(windowed.vibration),
    )

    # Simple frequency hint: ratio of max vibration to mean (proxy for peak prominence)
    features = agg.select(
        machine_id=agg.machine_id,
        mean_temp=agg.mean_temp,
        mean_vib=agg.mean_vib,
        mean_press=agg.mean_press,
        max_vib=agg.max_vib,
        std_vib=agg.std_vib,
        peak_ratio=agg.max_vib / (agg.mean_vib + 1e-6),
        sample_count=agg.count,
    )
    return features


# ----------- Scoring -----------


def simple_failure_score(mean_temp: float, peak_ratio: float, std_vib: float) -> float:
    # Replace with a call to your trained model (e.g., ONNXRuntime / sklearn).
    # Here we craft a lightweight heuristic combining temp and vibration stats.
    temp_risk = max(0.0, (mean_temp - 80.0) / 40.0)
    vib_risk = min(1.0, peak_ratio / 5.0 + std_vib / 10.0)
    return max(0.0, min(1.0, 0.4 * temp_risk + 0.6 * vib_risk))


def score(features: pw.Table) -> pw.Table:
    scored = features.select(
        machine_id=features.machine_id,
        score=pw.apply(simple_failure_score, features.mean_temp, features.peak_ratio, features.std_vib),
        mean_temp=features.mean_temp,
        mean_vib=features.mean_vib,
        mean_press=features.mean_press,
        sample_count=features.sample_count,
    )
    return scored.select(
        *pw.this,
        alert=pw.this.score >= 0.7,
    )


# ----------- Outputs -----------


def write_outputs(results: pw.Table) -> None:
    # Publish alerts back to Kafka (swap to MQTT similarly).
    if KAFKA_USER and KAFKA_PASS:
        rdkafka_settings = {
            "bootstrap.servers": BROKER_ENDPOINT,
            "security.protocol": "sasl_ssl",
            "sasl.mechanism": "SCRAM-SHA-256",
            "sasl.username": KAFKA_USER,
            "sasl.password": KAFKA_PASS,
        }
        pw.io.kafka.write(
            results,
            rdkafka_settings,
            topic=ALERTS_TOPIC,
            format="json",
        )
    # Also keep a CSV log for dashboards.
    pw.io.csv.write(results, "maintenance_alerts.csv")


if __name__ == "__main__":
    stream = read_stream()
    features = rolling_features(stream)
    results = score(features)
    write_outputs(results)
    pw.run()

