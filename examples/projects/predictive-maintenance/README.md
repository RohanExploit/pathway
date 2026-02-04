# Predictive Maintenance for Industrial Equipment with Pathway

This minimal end-to-end example shows how Pathway can be used to implement a predictive maintenance pipeline similar to the common Kafka/Flink/model-server architecture described in the prompt:

* **Ingestion**: Read sensor readings (temperature, vibration, pressure) from Kafka or MQTT.
* **Feature engineering**: Compute rolling statistics and simple frequency-domain hints on the stream using Pathway’s stateful engine.
* **Model scoring**: Apply an online model (here stubbed with a lightweight scoring UDF) to estimate failure probability.
* **Alerts & storage**: Publish alerts back to Kafka/MQTT and store results for dashboards.
* **Document Store**: Index maintenance manuals/logs to power technician Q&A.

## Files
- `pipeline.py`: Streaming pipeline reading from Kafka (swap to MQTT by changing connector) and emitting alerts.
- `docstore_example.py`: Tiny helper showing how to index maintenance PDFs/CSVs with Pathway Document Store for technician queries.

## Running the streaming pipeline
1) Set environment (example uses Upstash Kafka; replace with your broker or MQTT URI):
```bash
export KAFKA_ENDPOINT="your-broker:9092"
export KAFKA_USER="username"
export KAFKA_PASS="password"
export INPUT_TOPIC="sensors"
export ALERTS_TOPIC="maintenance-alerts"
```
2) Install requirements (Pathway already bundled in the repo):
```bash
pip install -e '.[dev]'
```
3) Run:
```bash
python pipeline.py
```
To use MQTT instead of Kafka, update the connector section in `pipeline.py` (see comments).

## Running the document store helper
Prepare a folder with maintenance manuals or logs (PDF, txt, csv). Then:
```bash
export MAINT_DOCS_PATH=/path/to/manuals
python docstore_example.py
```
This builds a vector index; you can adapt it to serve via the LLM xPack servers for technician Q&A.

## Notes
- The example keeps dependencies minimal and uses built-in Pathway connectors and stateful computations.
- Replace the stubbed scoring function with your trained model (e.g., ONNX, scikit-learn) as needed.
