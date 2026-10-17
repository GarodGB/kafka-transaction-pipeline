"""Listen to pipeline-events: check each run event, handle it once, reject bad ones."""
import json
import logging
import os
import signal
from dotenv import load_dotenv
load_dotenv()

import psycopg2
from kafka import KafkaConsumer, KafkaProducer

# ---------- settings (config file + environment) ----------
KAFKA = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
with open(os.getenv("PIPELINE_CONFIG", "config/pipeline_config.json")) as f:
    k = json.load(f)["kafka"]
DB = dict(host=os.getenv("PGHOST", "localhost"), port=os.getenv("PGPORT", "5433"),
          dbname=os.getenv("PGDATABASE", "transactions"),
          user=os.getenv("PGUSER", "admin"), password=os.environ["PGPASSWORD"])

# ---------- structured logs: one JSON line per thing that happens ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
def log(event, **fields):
    logging.info(json.dumps({"event": event, **fields}))

# ---------- 1. check the letter ----------
REQUIRED = {"run_id": str, "rows_read": int, "rows_cleaned": int, "rows_dropped": int,
            "rows_flagged": int, "flag_counts": dict, "target_table": str,
            "schema_version": str, "timestamp": str}
ALLOWED_TABLES = {"transactions_cleaned"}

def validate(raw_bytes):
    """Return (event, None) if the letter is OK, or (None, reason) if not."""
    try:
        event = json.loads(raw_bytes)
    except (ValueError, TypeError):
        return None, "not valid JSON"
    if not isinstance(event, dict):
        return None, "not a JSON object"
    for field, typ in REQUIRED.items():
        if field not in event:
            return None, f"missing field: {field}"
        if not isinstance(event[field], typ) or isinstance(event[field], bool):
            return None, f"wrong type: {field}"
    if event["target_table"] not in ALLOWED_TABLES:
        return None, f"unknown table: {event['target_table']}"
    if event["rows_read"] != event["rows_cleaned"] + event["rows_dropped"]:
        return None, "rows_read != rows_cleaned + rows_dropped"
    return event, None

# ---------- 2. the real work (placeholder for now) ----------
def downstream_hook(event):
    """Here a real system would refresh a report, notify a team, start the next job..."""
    log("downstream_hook", run_id=event["run_id"], note="placeholder, nothing to do yet")

# ---------- 3. bad letter -> dead letter topic + rejected_events ----------
def reject(msg, reason, conn, dlq):
    dlq.send(k["dead_letter_topic"], key=msg.key, value=msg.value,
             headers=[("reason", reason.encode())]).get(timeout=10)
    with conn, conn.cursor() as cur:
        cur.execute("INSERT INTO rejected_events (reason, payload) VALUES (%s, %s)",
                    (reason, msg.value.decode("utf-8", "replace") if msg.value else None))
        cur.execute("INSERT INTO audit_log (stage, event, details) VALUES ('consume', 'rejected', %s)",
                    (json.dumps({"reason": reason, "offset": msg.offset}),))
    log("rejected", reason=reason, offset=msg.offset)

class CountMismatch(Exception):
    pass

# ---------- 4. handle one letter ----------
def handle(msg, conn, dlq):
    event, reason = validate(msg.value)
    if reason:
        reject(msg, reason, conn, dlq)
        return
    run_id = event["run_id"]
    try:
        with conn, conn.cursor() as cur:          # one transaction
            cur.execute("""INSERT INTO processed_events (run_id, rows_cleaned) VALUES (%s, %s)
                           ON CONFLICT (run_id) DO NOTHING""", (run_id, event["rows_cleaned"]))
            if cur.rowcount == 0:                  # already in the table -> seen before
                cur.execute("INSERT INTO audit_log (run_id, stage, event) VALUES (%s, 'consume', 'duplicate_skipped')",
                            (run_id,))
                log("duplicate_skipped", run_id=run_id, offset=msg.offset)
                return

            cur.execute(f"SELECT COUNT(*) FROM {event['target_table']} WHERE run_id = %s", (run_id,))
            db_count = cur.fetchone()[0]
            if db_count != event["rows_cleaned"]:
                raise CountMismatch(f"letter says {event['rows_cleaned']} rows, table has {db_count}")

            downstream_hook(event)
            cur.execute("INSERT INTO audit_log (run_id, stage, event, details) VALUES (%s, 'consume', 'processed', %s)",
                        (run_id, json.dumps({"rows_cleaned": db_count, "offset": msg.offset})))
        log("processed", run_id=run_id, rows_cleaned=db_count, offset=msg.offset)
    except CountMismatch as e:                     # transaction was rolled back
        reject(msg, str(e), conn, dlq)

# ---------- 5. the listening loop ----------
def main():
    running = {"on": True}
    def stop(signum, frame):
        log("shutdown_requested", signal=signum)
        running["on"] = False
    signal.signal(signal.SIGINT, stop)             # Ctrl + C
    signal.signal(signal.SIGTERM, stop)            # docker stop

    conn = psycopg2.connect(**DB)
    dlq = KafkaProducer(bootstrap_servers=KAFKA, acks="all")
    consumer = KafkaConsumer(
        k["topic"],
        bootstrap_servers=KAFKA,
        group_id=k["consumer_group"],
        enable_auto_commit=False,                  # we say "done" ourselves, after the work
        auto_offset_reset="earliest",              # first time: start from the oldest letter
        max_poll_records=1,                        # one letter at a time
    )
    log("started", topic=k["topic"], group=k["consumer_group"])
    try:
        while running["on"]:
            for _, messages in consumer.poll(timeout_ms=1000).items():
                for msg in messages:
                    handle(msg, conn, dlq)
                    consumer.commit()              # only AFTER the work is saved
    finally:
        consumer.close()
        dlq.close()
        conn.close()
        log("stopped")

if __name__ == "__main__":
    main()