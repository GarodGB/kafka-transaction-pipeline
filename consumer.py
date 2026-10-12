import json
import psycopg2
from psycopg2.extras import RealDictCursor
from kafka import KafkaConsumer
from cleaning import clean_row

TOPIC = "transactions"

# 1. connect to Postgres (the database box)
conn = psycopg2.connect(
    host="localhost", port=5433,
    user="admin", password="admin", dbname="transactions",
)

# 2. connect to Kafka (the post office) and listen to the mailbox
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    group_id="cleaning-group",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
)
print("Listening on topic 'transactions'... (Ctrl+C to stop)")

for message in consumer:
    # 3. read the message -> get the id
    try:
        txn_id = json.loads(message.value)["id"]
    except (ValueError, KeyError, TypeError):
        print(f"Skipping bad message: {message.value!r}")
        consumer.commit()
        continue

    try:
        # 4. fetch the dirty row from raw_transactions
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM raw_transactions WHERE id = %s", (txn_id,))
            raw = cur.fetchone()
        if raw is None:
            print(f"id={txn_id} not found in raw_transactions, skipping")
            consumer.commit()
            continue

        # 5. clean it
        row = {k.upper(): v for k, v in raw.items()}
        clean = clean_row(row)
        clean["id"] = txn_id

        # 6. save it in transactions_cleaned
        cols = list(clean.keys())
        sql = (
            f"INSERT INTO transactions_cleaned ({', '.join(cols)}) "
            f"VALUES ({', '.join(['%s'] * len(cols))}) "
            f"ON CONFLICT (id) DO NOTHING"
        )
        with conn.cursor() as cur:
            cur.execute(sql, [clean[c] for c in cols])
        conn.commit()

        # 7. tell Kafka "done" (move the sticker)
        consumer.commit()
        print(f"id={txn_id} cleaned and saved: "
              f"{clean['merchant_name']} | {clean['merchant_city']} | {clean['txn_amount']}")

    except Exception as e:
        conn.rollback()
        print(f"ERROR on id={txn_id}: {e}  (not committed, will retry next time)")