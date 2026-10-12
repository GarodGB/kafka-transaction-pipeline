import psycopg2
from psycopg2.extras import RealDictCursor
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, IntegerType

KAFKA = "kafka:29092"     # Kafka address INSIDE Docker
TOPIC = "transactions"
DB = dict(host="postgres", port=5432, user="admin", password="admin", dbname="transactions")

# 1. start Spark (+ download the Kafka connector the first time)
spark = (SparkSession.builder
         .appName("spark-stream-cleaner")
         .master("local[*]")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")



# 2. read the Kafka topic 
stream = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", KAFKA)
          .option("subscribe", TOPIC)
          .option("startingOffsets", "earliest")
          .load())



# 3. unpack each message: bytes -> text -> {"id": 1} -> id   (bad messages -> null -> removed)
schema = StructType([StructField("id", IntegerType())])
ids = (stream
       .selectExpr("CAST(value AS STRING) AS json")
       .select(from_json(col("json"), schema).alias("msg"))
       .select("msg.id")
       .where(col("id").isNotNull()))



# 4. the work for ONE piece (partition) of a batch -- runs on a Spark worker
def save_partition(rows):
    from cleaning import clean_row
    id_list = [r["id"] for r in rows]
    if not id_list:
        return
    conn = psycopg2.connect(**DB)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM raw_transactions WHERE id = ANY(%s)", (id_list,))
        raw_rows = cur.fetchall()
    with conn.cursor() as cur:
        for raw in raw_rows:
            clean = clean_row({k.upper(): v for k, v in raw.items()})
            clean["id"] = raw["id"]
            cols = list(clean.keys())
            sql = (f"INSERT INTO transactions_cleaned ({', '.join(cols)}) "
                   f"VALUES ({', '.join(['%s'] * len(cols))}) "
                   f"ON CONFLICT (id) DO NOTHING")
            cur.execute(sql, [clean[c] for c in cols])
    conn.commit()
    conn.close()
    
    
    
    
# 5. what to do with each micro-batch
def process_batch(batch_df, batch_id):
    n = batch_df.count()
    if n == 0:
        return
    batch_df.foreachPartition(save_partition)
    print(f"batch {batch_id}: {n} id(s) cleaned and saved")
    
    
    
    
    
# 6. start the stream: a new batch every 5 seconds
query = (ids.writeStream
         .foreachBatch(process_batch)
         .option("checkpointLocation", "/tmp/spark-checkpoint")
         .trigger(processingTime="5 seconds")
         .start())
print("Spark streaming started, listening to 'transactions'... (Ctrl+C to stop)")
query.awaitTermination()