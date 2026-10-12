from pyspark.sql import SparkSession

# 1. start Spark
spark = SparkSession.builder.appName("hello-spark").master("local[*]").getOrCreate()
print("Spark version:", spark.version)

# 2. make a small table
df = spark.createDataFrame(
    [(1, "ZAATAR W ZEIT", 176711.0), (2, "VISTAPRINT", 1250.0)],
    ["id", "merchant", "amount"],
)
df.show()

# 3. a small calculation
print("Total amount:", df.groupBy().sum("amount").collect()[0][0])

spark.stop()