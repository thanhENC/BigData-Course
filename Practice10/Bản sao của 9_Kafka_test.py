from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
KAFKA_TOPIC = "MyTopic000"
KAFKA_SERVER = "localhost:9092"
# creating an instance of SparkSession
spark_session = SparkSession.builder.appName("Python Spark create RDD").getOrCreate()
# Subscribe to 1 topic
df = spark_session.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_SERVER).option("subscribe", KAFKA_TOPIC).load()
print("--*--"*15)
print(df.selectExpr("CAST(key AS STRING) as OUTPUT_KEY", "CAST(value AS STRING) OUTPUT_VALUE"))
