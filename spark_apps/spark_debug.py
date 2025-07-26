# spark_apps/spark_debug.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def debug_kafka_stream():
    """
    Kết nối tới Kafka và in ra nội dung JSON thô để kiểm tra.
    """
    spark = SparkSession.builder \
        .appName("KafkaDebug") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR") # Chỉ hiện lỗi để dễ nhìn

    KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
    # CHỈ ĐỌC TỪ 1 TOPIC ĐỂ GỠ LỖI
    TOPIC_TO_DEBUG = "server_docker.testdb.dbo.Customers"

    print(f"--- Bắt đầu chế độ Debug cho topic: {TOPIC_TO_DEBUG} ---")

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC_TO_DEBUG) \
        .option("startingOffsets", "latest") \
        .load()

    # Chỉ chọn cột 'value' và chuyển nó sang dạng chuỗi để in ra
    raw_json_df = kafka_df.select(
        col("value").cast("string").alias("raw_json_from_kafka")
    )

    # In ra console
    query = raw_json_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    print("--- Đang chờ dữ liệu mới... Hãy chèn thêm dữ liệu vào bảng Customers. ---")
    query.awaitTermination()

if __name__ == '__main__':
    debug_kafka_stream()