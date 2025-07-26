# spark_apps/spark_scd2_processor.py

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, lit, current_timestamp, row_number
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType, DecimalType
from pyspark.sql.window import Window
from delta import DeltaTable

# --- Cấu hình ---
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

# --- Schemas cho các bảng ---
customers_schema = StructType([
    StructField("CustomerID", IntegerType()), StructField("Name", StringType()), StructField("Email", StringType()),
    StructField("PhoneNumber", StringType()), StructField("CreatedAt", LongType()), StructField("UpdatedAt", LongType())
])

orders_schema = StructType([
    StructField("OrderID", IntegerType()), StructField("OrderNumber", StringType()), StructField("TotalAmount", DecimalType(18, 2)),
    StructField("StatusID", IntegerType()), StructField("CustomerID", IntegerType()), StructField("CreatedAt", LongType()),
    StructField("UpdatedAt", LongType())
])

order_items_schema = StructType([
    StructField("OrderItemID", IntegerType()), StructField("OrderID", IntegerType()), StructField("ProductID", IntegerType()),
    StructField("Quantity", IntegerType()), StructField("CurrentPrice", DecimalType(18, 2)), StructField("CreatedAt", LongType()),
    StructField("UpdatedAt", LongType())
])

products_schema = StructType([
    StructField("ProductID", IntegerType()), StructField("Name", StringType()), StructField("Description", StringType()),
    StructField("Price", DecimalType(18, 2)), StructField("CategoryID", IntegerType()), StructField("SellerID", IntegerType()),
    StructField("CreatedAt", LongType()), StructField("UpdatedAt", LongType())
])

inventory_schema = StructType([
    StructField("InventoryID", IntegerType()), StructField("InventoryName", StringType()), StructField("ProductID", IntegerType()),
    StructField("QuantityInStock", IntegerType()), StructField("ReorderThreshold", IntegerType()), StructField("UnitCost", DecimalType(18, 2)),
    StructField("CreatedAt", LongType()), StructField("UpdatedAt", LongType())
])

# Schema đầy đủ của Debezium
def get_debezium_schema(data_schema):
    return StructType([
        StructField("payload", StructType([
            StructField("before", data_schema),
            StructField("after", data_schema),
            StructField("op", StringType()),
            StructField("ts_ms", LongType())
        ]))
    ])

def configure_spark_session():
    return SparkSession.builder \
        .appName("RealtimePipelineProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.driver.host", "spark-master") \
        .getOrCreate()

def upsert_scd2_customers(micro_batch_df: DataFrame, batch_id: int):
    """
    Hàm xử lý logic SCD2 cho bảng Customers, bao gồm tạo bảng, đóng bản ghi cũ,
    và chèn bản ghi mới.
    """
    print(f"--- Bắt đầu xử lý SCD2 Customers Batch ID: {batch_id} ---")
    
    DELTA_TABLE_PATH = "s3a://datalake/customers_scd2"
    spark = micro_batch_df.sparkSession

    if micro_batch_df.rdd.isEmpty():
        print("Batch Customers rỗng, bỏ qua.")
        return

    # 1. Phân tích payload của Debezium
    debezium_payload_df = micro_batch_df.select(
        from_json(col("value").cast("string"), get_debezium_schema(customers_schema)).alias("message")
    ).select("message.payload.*")

    source_changes_raw = debezium_payload_df.filter(col("op").isNotNull())
    if source_changes_raw.rdd.isEmpty():
        print("Batch Customers không chứa thay đổi hợp lệ, bỏ qua.")
        return

    # 2. Loại bỏ các sự kiện trùng lặp trong cùng một batch
    window_spec = Window.partitionBy(col("after.CustomerID")).orderBy(col("ts_ms").desc())
    source_changes = source_changes_raw.withColumn("rank", row_number().over(window_spec)) \
                                       .filter(col("rank") == 1) \
                                       .drop("rank")
    source_changes.cache()

    # 3. Xử lý logic đóng bản ghi cũ nếu bảng đã tồn tại
    if DeltaTable.isDeltaTable(spark, DELTA_TABLE_PATH):
        updates_and_deletes = source_changes.filter(col("op").isin(['u', 'd']))
        if not updates_and_deletes.rdd.isEmpty():
            keys_to_expire = updates_and_deletes.selectExpr(
                "nvl(after.CustomerID, before.CustomerID) as CustomerID"
            ).distinct()
            print(f"Sẽ đóng {keys_to_expire.count()} bản ghi Customers cũ...")
            
            delta_table = DeltaTable.forPath(spark, DELTA_TABLE_PATH)
            delta_table.alias("target").merge(
                source=keys_to_expire.alias("source"),
                condition="target.CustomerID = source.CustomerID AND target.is_current = true"
            ).whenMatchedUpdate(
                set={
                    "is_current": lit(False),
                    "end_date": current_timestamp()
                }
            ).execute()
    else:
        print("Bảng Customers Delta chưa tồn tại. Sẽ tạo bảng mới.")

    # 4. Chuẩn bị và chèn các bản ghi mới (từ sự kiện CREATE và UPDATE)
    new_records_df = source_changes.filter(col("op").isin(['c', 'u'])).select("after.*")
    if not new_records_df.rdd.isEmpty():
        new_records_to_insert = new_records_df.withColumn(
            "source_created_at", (col("CreatedAt") / 1000000000).cast(TimestampType())
        ).withColumn(
            "source_updated_at", (col("UpdatedAt") / 1000000000).cast(TimestampType())
        ).drop("CreatedAt", "UpdatedAt") \
        .withColumn("is_current", lit(True)) \
        .withColumn("effective_date", current_timestamp()) \
        .withColumn("end_date", lit(None).cast(TimestampType()))
        print(f"Sẽ chèn {new_records_to_insert.count()} bản ghi Customers mới...")
        new_records_to_insert.write.format("delta").mode("append").save(DELTA_TABLE_PATH)

    source_changes.unpersist()
    print(f"--- Hoàn thành xử lý Customers Batch ID: {batch_id} ---")

# REPLACE THE OLD FUNCTION WITH THIS NEW ONE

def upsert_simple_table(micro_batch_df: DataFrame, batch_id: int, table_name: str, schema: StructType, p_key: str):
    """Hàm generic để upsert cho các bảng đơn giản, đã được thêm logic loại bỏ trùng lặp."""
    print(f"--- Bắt đầu xử lý {table_name} Batch ID: {batch_id} ---")
    
    path = f"s3a://datalake/{table_name.lower()}"
    spark = micro_batch_df.sparkSession
    
    debezium_payload_df = micro_batch_df.select(
        from_json(col("value").cast("string"), get_debezium_schema(schema)).alias("message")
    ).select("message.payload.*")
    
    source_changes_raw = debezium_payload_df.filter(col("op").isNotNull()) \
                                            .filter(col(f"after.{p_key}").isNotNull())

    if source_changes_raw.rdd.isEmpty():
        print(f"Batch {table_name} không có thay đổi hợp lệ.")
        return

    # --- BƯỚC QUAN TRỌNG: LOẠI BỎ TRÙNG LẶP ---
    # Chỉ giữ lại sự kiện cuối cùng cho mỗi Primary Key trong batch này
    window_spec = Window.partitionBy(col(f"after.{p_key}")).orderBy(col("ts_ms").desc())
    
    changes = source_changes_raw.withColumn("rank", row_number().over(window_spec)) \
                                .filter(col("rank") == 1) \
                                .drop("rank") \
                                .select("after.*")

    if changes.rdd.isEmpty():
        print(f"Batch {table_name} không có thay đổi sau khi loại bỏ trùng lặp.")
        return

    # Tự tạo bảng nếu chưa có
    if not DeltaTable.isDeltaTable(spark, path):
        print(f"Đã tạo bảng Delta mới cho {table_name}.")
        changes.write.format("delta").mode("overwrite").save(path)
        delta_table = DeltaTable.forPath(spark, path)
    else:
        delta_table = DeltaTable.forPath(spark, path)

    # Thực hiện merge
    delta_table.alias("target").merge(
        source=changes.alias("source"),
        condition=f"target.{p_key} = source.{p_key}"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print(f"--- Hoàn thành xử lý {table_name} Batch ID: {batch_id} ---")
if __name__ == '__main__':
    spark = configure_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    topics = [
        "server_docker.testdb.dbo.Customers",
        "server_docker.testdb.dbo.Orders",
        "server_docker.testdb.dbo.OrderItems",
        "server_docker.testdb.dbo.Products",
        "server_docker.testdb.dbo.Inventory"
    ]

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", ",".join(topics)) \
        .option("startingOffsets", "earliest") \
        .load()

    # Tạo các stream riêng cho từng bảng
    
    # 1. Stream cho Customers (SCD2)
    # customer_stream = kafka_df.filter(col("topic") == topics[0]).writeStream \
    #     .foreachBatch(upsert_scd2_customers) \
    #     .outputMode("update") \
    #     .option("checkpointLocation", "/tmp/delta/checkpoints/customers_scd2") \
    #     .start()

    # 2. Stream cho Orders
    orders_stream = kafka_df.filter(col("topic") == topics[1]).writeStream \
        .foreachBatch(lambda df, batch_id: upsert_simple_table(df, batch_id, "Orders", orders_schema, "OrderID")) \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/delta/checkpoints/orders") \
        .start()

    # 3. Stream cho OrderItems
    order_items_stream = kafka_df.filter(col("topic") == topics[2]).writeStream \
        .foreachBatch(lambda df, batch_id: upsert_simple_table(df, batch_id, "OrderItems", order_items_schema, "OrderItemID")) \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/delta/checkpoints/order_items") \
        .start()
        
    # 4. Stream cho Products
    products_stream = kafka_df.filter(col("topic") == topics[3]).writeStream \
        .foreachBatch(lambda df, batch_id: upsert_simple_table(df, batch_id, "Products", products_schema, "ProductID")) \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/delta/checkpoints/products") \
        .start()

    # 5. Stream cho Inventory
    inventory_stream = kafka_df.filter(col("topic") == topics[4]).writeStream \
        .foreachBatch(lambda df, batch_id: upsert_simple_table(df, batch_id, "Inventory", inventory_schema, "InventoryID")) \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/delta/checkpoints/inventory") \
        .start()

    print("🚀 Bắt đầu chạy các pipeline streaming...")
    spark.streams.awaitAnyTermination()