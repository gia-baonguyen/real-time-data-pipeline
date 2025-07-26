# spark_apps/spark_reader.py

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# --- Cấu hình (phải khớp với kịch bản ghi) ---
DELTA_TABLE_PATH = "s3a://datalake/customers_scd2"
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

def configure_spark_session():
    """Cấu hình Spark Session để đọc Delta Lake từ MinIO."""
    return SparkSession.builder \
        .appName("DeltaTableReader") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def main():
    """
    Hàm chính để đọc và hiển thị dữ liệu từ bảng Delta.
    """
    spark = configure_spark_session()
    spark.sparkContext.setLogLevel("ERROR") # Chỉ hiện lỗi để dễ nhìn
    
    print(f"📖 Đang đọc từ bảng Delta tại: {DELTA_TABLE_PATH}")
    
    try:
        delta_df = spark.read.format("delta").load(DELTA_TABLE_PATH)
        print("✅ Đọc bảng thành công.")
    except Exception as e:
        if "Path does not exist" in str(e):
            print(f"❌ Lỗi: Đường dẫn '{DELTA_TABLE_PATH}' không tồn tại.")
            print("Hãy đảm bảo kịch bản streaming (spark_scd2_processor.py) đã chạy và tạo ra dữ liệu.")
            spark.stop()
            return
        else:
            raise e

    # Kiểm tra xem người dùng có cung cấp CustomerID làm tham số không
    if len(sys.argv) > 1:
        try:
            customer_id_to_check = int(sys.argv[1])
            print(f"\n🔍 Lọc lịch sử cho CustomerID: {customer_id_to_check}")
            
            result_df = delta_df.filter(col("CustomerID") == customer_id_to_check) \
                                .orderBy(col("effective_date"))
            
            if result_df.rdd.isEmpty():
                 print("Không tìm thấy dữ liệu cho CustomerID này.")
            else:
                 result_df.show(truncate=False)

        except ValueError:
            print(f"Lỗi: '{sys.argv[1]}' không phải là một CustomerID hợp lệ. Vui lòng nhập một số.")
    else:
        print("\n📚 Hiển thị 10 dòng đầu tiên của toàn bộ bảng lịch sử...")
        print("(Để xem lịch sử của một khách hàng cụ thể, hãy chạy: spark-submit <tên_file> <customer_id>)")
        delta_df.orderBy("CustomerID", "effective_date").show(10, truncate=False)

    spark.stop()

if __name__ == '__main__':
    main()