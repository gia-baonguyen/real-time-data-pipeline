# streamlit_app/app.py

import streamlit as st
import pandas as pd
import plotly.express as px
from pyspark.sql import SparkSession
import time

# --- Cấu hình ---
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

st.set_page_config(
    page_title="Top 10 Sản phẩm bán chạy",
    layout="wide",
)

@st.cache_resource
def get_spark_session():
    """Tạo và trả về một Spark Session đã được cấu hình."""
    print("--- ĐANG KHỞI TẠO SPARK SESSION (CHỈ CHẠY 1 LẦN) ---")
    return SparkSession.builder \
        .appName("StreamlitTopProducts") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.driver.host", "spark-master") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .getOrCreate()

def load_delta_table(spark, table_name):
    """Hàm generic để đọc một bảng Delta."""
    path = f"s3a://datalake/{table_name.lower()}"
    try:
        return spark.read.format("delta").load(path).toPandas()
    except Exception:
        return pd.DataFrame()

# --- Giao diện ứng dụng ---
st.title("🏆 Top 10 Sản phẩm bán chạy nhất Real-Time")
spark = get_spark_session()

# Đọc các bảng cần thiết
order_items_df = load_delta_table(spark, "OrderItems")
products_df = load_delta_table(spark, "Products")

if order_items_df.empty or products_df.empty:
    st.info("Chưa có đủ dữ liệu để hiển thị. Đang chờ dữ liệu...")
else:
    st.success(f"Dữ liệu được cập nhật lần cuối lúc: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Gộp bảng order_items và products để lấy tên sản phẩm
    merged_df = pd.merge(order_items_df, products_df, on='ProductID')
    
    # Nhóm theo tên sản phẩm và tính tổng số lượng đã bán
    top_products = merged_df.groupby('Name')['Quantity'].sum().nlargest(10).reset_index()
    top_products = top_products.sort_values(by='Quantity', ascending=False) # Sắp xếp giảm dần
    
    fig = px.bar(
        top_products, 
        x='Quantity', 
        y='Name', 
        orientation='h',
        title="Tổng số lượng đã bán",
        labels={'Quantity': 'Số lượng đã bán', 'Name': 'Tên sản phẩm'}
    )
    # Sắp xếp lại trục y để sản phẩm bán chạy nhất ở trên cùng
    fig.update_layout(yaxis={'categoryorder':'total ascending'})
    st.plotly_chart(fig, use_container_width=True)

# Cơ chế tự động làm mới sau 10 giây
time.sleep(10)
st.rerun()