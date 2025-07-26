# streamlit_app/Dockerfile

# Bắt đầu từ image đã có sẵn Spark
FROM jupyter/pyspark-notebook:latest

# Chuyển sang user có quyền để cài đặt thư viện
USER root

# Tạo thư mục cho ứng dụng
WORKDIR /app

# Sao chép tệp yêu cầu thư viện và cài đặt chúng
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Sao chép mã nguồn ứng dụng
COPY app.py .

# Mở cổng mặc định của Streamlit
EXPOSE 8501

# Lệnh để chạy ứng dụng khi container khởi động
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]