# realtime_faker.py (Advanced Version)

import pyodbc
import random
import time
from faker import Faker
from datetime import datetime

# --- CẤU HÌNH ---
SERVER_NAME = 'localhost,1434'
DATABASE = 'testdb'
USER = 'sa'
PASSWORD = 'yourStrong(!)Password'

fake = Faker()

def get_db_connection():
    """Tạo kết nối đến cơ sở dữ liệu SQL Server."""
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER_NAME};"
        f"DATABASE={DATABASE};"
        f"UID={USER};"
        f"PWD={PASSWORD};"
        f"TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)

def get_random_id(cursor, table_name, column_name, where_clause="1=1"):
    """Lấy một ID ngẫu nhiên từ một bảng với điều kiện cho trước."""
    sql = f"SELECT TOP 1 {column_name} FROM {table_name} WHERE {where_clause} ORDER BY NEWID()"
    cursor.execute(sql)
    result = cursor.fetchone()
    return result[0] if result else None

def simulate_new_customer(cursor):
    """Mô phỏng một khách hàng mới đăng ký."""
    name = fake.name()
    email = fake.unique.email()
    phone = fake.phone_number()[:20]
    sql = "INSERT INTO Customers (Name, Email, PhoneNumber) VALUES (?, ?, ?)"
    cursor.execute(sql, name, email, phone)
    print(f"✅ NEW CUSTOMER: Khách hàng '{name}' đã đăng ký.")

def simulate_new_order(cursor):
    """Mô phỏng một khách hàng hiện tại tạo một đơn hàng mới với nhiều sản phẩm."""
    customer_id = get_random_id(cursor, "Customers", "CustomerID")
    if not customer_id:
        print("⚠️ SKIPPED ORDER: Chưa có khách hàng nào trong CSDL.")
        return

    num_items = random.randint(1, 4)
    order_total_amount = 0
    order_items_to_insert = []

    for _ in range(num_items):
        # --- THAY ĐỔI LOGIC TẠI ĐÂY ---
        # Lấy một bản ghi tồn kho cụ thể (InventoryID) còn hàng
        sql_get_stock = "SELECT TOP 1 InventoryID, ProductID, QuantityInStock FROM Inventory WHERE QuantityInStock > 0 ORDER BY NEWID()"
        cursor.execute(sql_get_stock)
        stock_info = cursor.fetchone()

        if not stock_info:
            continue # Bỏ qua nếu không tìm thấy sản phẩm nào còn hàng

        inventory_id, product_id, quantity_in_stock = stock_info
        
        cursor.execute("SELECT Price FROM Products WHERE ProductID = ?", product_id)
        price = cursor.fetchone()[0]

        # Đảm bảo số lượng mua không vượt quá tồn kho
        quantity_to_order = random.randint(1, min(3, quantity_in_stock))
        
        order_total_amount += float(price) * quantity_to_order
        order_items_to_insert.append({
            'inventory_id': inventory_id,
            'product_id': product_id,
            'quantity': quantity_to_order,
            'price': price
        })

    if not order_items_to_insert:
        print("⚠️ SKIPPED ORDER: Không tìm thấy sản phẩm nào còn hàng.")
        return
        
    order_sql = "INSERT INTO Orders (OrderNumber, TotalAmount, StatusID, CustomerID) OUTPUT INSERTED.OrderID VALUES (?, ?, ?, ?)"
    order_number = f"ORD-{fake.random_int(min=100000, max=999999)}"
    cursor.execute(order_sql, order_number, order_total_amount, 1, customer_id)
    order_id = cursor.fetchone()[0]
    
    # Thêm OrderItems và cập nhật Inventory theo InventoryID cụ thể
    item_sql = "INSERT INTO OrderItems (OrderID, ProductID, Quantity, CurrentPrice) VALUES (?, ?, ?, ?)"
    inventory_sql = "UPDATE Inventory SET QuantityInStock = QuantityInStock - ? WHERE InventoryID = ?"
    for item in order_items_to_insert:
        cursor.execute(item_sql, order_id, item['product_id'], item['quantity'], item['price'])
        cursor.execute(inventory_sql, item['quantity'], item['inventory_id'])

    print(f"✅ NEW ORDER: Đơn hàng #{order_id} với {len(order_items_to_insert)} loại sản phẩm đã được tạo. Tồn kho đã được cập nhật.")

def simulate_order_progression(cursor):
    """Mô phỏng quá trình xử lý đơn hàng: từ Ordered -> Shipped -> Delivered."""
    # Ưu tiên cập nhật các đơn hàng đã đặt (StatusID=1)
    order_to_ship = get_random_id(cursor, "Orders", "OrderID", "StatusID = 1")
    if order_to_ship:
        cursor.execute("UPDATE Orders SET StatusID = 2 WHERE OrderID = ?", order_to_ship) # 2=Shipped
        print(f"🔄 STATUS UPDATE: Đơn hàng #{order_to_ship} đã được giao cho đơn vị vận chuyển (Shipped).")
        return

    # Nếu không còn đơn nào để ship, cập nhật các đơn đang ship (StatusID=2)
    order_to_deliver = get_random_id(cursor, "Orders", "OrderID", "StatusID = 2")
    if order_to_deliver:
        cursor.execute("UPDATE Orders SET StatusID = 3 WHERE OrderID = ?", order_to_deliver) # 3=Delivered
        print(f"🔄 STATUS UPDATE: Đơn hàng #{order_to_deliver} đã được giao thành công (Delivered).")
        
def simulate_review(cursor):
    """Mô phỏng khách hàng để lại đánh giá cho một sản phẩm đã được giao."""
    delivered_order_id = get_random_id(cursor, "Orders", "OrderID", "StatusID = 3")
    if not delivered_order_id:
        return # Bỏ qua nếu không có đơn hàng nào đã giao
        
    cursor.execute("SELECT CustomerID FROM Orders WHERE OrderID = ?", delivered_order_id)
    customer_id = cursor.fetchone()[0]
    
    product_id = get_random_id(cursor, "OrderItems", "ProductID", f"OrderID = {delivered_order_id}")
    if not product_id:
        return

    rating = random.randint(3, 5)
    comment = fake.sentence(nb_words=10)
    
    review_sql = "INSERT INTO Reviews (ProductID, CustomerID, Rating, Comment) VALUES (?, ?, ?, ?)"
    cursor.execute(review_sql, product_id, customer_id, rating, comment)
    print(f"✅ NEW REVIEW: Khách hàng #{customer_id} đã đánh giá sản phẩm #{product_id} {rating} sao.")

def main():
    print("🚀 Bắt đầu quá trình mô phỏng dữ liệu e-commerce. Nhấn Ctrl+C để dừng.")
    
    # Các hành động có thể thực hiện và trọng số (xác suất) của chúng
    actions = [
        simulate_new_order,
        simulate_order_progression,
        simulate_new_customer,
        simulate_review
    ]
    weights = [10, 8, 1, 2] # 10 phần cơ hội tạo đơn hàng, 8 phần cập nhật, 1 phần khách mới, 2 phần review

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        while True:
            # Chọn một hành động ngẫu nhiên dựa trên trọng số
            action_to_perform = random.choices(actions, weights=weights, k=1)[0]
            
            action_to_perform(cursor)
            conn.commit()
            
            sleep_time = random.uniform(1, 4) # Chờ từ 1 đến 4 giây
            print(f"--- Chờ {sleep_time:.2f} giây... ---\n")
            time.sleep(sleep_time)

    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"❌ Lỗi cơ sở dữ liệu: {sqlstate}\n{ex}")
    except KeyboardInterrupt:
        print("\n🛑 Đã dừng chương trình.")
    finally:
        if conn:
            conn.close()
            print("🔌 Đã đóng kết nối CSDL.")

if __name__ == "__main__":
    main()