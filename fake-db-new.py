import random
import pyodbc
from faker import Faker
from datetime import datetime, timedelta

# Khởi tạo Faker
fake = Faker()

# Các hằng số
NUM_ORDERS = 100
NUM_SELLERS = 40
NUM_PRODUCTS = 60
NUM_CUSTOMERS = 70
ORDER_STATUSES = ['Ordered', 'Shipped', 'Delivered', 'Returned', 'Cancelled']
PAYMENT_METHODS = ['Cash', 'Credit Card', 'Debit Card', 'Bank Transfer', 'Digital Wallet']
PRODUCT_CATEGORIES = [
    'Electronics', 'Fashion', 'Home & Living', 'Sports', 'Beauty', 'Toys',
    'Automotive', 'Books', 'Garden & Outdoors', 'Office Supplies'
]
INVENTORY_REGIONS = ['North America', 'Europe', 'Asia', 'South America', 'Africa', 'Australian']

# Kết nối cơ sở dữ liệu SQL Server
def get_db_connection():
    """Tạo kết nối đến cơ sở dữ liệu SQL Server."""
    # THAY THẾ CÁC CHI TIẾT KẾT NỐI BẰNG THÔNG TIN CỦA BẠN
    server = 'localhost,1434'  # ví dụ: 'localhost\\SQLEXPRESS'
    database = 'testdb'
    username = 'sa'
    password = 'yourStrong(!)Password'
    # Sử dụng trình điều khiển ODBC phù hợp đã được cài đặt trên hệ thống của bạn
    driver = '{ODBC Driver 17 for SQL Server}' # Hoặc '{SQL Server}'
    
    conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    return pyodbc.connect(conn_str)

# Xóa tất cả dữ liệu hiện có
def clear_existing_data():
    """Xóa dữ liệu từ các bảng và đặt lại cột IDENTITY."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Thứ tự xóa rất quan trọng do các ràng buộc khóa ngoại
    tables_in_order = [
        'Payments', 'OrderItems', 'Reasons', 'Reviews', 'CartItems', 
        'ShoppingCarts', 'Addresses', 'Inventory', 'Orders', 'Products', 
        'ProductCategories', 'Customers', 'Sellers', 'PaymentMethods', 'OrderStatus'
    ]
    
    print("Bắt đầu xóa dữ liệu hiện có...")
    for table in tables_in_order:
        try:
            print(f"Đang xóa dữ liệu từ bảng {table}...")
            cursor.execute(f"DELETE FROM {table};")
            # Đặt lại giá trị seed của cột IDENTITY
            try:
                cursor.execute(f"DBCC CHECKIDENT ('{table}', RESEED, 0);")
            except pyodbc.ProgrammingError:
                # Bỏ qua lỗi nếu bảng không có cột identity
                pass
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            print(f"Lỗi khi xóa bảng {table}: {sqlstate}")
            print(ex)

    conn.commit()
    cursor.close()
    conn.close()
    print("Đã xóa xong tất cả dữ liệu hiện có.")

# Tạo Người bán
def generate_sellers(num_sellers):
    sellers = []
    for i in range(1, num_sellers + 1):
        sellers.append({
            'SellerID': i,
            'Name': fake.company(),
            'Email': fake.company_email(),
            'PhoneNumber': fake.phone_number()[:20],
            'CreatedAt': fake.date_time_between(start_date='-2y', end_date='now'),
            'UpdatedAt': fake.date_time_between(start_date='-1y', end_date='now')
        })
    return sellers

# Tạo Khách hàng
def generate_customers(num_customers):
    customers = []
    for i in range(1, num_customers + 1):
        customers.append({
            'CustomerID': i,
            'Name': fake.name(),
            'Email': fake.email(),
            'PhoneNumber': fake.phone_number()[:20],
            'CreatedAt': fake.date_time_between(start_date='-2y', end_date='now'),
            'UpdatedAt': fake.date_time_between(start_date='-1y', end_date='now')
        })
    return customers

# Tạo Giỏ hàng (1 giỏ hàng mỗi khách hàng)
def generate_shopping_carts(customers):
    carts = []
    for i, customer in enumerate(customers, start=1):
        carts.append({
            'CartID': i,
            'CustomerID': customer['CustomerID'],
            'CreatedAt': fake.date_time_between(start_date='-1y', end_date='now'),
            'UpdatedAt': fake.date_time_between(start_date='-1y', end_date='now')
        })
    return carts

# Tạo Chi tiết giỏ hàng
def generate_cart_items(carts, products):
    cart_items = []
    cart_item_id = 1
    for cart in carts:
        num_items = random.randint(1, 5)
        for _ in range(num_items):
            cart_items.append({
                'CartItemID': cart_item_id,
                'CartID': cart['CartID'],
                'ProductID': random.choice(products)['ProductID'],
                'Quantity': random.randint(1, 3),
                'CreatedAt': cart['CreatedAt'],
                'UpdatedAt': cart['UpdatedAt']
            })
            cart_item_id += 1
    return cart_items

# Tạo Danh mục sản phẩm
def generate_product_categories():
    categories = []
    for i, category in enumerate(PRODUCT_CATEGORIES, start=1):
        categories.append({
            'CategoryID': i,
            'CategoryName': category,
            'CategoryDescription': f"Category for {category.lower()} products",
            'CreatedAt': fake.date_time_between(start_date='-2y', end_date='now'),
            'UpdatedAt': fake.date_time_between(start_date='-1y', end_date='now')
        })
    return categories

# Tạo Sản phẩm
def generate_products(num_products, num_sellers):
    product_templates = {
        'Electronics': [('Smartphone', 'A high-end smartphone with advanced features.'), ('Laptop', 'A powerful laptop for work and gaming.'), ('Headphones', 'Noise-cancelling headphones for immersive sound.'), ('Smartwatch', 'A smartwatch with health tracking features.'), ('Tablet', 'A lightweight tablet for entertainment and productivity.')],
        'Fashion': [('T-Shirt', 'A comfortable cotton t-shirt.'), ('Jeans', 'Stylish denim jeans for everyday wear.'), ('Sneakers', 'Lightweight sneakers for casual outings.'), ('Dress', 'A fashionable dress for special occasions.'), ('Jacket', 'A warm jacket for cold weather.')],
        'Home & Living': [('Sofa', 'A cozy sofa for your living room.'), ('Dining Table', 'A wooden dining table for family meals.'), ('Lamp', 'A decorative lamp for ambient lighting.'), ('Curtains', 'Elegant curtains for your windows.'), ('Bookshelf', 'A spacious bookshelf for your collection.')],
        'Sports': [('Football', 'A durable football for outdoor games.'), ('Tennis Racket', 'A lightweight tennis racket for professionals.'), ('Yoga Mat', 'A non-slip yoga mat for workouts.'), ('Basketball', 'An official size basketball.'), ('Running Shoes', 'Comfortable running shoes for athletes.')],
        'Beauty': [('Lipstick', 'A long-lasting lipstick in vibrant colors.'), ('Face Cream', 'A moisturizing face cream for daily use.'), ('Perfume', 'A refreshing perfume with floral notes.'), ('Shampoo', 'A nourishing shampoo for healthy hair.'), ('Nail Polish', 'Glossy nail polish in various shades.')],
        'Toys': [('Action Figure', 'A collectible action figure for kids.'), ('Puzzle', 'A challenging puzzle for brain exercise.'), ('Toy Car', 'A remote-controlled toy car for fun.'), ('Doll', 'A beautiful doll with accessories.'), ('Board Game', 'A fun board game for family nights.')],
        'Automotive': [('Car Vacuum', 'A portable vacuum cleaner for cars.'), ('GPS Navigator', 'A GPS navigator with real-time updates.'), ('Car Cover', 'A waterproof car cover for protection.'), ('Dash Cam', 'A high-definition dash camera.'), ('Tire Inflator', 'An electric tire inflator for emergencies.')],
        'Books': [('Mystery Novel', 'A thrilling mystery novel.'), ('Cookbook', 'A cookbook with delicious recipes.'), ('Science Textbook', 'A comprehensive science textbook.'), ('Children\'s Book', 'An illustrated book for children.'), ('Travel Guide', 'A guidebook for world travelers.')],
        'Garden & Outdoors': [('Garden Hose', 'A flexible garden hose for watering plants.'), ('BBQ Grill', 'A portable BBQ grill for outdoor cooking.'), ('Tent', 'A waterproof tent for camping.'), ('Lawn Mower', 'An electric lawn mower for easy gardening.'), ('Patio Set', 'A stylish patio set for your garden.')],
        'Office Supplies': [('Notebook', 'A ruled notebook for notes.'), ('Desk Chair', 'An ergonomic desk chair for comfort.'), ('Pen Set', 'A set of smooth-writing pens.'), ('Monitor Stand', 'A stand to elevate your monitor.'), ('Desk Lamp', 'A bright desk lamp for work.')]
    }
    products = []
    category_count = len(PRODUCT_CATEGORIES)
    used_names = set()
    for i in range(1, num_products + 1):
        category_idx = random.randint(0, category_count - 1)
        category = PRODUCT_CATEGORIES[category_idx]
        name, desc = random.choice(product_templates[category])
        while True:
            unique_code = fake.unique.bothify(text='??-###')
            unique_name = f"{name} {unique_code}"
            if unique_name not in used_names:
                used_names.add(unique_name)
                break
        products.append({
            'ProductID': i,
            'Name': unique_name,
            'Description': desc,
            'Price': round(random.uniform(5.00, 500.00), 2),
            'CategoryID': category_idx + 1,
            'SellerID': random.randint(1, num_sellers),
            'CreatedAt': fake.date_time_between(start_date='-2y', end_date='now'),
            'UpdatedAt': fake.date_time_between(start_date='-1y', end_date='now')
        })
    return products

# Tạo Trạng thái đơn hàng
def generate_order_status():
    now = datetime.now()
    return [
        {'StatusID': 1, 'StatusName': 'Ordered', 'StatusDescription': 'Order has been placed and confirmed', 'CreatedAt': now, 'UpdatedAt': now},
        {'StatusID': 2, 'StatusName': 'Shipped', 'StatusDescription': 'Order has been shipped to customer', 'CreatedAt': now, 'UpdatedAt': now},
        {'StatusID': 3, 'StatusName': 'Delivered', 'StatusDescription': 'Order has been delivered to customer', 'CreatedAt': now, 'UpdatedAt': now},
        {'StatusID': 4, 'StatusName': 'Returned', 'StatusDescription': 'Order has been returned by customer', 'CreatedAt': now, 'UpdatedAt': now},
        {'StatusID': 5, 'StatusName': 'Cancelled', 'StatusDescription': 'Order has been cancelled', 'CreatedAt': now, 'UpdatedAt': now}
    ]

# Tạo Đơn hàng
def generate_orders(num_orders, customers):
    orders = []
    status_weights = [0.45, 0.3, 0.2, 0.03, 0.02]
    for i in range(1, num_orders + 1):
        customer = random.choice(customers)
        status = random.choices(ORDER_STATUSES, weights=status_weights, k=1)[0]
        created_at = fake.date_time_between(start_date='-1y', end_date='now')
        orders.append({
            'OrderID': i,
            'OrderNumber': f"ORD-{fake.random_int(min=100000, max=999999)}",
            'TotalAmount': round(random.uniform(10.00, 1000.00), 2),
            'StatusID': ORDER_STATUSES.index(status) + 1,
            'CustomerID': customer['CustomerID'],
            'CreatedAt': created_at,
            'UpdatedAt': fake.date_time_between(start_date=created_at, end_date='now')
        })
    return orders

# Tạo Chi tiết đơn hàng
def generate_order_items(orders, products):
    order_items = []
    order_item_id = 1
    for order in orders:
        num_items = random.randint(1, 5)
        for _ in range(num_items):
            product = random.choice(products)
            order_items.append({
                'OrderItemID': order_item_id,
                'OrderID': order['OrderID'],
                'ProductID': product['ProductID'],
                'Quantity': random.randint(1, 3),
                'CurrentPrice': product['Price'],
                'CreatedAt': order['CreatedAt'],
                'UpdatedAt': order['UpdatedAt']
            })
            order_item_id += 1
    return order_items

# Tạo Lý do
def generate_reasons(orders):
    reasons = []
    reason_id = 1
    returned_reasons = ['Damage or Defects', 'Incorrect Item Received', 'Product Mismatch', 'Size or Fit Issues', 'Poor Quality', 'Functionality Problems', 'Customer did not receive']
    cancelled_reasons = ['Change of Mind', 'Found a Better Price', 'Out of Stock', 'Late Delivery', 'High Shipping Cost']
    for order in orders:
        status = ORDER_STATUSES[order['StatusID'] - 1]
        if status == 'Returned':
            reason_type, reason_description = 'Return', random.choice(returned_reasons)
        elif status == 'Cancelled':
            reason_type, reason_description = 'Cancellation', random.choice(cancelled_reasons)
        else:
            continue
        reasons.append({
            'ReasonID': reason_id,
            'OrderID': order['OrderID'],
            'ReasonType': reason_type,
            'ReasonDescription': reason_description,
            'CreatedAt': order['CreatedAt'],
            'UpdatedAt': order['UpdatedAt']
        })
        reason_id += 1
    return reasons

# Tạo Đánh giá
def generate_reviews(products, customers):
    reviews = []
    review_id = 1
    review_templates = {
        'Electronics': {5: ["Amazing product! Exceeded my expectations.", "Top-notch quality and features."], 4: ["Very good, works as described.", "Satisfied with the performance."], 3: ["Average, does the job.", "It's okay, nothing special."], 2: ["Not as good as I hoped.", "Some issues with performance."], 1: ["Very disappointed. Would not buy again.", "Stopped working quickly."]},
        'Fashion': {5: ["Fits perfectly and looks great!", "Stylish and comfortable."], 4: ["Nice quality, would buy again.", "Looks good, fits well."], 3: ["It's okay, fits as expected.", "Average quality."], 2: ["Material feels cheap.", "Didn't fit as expected."], 1: ["Terrible fit and quality.", "Very disappointed."]},
        'Home & Living': {5: ["Makes my home so much better!", "Excellent quality and design."], 4: ["Looks great in my house.", "Good value for the price."], 3: ["It's okay, does the job.", "Average quality."], 2: ["Not as sturdy as expected.", "Some issues with assembly."], 1: ["Very poor quality.", "Broke after a week."]},
        'Sports': {5: ["Perfect for my workouts!", "Great quality sports gear."], 4: ["Works well for training.", "Good value for sports lovers."], 3: ["It's okay, does the job.", "Average sports equipment."], 2: ["Not as durable as expected.", "Some issues during use."], 1: ["Very poor quality.", "Broke after a few uses."]},
        'Beauty': {5: ["My favorite beauty product!", "Excellent results, highly recommend."], 4: ["Works well, would buy again.", "Good quality beauty item."], 3: ["It's okay, nothing special.", "Average results."], 2: ["Didn't work as expected.", "Some irritation occurred."], 1: ["Very disappointed.", "Caused skin issues."]},
        'Toys': {5: ["Kids love it!", "Great toy, lots of fun."], 4: ["Good quality toy.", "Fun and entertaining."], 3: ["It's okay, keeps kids busy.", "Average toy."], 2: ["Broke after a short time.", "Not as fun as expected."], 1: ["Very poor quality.", "Not safe for kids."]},
        'Automotive': {5: ["Works perfectly for my car!", "Great automotive accessory."], 4: ["Good value for the price.", "Useful and reliable."], 3: ["It's okay, does the job.", "Average quality."], 2: ["Not as effective as expected.", "Some issues during use."], 1: ["Very disappointed.", "Stopped working quickly."]},
        'Books': {5: ["Couldn't put it down!", "Excellent read, highly recommend."], 4: ["Very good book.", "Enjoyed reading it."], 3: ["It's okay, decent story.", "Average book."], 2: ["Not as interesting as expected.", "Some parts were boring."], 1: ["Did not enjoy it.", "Very boring."]},
        'Garden & Outdoors': {5: ["Perfect for my garden!", "Excellent quality outdoor product."], 4: ["Works well in my yard.", "Good value for the price."], 3: ["It's okay, does the job.", "Average garden tool."], 2: ["Not as sturdy as expected.", "Some issues with use."], 1: ["Very poor quality.", "Broke after a week."]},
        'Office Supplies': {5: ["Makes my work easier!", "Excellent office product."], 4: ["Good quality, would buy again.", "Works well at my desk."], 3: ["It's okay, does the job.", "Average office supply."], 2: ["Not as durable as expected.", "Some issues during use."], 1: ["Very poor quality.", "Broke quickly."]}
    }
    default_templates = {5: ["Excellent product!", "Very satisfied."], 4: ["Good product.", "Works well."], 3: ["Average.", "It's okay."], 2: ["Not great.", "Had some issues."], 1: ["Very bad.", "Not recommended."]}
    for product in products:
        num_reviews = random.randint(0, 5)
        category = PRODUCT_CATEGORIES[product['CategoryID'] - 1]
        for _ in range(num_reviews):
            customer = random.choice(customers)
            rating = random.randint(1, 5)
            templates = review_templates.get(category, default_templates)
            comment = random.choice(templates.get(rating, default_templates[rating]))
            reviews.append({
                'ReviewID': review_id,
                'ProductID': product['ProductID'],
                'CustomerID': customer['CustomerID'],
                'Rating': rating,
                'Comment': comment,
                'CreatedAt': fake.date_time_between(start_date='-1y', end_date='now'),
                'UpdatedAt': fake.date_time_between(start_date='-1y', end_date='now')
            })
            review_id += 1
    return reviews

# Tạo Địa chỉ
def generate_addresses(customers):
    addresses = []
    address_id = 1
    for customer in customers:
        num_addresses = random.randint(1, 2)
        is_billing_set = False
        for _ in range(num_addresses):
            is_billing = not is_billing_set and (num_addresses == 1 or random.choice([True, False]))
            is_shipping = not is_billing
            if is_billing: is_billing_set = True
            
            addresses.append({
                'AddressID': address_id,
                'CustomerID': customer['CustomerID'],
                'AddressLine': fake.street_address(),
                'City': fake.city(),
                'State': fake.state(),
                'ZipCode': fake.zipcode(),
                'Country': fake.country()[:50],
                'IsBillingAddress': is_billing,
                'IsShippingAddress': is_shipping,
                'CreatedAt': fake.date_time_between(start_date='-2y', end_date='now'),
                'UpdatedAt': fake.date_time_between(start_date='-1y', end_date='now')
            })
            address_id += 1
    return addresses

# Tạo Tồn kho
def generate_inventory(products):
    inventory = []
    inventory_id = 1
    region_product_percent = {'Asia': 1.0, 'Europe': 0.7, 'North America': 0.5, 'South America': 0.3, 'Australian': 0.2, 'Africa': 0.1}
    for region, percent in region_product_percent.items():
        num_products_in_region = int(len(products) * percent)
        region_products = random.sample(products, num_products_in_region)
        for product in region_products:
            inventory.append({
                'InventoryID': inventory_id,
                'InventoryName': region,
                'ProductID': product['ProductID'],
                'QuantityInStock': random.randint(0, 1000),
                'ReorderThreshold': random.randint(10, 50),
                'UnitCost': round(product['Price'] * random.uniform(0.5, 0.8), 2),
                'CreatedAt': fake.date_time_between(start_date='-2y', end_date='now'),
                'UpdatedAt': fake.date_time_between(start_date='-1y', end_date='now')
            })
            inventory_id += 1
    return inventory

# Tạo Phương thức thanh toán
def generate_payment_methods():
    now = datetime.now()
    return [
        {'PaymentMethodID': 1, 'MethodName': 'Cash', 'MethodDescription': 'Payment made in cash', 'CreatedAt': now, 'UpdatedAt': now},
        {'PaymentMethodID': 2, 'MethodName': 'Credit Card', 'MethodDescription': 'Payment using credit card', 'CreatedAt': now, 'UpdatedAt': now},
        {'PaymentMethodID': 3, 'MethodName': 'Debit Card', 'MethodDescription': 'Payment using debit card', 'CreatedAt': now, 'UpdatedAt': now},
        {'PaymentMethodID': 4, 'MethodName': 'Bank Transfer', 'MethodDescription': 'Payment via bank transfer', 'CreatedAt': now, 'UpdatedAt': now},
        {'PaymentMethodID': 5, 'MethodName': 'Digital Wallet', 'MethodDescription': 'Payment using electronic wallet', 'CreatedAt': now, 'UpdatedAt': now}
    ]

# Tạo Thanh toán
def generate_payments(orders, payment_methods):
    payments = []
    payment_id = 1
    for order in orders:
        status = ORDER_STATUSES[order['StatusID'] - 1]
        if status in ['Cancelled', 'Returned']:
            continue
        payment_method = random.choice(payment_methods)
        if payment_method['MethodName'] == 'Cash' and status != 'Delivered':
            continue
        payments.append({
            'PaymentID': payment_id,
            'OrderID': order['OrderID'],
            'PaymentMethodID': payment_method['PaymentMethodID'],
            'Amount': order['TotalAmount'],
            'CreatedAt': order['CreatedAt'] + timedelta(minutes=random.randint(1, 60)),
            'UpdatedAt': order['UpdatedAt']
        })
        payment_id += 1
    return payments

# Chèn dữ liệu vào cơ sở dữ liệu
def insert_data_to_db(table_name, data):
    """Chèn một danh sách các dict vào bảng được chỉ định."""
    if not data:
        print(f"Không có dữ liệu để chèn vào {table_name}.")
        return

    conn = get_db_connection()
    cursor = conn.cursor()

    columns = ', '.join(f'[{col}]' for col in data[0].keys())
    placeholders = ', '.join(['?'] * len(data[0]))
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    
    try:
        # Bật IDENTITY_INSERT để cho phép chèn giá trị tường minh vào cột identity
        cursor.execute(f"SET IDENTITY_INSERT {table_name} ON;")
        
        rows = [tuple(d.values()) for d in data]
        cursor.executemany(sql, rows)
        
        conn.commit()
        print(f"Đã chèn {len(data)} bản ghi vào {table_name}.")
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Lỗi khi chèn dữ liệu vào {table_name}: {sqlstate}")
        print(ex)
        conn.rollback()
    finally:
        # Luôn tắt IDENTITY_INSERT sau khi hoàn tất
        try:
            cursor.execute(f"SET IDENTITY_INSERT {table_name} OFF;")
        except pyodbc.Error:
            pass # Lỗi có thể xảy ra nếu SET ON ban đầu thất bại
        cursor.close()
        conn.close()

# Hàm chính
def main():
    clear_existing_data()

    print("Bắt đầu tạo dữ liệu giả...")
    sellers = generate_sellers(NUM_SELLERS)
    customers = generate_customers(NUM_CUSTOMERS)
    shopping_carts = generate_shopping_carts(customers)
    product_categories = generate_product_categories()
    products = generate_products(NUM_PRODUCTS, NUM_SELLERS)
    order_status = generate_order_status()
    payment_methods = generate_payment_methods()
    orders = generate_orders(NUM_ORDERS, customers)
    cart_items = generate_cart_items(shopping_carts, products)
    order_items = generate_order_items(orders, products)
    reasons = generate_reasons(orders)
    reviews = generate_reviews(products, customers)
    addresses = generate_addresses(customers)
    payments = generate_payments(orders, payment_methods)
    inventory = generate_inventory(products)

    print("\nBắt đầu chèn dữ liệu vào cơ sở dữ liệu...")
    # Sắp xếp thứ tự chèn để đảm bảo tính toàn vẹn của khóa ngoại
    insert_data_to_db('Sellers', sellers)
    insert_data_to_db('Customers', customers)
    insert_data_to_db('ProductCategories', product_categories)
    insert_data_to_db('OrderStatus', order_status)
    insert_data_to_db('PaymentMethods', payment_methods)
    insert_data_to_db('Products', products)
    insert_data_to_db('ShoppingCarts', shopping_carts)
    insert_data_to_db('Addresses', addresses)
    insert_data_to_db('Orders', orders)
    insert_data_to_db('CartItems', cart_items)
    insert_data_to_db('OrderItems', order_items)
    insert_data_to_db('Reasons', reasons)
    insert_data_to_db('Reviews', reviews)
    insert_data_to_db('Payments', payments)
    insert_data_to_db('Inventory', inventory)

    print("\nHoàn tất quá trình tạo dữ liệu.")

if __name__ == "__main__":
    main()
