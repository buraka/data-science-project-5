import psycopg2

## Bu değeri localinde çalışırken kendi passwordün yap. Ama kodu pushlarken 'postgres' olarak bırak.
password = '1234'

def connect_db():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="postgres",
        user="postgres",
        password=password
    )

# 1- Null emailleri 'unknown@example.com' ile değiştir
def clean_null_emails():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE customers
                SET email = 'unknown@example.com'
                WHERE email IS NULL;
            """)
            conn.commit()

# 2- Hatalı emailleri bul (İçerisinde '@' işareti geçmeyen emailleri bul.)
def find_invalid_emails():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT full_name, email
                FROM customers
                WHERE email IS NOT NULL AND POSITION('@' IN email) = 0;
            """)
            return cur.fetchall()

# 3- İsimlerin ilk 3 harfi (customer tablosundan önce kullanıcının ismini daha sonra isminin ilk 3 harfini short_name ismiyle getir.)
def get_first_3_letters_of_names():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT full_name, LEFT(full_name, 3) AS short_name
                FROM customers;
            """)
            return cur.fetchall()

# 4- Email domainlerini bul (customer tablosundan önce kullanıcının ismini daha sonra emailinin @ işaretinin sağ tarafında kalan kısmını(domain) bilgisini getiriniz.)
def get_email_domains():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT full_name, 
                        SUBSTRING(email FROM POSITION('@' IN email) + 1) AS domain
                FROM customers
                WHERE email LIKE '%@%';
            """)
            return cur.fetchall()

# 5- İsim ve email birleştir (Tüm müşterilerin isim ile emaili birleştirerek full_info ismiyle dönen sorguyu yazınız.)
def concat_name_and_email():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT CONCAT(full_name, ' - ', email) AS full_info
                FROM customers;
            """)
            return cur.fetchall()

# 6- Sipariş tutarlarını tam sayıya çevir (orders tablosundan tüm tutarları INTEGER'a çevirip order_id ile birlikte total_amount_int ismiyle dönen sorguyu yazınız.)
def cast_total_amount_to_integer():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT order_id, CAST(total_amount AS INTEGER) AS total_amount_int
                FROM orders;
            """)
            return cur.fetchall()

# 7- Email '@' pozisyonu (müşterilerin ismini ve emaillerindeki @ işaretinin kaçıncı indkste olduğunun bilgisini at_position ismiyle dönen sorguyu yazınız.)
def find_at_position_in_email():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT full_name, POSITION('@' IN email) AS at_position
                FROM customers
                WHERE email IS NOT NULL;
            """)
            return cur.fetchall()

# 8- NULL kategoriye 'Unknown' yaz (ürünler tablosundan ürünün ismini ve kategorisini product_category ismiyle dönecek sorguyu yazınız. Eğer kategori NULL ise NULL terimini 'Unknown' ile değiştiriniz.)
def fill_null_product_category():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT product_name, COALESCE(category, 'Unknown') AS product_category
                FROM products;
            """)
            return cur.fetchall()

# 9- Müşteri harcama sıralaması (RANK) (orders tablosu üzerinden customer_id, total_amount ve toplam harcamaya göre sıralamasını(RANK) rank_by_spend ismiyle dönen sorguyu yazınız.)
def rank_customers_by_spending():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    customer_id,
                    SUM(total_amount) AS total_spent,
                    RANK() OVER (ORDER BY SUM(total_amount) DESC) AS rank_by_spend
                FROM orders
                GROUP BY customer_id
                ORDER BY rank_by_spend;
            """)
            return cur.fetchall()

# 10- Müşteri siparişlerinde running total (Siparişlere göre çalışan toplamı (Running Total - SUM OVER) değerini bulan sorguyu yazınız.)
def running_total_per_customer():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    order_id,
                    order_date,
                    total_amount,
                    SUM(total_amount) OVER (PARTITION BY customer_id ORDER BY order_date) AS running_total_per_customer
                FROM orders
                ORDER BY order_date;
            """)
            return cur.fetchall()

# 11- Elektronik ve Beyaz Eşya ürünleri (Elektronik ve beyaz eşya ürünlerini tek listede getiren sorguyu yazınız)
def get_electronics_and_appliances():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT product_name, category
                FROM products
                WHERE category IN ('Electronics', 'Appliances');
            """)
            return cur.fetchall()

# 12-(Tüm siparişler ve eksik siparişleri birleştiren sorguyu yazınız.)
def get_orders_with_missing_customers():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    c.full_name,
                    o.order_id
                FROM orders o
                LEFT JOIN customers c ON o.customer_id = c.customer_id
                ORDER BY c.full_name, o.order_id;
            """)
            return cur.fetchall()