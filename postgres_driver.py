import os
import psycopg2
from dotenv import load_dotenv

class PostgresDriver:
    def __init__(self):
        """Инициализирует драйвер, загружая переменные окружения."""
        load_dotenv(encoding="utf-8-sig")
        self.conn_params = {
            "host": os.getenv("DB_HOST"),
            "port": os.getenv("DB_PORT"),
            "dbname": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
        }
        self.conn = None

    def __enter__(self):
        """Устанавливает соединение при входе в контекстный менеджер."""
        try:
            self.conn = psycopg2.connect(**self.conn_params)
            # Включаем автокоммит, чтобы операции изменения данных сразу фиксировались
            self.conn.autocommit = True
            print("Соединение с PostgreSQL установлено.")
            return self
        except psycopg2.Error as e:
            print(f"Ошибка при подключении к PostgreSQL: {e}")
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Закрывает соединение при выходе из контекстного менеджера."""
        if self.conn:
            self.conn.close()
            print("Соединение с PostgreSQL закрыто.")

    def execute_query(self, query, params=None):
        """Выполняет запрос с параметрами и возвращает результат."""
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            if cur.description:
                return cur.fetchall()

    def add_user(self, name, age):
        """Добавляет нового пользователя, используя параметризованный запрос."""
        query = "INSERT INTO users (name, age) VALUES (%s, %s) RETURNING id;"
        user_id = self.execute_query(query, (name, age))[0][0]
        print(f"Добавлен пользователь: {name}, ID: {user_id}")
        return user_id

    def add_order(self, user_id, amount):
        """Добавляет новый заказ для пользователя."""
        query = "INSERT INTO orders (user_id, amount) VALUES (%s, %s);"
        self.execute_query(query, (user_id, amount))
        print(f"Добавлен заказ на сумму {amount} для пользователя с ID {user_id}")

    def get_user_totals(self):
        """Получает сумму заказов по каждому пользователю."""
        query = """
        SELECT
            u.name,
            COALESCE(SUM(o.amount), 0) AS total_amount
        FROM users u
        LEFT JOIN orders o ON o.user_id = u.id
        GROUP BY u.id, u.name
        ORDER BY total_amount DESC;
        """
        return self.execute_query(query)