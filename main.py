import psycopg2
from postgres_driver import PostgresDriver

def main():
    try:
        # Используем контекстный менеджер для гарантии закрытия соединения
        with PostgresDriver() as driver:
            # Добавляем пользователей
            user1_id = driver.add_user("Alice", 28)
            user2_id = driver.add_user("Bob", 35)
            driver.add_user("Charlie", 22) # Пользователь без заказов

            # Добавляем заказы
            driver.add_order(user1_id, 499.90)
            driver.add_order(user1_id, 120.50)
            driver.add_order(user2_id, 750.00)

            # Получаем и выводим итоговые суммы
            print("\n--- Итоговая сумма заказов по пользователям ---")
            user_totals = driver.get_user_totals()
            for name, total_amount in user_totals:
                print(f"{name} — {total_amount:.2f}")

    except psycopg2.Error as e:
        print(f"Произошла ошибка базы данных: {e}")
    except Exception as e:
        print(f"Произошла непредвиденная ошибка: {e}")


if __name__ == "__main__":
    main()