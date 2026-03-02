import os
from dotenv import load_dotenv
import clickhouse_connect
import pandas as pd

# Загрузка переменных окружения
load_dotenv()

# Класс для управления подключением к ClickHouse
class DatabaseConnection:
    def __init__(self, host, port, username, password, database='theatre'):
        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password
        )
        self.database = database

    # Закрытие соединения с базой данных
    def close(self):
        self.client.close()

    # Очистка всех таблиц в базе данных
    def clear_database(self):
        try:
            tables = self.client.query(f"SHOW TABLES FROM {self.database}").result_rows
            for table in tables:
                table_name = table[0]
                self.client.command(f"DROP TABLE IF EXISTS {self.database}.{table_name}")
            print(f"База данных {self.database} успешно очищена")
            return True
        except Exception as e:
            print(f"Ошибка при очистке базы данных: {e}")
            return False

# Класс для создания таблиц и заполнения данными
class TheatreDatabaseCreator:
    def __init__(self, connection):
        self.connection = connection
        self.database = connection.database

    # Создание всех таблиц
    def create_tables(self):
        # Таблица actors
        self.connection.client.command(f"""
            CREATE TABLE IF NOT EXISTS {self.database}.actors
            (
                actor_id UInt32,
                first_name String,
                last_name String,
                birth_date Date,
                rank Nullable(String),
                awards Array(String)
            )
            ENGINE = MergeTree()
            PRIMARY KEY (last_name, first_name, actor_id)
            ORDER BY (last_name, first_name, actor_id)
        """)
        print("Таблица actors создана")

        # Таблица employees
        self.connection.client.command(f"""
            CREATE TABLE IF NOT EXISTS {self.database}.employees
            (
                employee_id UInt32,
                position String,
                first_name String,
                last_name String
            )
            ENGINE = MergeTree()
            PRIMARY KEY (position, last_name, employee_id)
            ORDER BY (position, last_name, employee_id)
        """)
        print("Таблица employees создана")

        # Таблица performances
        self.connection.client.command(f"""
            CREATE TABLE IF NOT EXISTS {self.database}.performances
            (
                performance_id UInt32,
                title String,
                author String,
                genre String,
                audience_type String,
                premiere_date Date
            )
            ENGINE = MergeTree()
            PRIMARY KEY (premiere_date, title, performance_id)
            ORDER BY (premiere_date, title, performance_id)
        """)
        print("Таблица performances создана")

        # Таблица shows
        self.connection.client.command(f"""
            CREATE TABLE IF NOT EXISTS {self.database}.shows
            (
                show_id UInt32,
                performance_id UInt32,
                show_date Date,
                show_time String,
                hall String
            )
            ENGINE = MergeTree()
            PRIMARY KEY (show_date, show_time, show_id)
            ORDER BY (show_date, show_time, show_id)
        """)
        print("Таблица shows создана")

        # Таблица tickets
        self.connection.client.command(f"""
            CREATE TABLE IF NOT EXISTS {self.database}.tickets
            (
                ticket_id UInt32,
                show_id UInt32,
                row_number UInt32,
                seat_number UInt32,
                price UInt32,
                sale_date Date
            )
            ENGINE = MergeTree()
            PRIMARY KEY (sale_date, show_id, ticket_id)
            ORDER BY (sale_date, show_id, ticket_id)
        """)
        print("Таблица tickets создана")

    # Заполнение таблиц данными
    def insert_data(self):
        # Заполнение актеров
        actors_data = [
            (1, 'Бен', 'Барнс', '1981-08-20', 'Member of the Order of the British Empire (MBE)',
             ['Laurence Olivier Award', 'Critics Circle Theatre Award']),
            (2, 'Кира', 'Найтли', '1985-03-26', 'Officer of the Order of the British Empire (OBE)',
             ['Laurence Olivier Award', 'Empire Award']),
            (3, 'Кейт', 'Уинслет', '1975-10-05', 'Commander of the Order of the British Empire (CBE)',
             ['BAFTA', 'Golden Globe', 'Laurence Olivier Award']),
            (4, 'Эмили', 'Блант', '1983-02-23', 'Member of the Order of the British Empire (MBE)',
             ['Golden Globe', 'Screen Actors Guild Award']),
            (5, 'Хью', 'Лори', '1959-06-11', 'Commander of the Order of the British Empire (CBE)',
             ['Golden Globe', 'Screen Actors Guild Award', 'BAFTA']),
            (6, 'Николас', 'Холт', '1989-12-07', None, []),
            (7, 'Киллиан', 'Мерфи', '1976-05-25', 'Officer of the Order of the British Empire (OBE)',
             ['BAFTA', 'Golden Globe', 'Laurence Olivier Award'])
        ]

        for actor in actors_data:
            actor_id, first_name, last_name, birth_date, rank, awards = actor

            # Форматирование массива наград
            if not awards:
                awards_str = "[]"
            else:
                awards_items = [f"'{a}'" for a in awards]
                awards_str = "[" + ",".join(awards_items) + "]"

            # Форматирование звания
            rank_str = "NULL" if rank is None else f"'{rank}'"

            query = f"""
                INSERT INTO {self.database}.actors 
                (actor_id, first_name, last_name, birth_date, rank, awards) 
                VALUES 
                ({actor_id}, '{first_name}', '{last_name}', '{birth_date}', 
                 {rank_str}, {awards_str})
            """
            self.connection.client.command(query)

        print(f"Добавлено актеров: {len(actors_data)}")

        # Заполнение сотрудников
        employees_data = [
            (1, 'Директор', 'Джеймс', 'Томпсон'),
            (2, 'Режиссер-постановщик', 'Майкл', 'Харрисон'),
            (3, 'Художник-постановщик', 'Сара', 'Коннор'),
            (4, 'Дирижер-постановщик', 'Роберт', 'Уильямс'),
            (5, 'Заведующий труппой', 'Эмма', 'Уотсон'),
            (6, 'Кассир', 'Оливия', 'Браун'),
            (7, 'Администратор', 'Уильям', 'Джонс'),
            (8, 'Художник по свету', 'Дэвид', 'Миллер'),
            (9, 'Звукорежиссер', 'Кристофер', 'Дэвис'),
            (10, 'Заведующий костюмерной', 'Элизабет', 'Тейлор')
        ]

        for emp in employees_data:
            query = f"""
                INSERT INTO {self.database}.employees 
                (employee_id, position, first_name, last_name) 
                VALUES 
                ({emp[0]}, '{emp[1]}', '{emp[2]}', '{emp[3]}')
            """
            self.connection.client.command(query)

        print(f"Добавлено сотрудников: {len(employees_data)}")

        # Заполнение спектаклей
        performances_data = [
            (1, 'Гамлет', 'Уильям Шекспир', 'Трагедия', 'Взрослый', '2023-03-15'),
            (2, 'Вишневый сад', 'Антон Чехов', 'Комедия', 'Взрослый', '2022-11-20'),
            (3, 'Мастер и Маргарита', 'Михаил Булгаков', 'Мистическая драма', 'Взрослый', '2024-01-25'),
            (4, 'Золушка', 'Шарль Перро', 'Сказка', 'Детский', '2023-12-10'),
            (5, 'Аида', 'Джузеппе Верди', 'Опера', 'Взрослый', '2021-09-05'),
            (6, 'Щелкунчик', 'Петр Чайковский', 'Балет', 'Детский', '2022-12-18'),
            (7, 'Три сестры', 'Антон Чехов', 'Драма', 'Взрослый', '2023-05-30'),
            (8, 'Собачье сердце', 'Михаил Булгаков', 'Сатира', 'Взрослый', '2024-02-14')
        ]

        for perf in performances_data:
            query = f"""
                INSERT INTO {self.database}.performances 
                (performance_id, title, author, genre, audience_type, premiere_date) 
                VALUES 
                ({perf[0]}, '{perf[1]}', '{perf[2]}', '{perf[3]}', '{perf[4]}', '{perf[5]}')
            """
            self.connection.client.command(query)

        print(f"Добавлено спектаклей: {len(performances_data)}")

        # Заполнение представлений
        shows_data = [
            (1, 1, '2026-05-15', '19:00', 'Основная сцена'),
            (2, 2, '2026-05-16', '19:00', 'Основная сцена'),
            (3, 4, '2026-05-17', '18:00', 'Малая сцена'),
            (4, 3, '2026-05-18', '19:00', 'Основная сцена'),
            (5, 5, '2026-05-20', '19:00', 'Основная сцена'),
            (6, 6, '2026-05-22', '18:00', 'Малая сцена'),
            (7, 1, '2026-05-23', '14:00', 'Основная сцена'),
            (8, 2, '2026-05-24', '19:00', 'Основная сцена')
        ]

        for show in shows_data:
            query = f"""
                INSERT INTO {self.database}.shows 
                (show_id, performance_id, show_date, show_time, hall) 
                VALUES 
                ({show[0]}, {show[1]}, '{show[2]}', '{show[3]}', '{show[4]}')
            """
            self.connection.client.command(query)

        print(f"Добавлено представлений: {len(shows_data)}")

        # Заполнение билетов
        tickets_data = [
            (1, 1, 5, 12, 1500, '2026-04-01'),
            (2, 1, 3, 8, 2000, '2026-04-03'),
            (3, 2, 10, 5, 1200, '2026-04-05'),
            (4, 2, 2, 15, 2500, '2026-04-07'),
            (5, 3, 7, 3, 1800, '2026-04-10'),
            (6, 3, 4, 10, 2200, '2026-04-12'),
            (7, 4, 1, 1, 3000, '2026-04-15'),
            (8, 4, 8, 7, 1600, '2026-04-18'),
            (9, 5, 6, 4, 1900, '2026-04-20'),
            (10, 5, 9, 9, 1400, '2026-04-22')
        ]

        for ticket in tickets_data:
            query = f"""
                INSERT INTO {self.database}.tickets 
                (ticket_id, show_id, row_number, seat_number, price, sale_date) 
                VALUES 
                ({ticket[0]}, {ticket[1]}, {ticket[2]}, {ticket[3]}, {ticket[4]}, '{ticket[5]}')
            """
            self.connection.client.command(query)

        print(f"Добавлено билетов: {len(tickets_data)}")

# Класс для выполнения запросов к базе данных театра
class TheatreQueries:
    def __init__(self, connection):
        self.connection = connection
        self.database = connection.database
        # Переключаемся на нужную базу данных
        self.connection.client.command(f"USE {self.database}")


    def query1(self):
        """Запрос 1: Все спектакли в репертуаре"""
        query = """
            SELECT title, author, genre, audience_type, premiere_date
            FROM performances
            ORDER BY premiere_date DESC
        """
        result = self.connection.client.query(query)
        df = pd.DataFrame(
            result.result_rows,
            columns=['title', 'author', 'genre', 'audience_type', 'premiere_date']
        )
        return df

    def query2(self):
        """Запрос 2: Ближайшие представления"""
        query = """
            SELECT p.title, s.show_date, s.show_time, s.hall
            FROM shows s
            JOIN performances p ON s.performance_id = p.performance_id
            WHERE s.show_date >= today()
            ORDER BY s.show_date, s.show_time
            LIMIT 5
        """
        result = self.connection.client.query(query)
        df = pd.DataFrame(
            result.result_rows,
            columns=['title', 'show_date', 'show_time', 'hall']
        )
        return df

    def query3(self):
        """Запрос 3: Занятость актеров"""
        query = """
            SELECT CONCAT(a.first_name, ' ', a.last_name) AS actor_name,
                p.title AS performance,
                COUNT(DISTINCT s.show_id) AS appearances
            FROM actors a
            CROSS JOIN shows s 
            JOIN performances p ON s.performance_id = p.performance_id
            GROUP BY actor_name, p.title
            ORDER BY actor_name
        """
        result = self.connection.client.query(query)
        df = pd.DataFrame(
            result.result_rows,
            columns=['actor_name', 'performance', 'appearances']
        )
        return df

    def query4(self):
        """Запрос 4: Продажи билетов по спектаклям"""
        query = """
            SELECT 
                p.title,
                COUNT(t.ticket_id) AS tickets_sold,
                SUM(t.price) AS total_revenue,
                AVG(t.price) AS avg_ticket_price
            FROM tickets t
            JOIN shows s ON t.show_id = s.show_id
            JOIN performances p ON s.performance_id = p.performance_id
            GROUP BY p.title
            ORDER BY total_revenue DESC
        """
        result = self.connection.client.query(query)
        df = pd.DataFrame(
            result.result_rows,
            columns=['title', 'tickets_sold', 'total_revenue', 'avg_ticket_price']
        )
        return df

    def query5(self):
        """Запрос 5: Количество мест, проданное на каждое представление"""
        query = """
            SELECT p.title, s.show_date, s.show_time, s.hall,
                COUNT(t.ticket_id) AS sold_seats,
                SUM(t.price) AS revenue
            FROM shows s
            JOIN performances p ON s.performance_id = p.performance_id
            LEFT JOIN tickets t ON s.show_id = t.show_id
            GROUP BY p.title, s.show_date, s.show_time, s.hall
            ORDER BY s.show_date
        """
        result = self.connection.client.query(query)
        df = pd.DataFrame(
            result.result_rows,
            columns=['title', 'show_date', 'show_time', 'hall', 'sold_seats', 'revenue']
        )
        return df

    def query6(self):
        """Запрос 6: Спектакли по авторам и их кассовые сборы"""
        query = """
            SELECT p.author,
                COUNT(DISTINCT p.performance_id) AS plays_count,
                COUNT(t.ticket_id) AS tickets_sold,
                SUM(t.price) AS total_revenue,
                ROUND(AVG(t.price), 2) AS avg_ticket_price
            FROM performances p
            JOIN shows s ON p.performance_id = s.performance_id
            JOIN tickets t ON s.show_id = t.show_id
            GROUP BY p.author
            ORDER BY total_revenue DESC
        """
        result = self.connection.client.query(query)
        df = pd.DataFrame(
            result.result_rows,
            columns=['author', 'plays_count', 'tickets_sold', 'total_revenue', 'avg_ticket_price']
        )
        return df

    def query7(self):
        """Запрос 7: Наиболее прибыльные жанры"""
        query = """
            SELECT p.genre,
                COUNT(DISTINCT p.performance_id) AS performances_count,
                COUNT(t.ticket_id) AS tickets_sold,
                SUM(t.price) AS total_revenue,
                ROUND(AVG(t.price), 2) AS avg_ticket_price
            FROM performances p
            JOIN shows s ON p.performance_id = s.performance_id
            JOIN tickets t ON s.show_id = t.show_id
            GROUP BY p.genre
            ORDER BY total_revenue DESC
        """
        result = self.connection.client.query(query)
        df = pd.DataFrame(
            result.result_rows,
            columns=['genre', 'performances_count', 'tickets_sold', 'total_revenue', 'avg_ticket_price']
        )
        return df

    # Выполнение всех запросов и вывод результатов
    def execute_all_queries(self):
        print("Запрос 1: Все спектакли в репертуаре")
        df1 = self.query1()
        print(df1.to_string(index=False))

        print("\nЗапрос 2: Ближайшие представления")
        df2 = self.query2()
        print(df2.to_string(index=False))

        print("\nЗапрос 3: Занятость актеров")
        df3 = self.query3()
        print(df3.to_string(index=False))

        print("\nЗапрос 4: Продажи билетов по спектаклям")
        df4 = self.query4()
        print(df4.to_string(index=False))

        print("\nЗапрос 5: Количество мест, проданное на каждое представление")
        df5 = self.query5()
        print(df5.to_string(index=False))

        print("\nЗапрос 6: Спектакли по авторам и их кассовые сборы")
        df6 = self.query6()
        print(df6.to_string(index=False))

        print("\nЗапрос 7: Наиболее прибыльные жанры")
        df7 = self.query7()
        print(df7.to_string(index=False))

        return {
            'query1': df1,
            'query2': df2,
            'query3': df3,
            'query4': df4,
            'query5': df5,
            'query6': df6,
            'query7': df7
        }

# Основная функция программы
def main():
    # Получение параметров подключения
    host = os.getenv("CLICKHOUSE_HOST", "localhost")
    port = int(os.getenv("CLICKHOUSE_PORT", 18123))
    username = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "0000")

    # Установление соединения
    connection = DatabaseConnection(host, port, username, password, database='theatre')

    # Создание и заполнение базы данных
    connection.clear_database()

    db_creator = TheatreDatabaseCreator(connection)
    db_creator.create_tables()
    db_creator.insert_data()

    print("\nБаза данных theatre успешно создана и заполнена данными!")

    # Проверка создания таблиц
    tables = connection.client.query(f"SHOW TABLES FROM theatre").result_rows
    print(f"\nСозданные таблицы: {[table[0] for table in tables]}")

    # Выполнение запросов
    queries = TheatreQueries(connection)
    results = queries.execute_all_queries()

    # Закрытие соединения
    connection.close()
    print("Соединение с базой данных закрыто")

if __name__ == "__main__":
    main()