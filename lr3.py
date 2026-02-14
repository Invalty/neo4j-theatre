import os
import random
from datetime import datetime, timedelta
from dotenv import load_dotenv
from faker import Faker
from neo4j import GraphDatabase
from prettytable import PrettyTable

# Загрузка переменных окружения
load_dotenv()

# Получение учетных данных Neo4j из переменных окружения
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
NEO4J_DBNAME = os.getenv("NEO4J_DBNAME")


# Класс для управления подключением и сессией базы данных Neo4j.
class Neo4jConnection:
    def __init__(self, uri, username, password):
        self._uri = uri
        self._username = username
        self._password = password
        self._driver = None

    def __enter__(self):
        self._driver = GraphDatabase.driver(
            self._uri, auth=(self._username, self._password)
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        if self._driver:
            self._driver.close()

    def execute_query(self, query, parameters=None):
        with self._driver.session(database=NEO4J_DBNAME) as session:
            result = session.run(query, parameters or {})
            return [record for record in result]


# Класс, представляющий сущность Actor в БД
class Actor:
    def __init__(self, connection):
        self.connection = connection

    def clear(self):
        query = "MATCH (a:Actor) DETACH DELETE a"
        self.connection.execute_query(query)

    def create(self, actor_id, first_name, last_name, birth_date, rank=None, awards=None):
        if awards is None:
            awards = []

        query = """
        CREATE (:Actor {
            actorId: $actorId,
            firstName: $firstName,
            lastName: $lastName,
            birthDate: date($birthDate),
            rank: $rank,
            awards: $awards
        })
        """
        self.connection.execute_query(
            query,
            {
                "actorId": actor_id,
                "firstName": first_name,
                "lastName": last_name,
                "birthDate": birth_date,
                "rank": rank,
                "awards": awards
            },
        )

    def get_all(self):
        query = """
        MATCH (a:Actor) 
        RETURN a.actorId AS actorId, 
               a.firstName AS firstName, 
               a.lastName AS lastName, 
               a.birthDate AS birthDate,
               a.rank AS rank,
               a.awards AS awards
        """
        return self.connection.execute_query(query)

    def show_all(self):
        actors = self.get_all()
        actors_table = PrettyTable()
        actors_table.field_names = [
            "ID актера",
            "Имя",
            "Фамилия",
            "Дата рождения",
            "Звание",
            "Награды"
        ]
        for actor in actors:
            awards_str = ", ".join(actor["awards"]) if actor["awards"] else ""
            actors_table.add_row(
                [
                    actor["actorId"],
                    actor["firstName"],
                    actor["lastName"],
                    actor["birthDate"],
                    actor["rank"] if actor["rank"] else "",
                    awards_str
                ]
            )
        print("\nВсе актеры:")
        print(actors_table)


# Класс, представляющий сущность Employee в БД
class Employee:
    def __init__(self, connection):
        self.connection = connection

    def clear(self):
        query = "MATCH (e:Employee) DETACH DELETE e"
        self.connection.execute_query(query)

    def create(self, employee_id, position, first_name, last_name):
        query = """
        CREATE (:Employee {
            employeeId: $employeeId,
            position: $position,
            firstName: $firstName,
            lastName: $lastName
        })
        """
        self.connection.execute_query(
            query,
            {
                "employeeId": employee_id,
                "position": position,
                "firstName": first_name,
                "lastName": last_name
            },
        )

    def get_all(self):
        query = """
        MATCH (e:Employee) 
        RETURN e.employeeId AS employeeId, 
               e.position AS position, 
               e.firstName AS firstName, 
               e.lastName AS lastName
        ORDER BY e.employeeId
        """
        return self.connection.execute_query(query)

    def show_all(self):
        employees = self.get_all()
        employees_table = PrettyTable()
        employees_table.field_names = [
            "ID сотрудника",
            "Должность",
            "Имя",
            "Фамилия"
        ]
        for emp in employees:
            employees_table.add_row(
                [
                    emp["employeeId"],
                    emp["position"],
                    emp["firstName"],
                    emp["lastName"]
                ]
            )
        print("\nВсе сотрудники:")
        print(employees_table)

# Класс, представляющий сущность Performance в БД
class Performance:
    def __init__(self, connection):
        self.connection = connection

    def clear(self):
        query = "MATCH (p:Performance) DETACH DELETE p"
        self.connection.execute_query(query)

    def create(self, performance_id, title, author, genre, audience_type, premiere_date):
        query = """
        CREATE (:Performance {
            performanceId: $performanceId,
            title: $title,
            author: $author,
            genre: $genre,
            audienceType: $audienceType,
            premiereDate: date($premiereDate)
        })
        """
        self.connection.execute_query(
            query,
            {
                "performanceId": performance_id,
                "title": title,
                "author": author,
                "genre": genre,
                "audienceType": audience_type,
                "premiereDate": premiere_date
            },
        )

    def get_all(self):
        query = """
        MATCH (p:Performance) 
        RETURN p.performanceId AS performanceId, 
               p.title AS title, 
               p.author AS author, 
               p.genre AS genre,
               p.audienceType AS audienceType,
               p.premiereDate AS premiereDate
        ORDER BY p.performanceId
        """
        return self.connection.execute_query(query)

    def show_all(self):
        performances = self.get_all()
        perf_table = PrettyTable()
        perf_table.field_names = [
            "ID спектакля",
            "Название",
            "Автор",
            "Жанр",
            "Аудитория",
            "Дата премьеры"
        ]
        for perf in performances:
            perf_table.add_row(
                [
                    perf["performanceId"],
                    perf["title"],
                    perf["author"],
                    perf["genre"],
                    perf["audienceType"],
                    perf["premiereDate"]
                ]
            )
        print("\nВсе спектакли:")
        print(perf_table)

# Класс, представляющий сущность Show (Показ спектакля) в БД
class Show:
    def __init__(self, connection):
        self.connection = connection

    def clear(self):
        query = "MATCH (s:Show) DETACH DELETE s"
        self.connection.execute_query(query)

    def create(self, show_id, date, time, hall):
        query = """
        CREATE (:Show {
            showId: $showId,
            date: date($date),
            time: $time,
            hall: $hall
        })
        """
        self.connection.execute_query(
            query,
            {
                "showId": show_id,
                "date": date,
                "time": time,
                "hall": hall
            },
        )

    def get_all(self):
        query = """
        MATCH (s:Show) 
        RETURN s.showId AS showId, 
               s.date AS date, 
               s.time AS time, 
               s.hall AS hall
        ORDER BY s.date, s.time
        """
        return self.connection.execute_query(query)

    def show_all(self):
        shows = self.get_all()
        shows_table = PrettyTable()
        shows_table.field_names = [
            "ID показа",
            "Дата",
            "Время",
            "Зал"
        ]
        for show in shows:
            shows_table.add_row(
                [
                    show["showId"],
                    show["date"],
                    show["time"],
                    show["hall"]
                ]
            )
        print("\nВсе показы:")
        print(shows_table)

# Класс, представляющий сущность Ticket в БД
class Ticket:
    def __init__(self, connection):
        self.connection = connection

    def clear(self):
        query = "MATCH (t:Ticket) DETACH DELETE t"
        self.connection.execute_query(query)

    def create(self, ticket_id, row, seat, price, sale_date):
        query = """
        CREATE (:Ticket {
            ticketId: $ticketId,
            row: $row,
            seat: $seat,
            price: $price,
            saleDate: date($saleDate)
        })
        """
        self.connection.execute_query(
            query,
            {
                "ticketId": ticket_id,
                "row": row,
                "seat": seat,
                "price": price,
                "saleDate": sale_date
            },
        )

    def get_all(self):
        query = """
        MATCH (t:Ticket) 
        RETURN t.ticketId AS ticketId, 
               t.row AS row, 
               t.seat AS seat, 
               t.price AS price,
               t.saleDate AS saleDate
        ORDER BY t.ticketId
        """
        return self.connection.execute_query(query)

    def show_all(self):
        tickets = self.get_all()
        tickets_table = PrettyTable()
        tickets_table.field_names = [
            "ID билета",
            "Ряд",
            "Место",
            "Цена",
            "Дата продажи"
        ]
        for ticket in tickets:
            tickets_table.add_row(
                [
                    ticket["ticketId"],
                    ticket["row"],
                    ticket["seat"],
                    ticket["price"],
                    ticket["saleDate"]
                ]
            )
        print("\nВсе билеты:")
        print(tickets_table)

# Класс для представления связей между узлами
class Relationship:
    def __init__(self, connection):
        self.connection = connection

    # Связь: Актер играет роль в спектакле
    def create_actor_plays_role(self, actor_first_name, actor_last_name, performance_title, role_name):
        query = """
        MATCH (a:Actor {firstName: $actorFirstName, lastName: $actorLastName})
        MATCH (p:Performance {title: $performanceTitle})
        CREATE (a)-[:Играет_роль {roleName: $roleName}]->(p)
        """
        self.connection.execute_query(
            query,
            {
                "actorFirstName": actor_first_name,
                "actorLastName": actor_last_name,
                "performanceTitle": performance_title,
                "roleName": role_name
            }
        )

    # Связь: Сотрудник работает над спектаклем
    def create_employee_works_on(self, employee_first_name, employee_last_name, performance_title):
        query = """
        MATCH (e:Employee {firstName: $employeeFirstName, lastName: $employeeLastName})
        MATCH (p:Performance {title: $performanceTitle})
        CREATE (e)-[:Работает_над]->(p)
        """
        self.connection.execute_query(
            query,
            {
                "employeeFirstName": employee_first_name,
                "employeeLastName": employee_last_name,
                "performanceTitle": performance_title
            }
        )

    # Связь: Директор утверждает спектакль
    def create_director_approves(self, director_first_name, director_last_name, performance_title):
        query = """
        MATCH (e:Employee {firstName: $directorFirstName, lastName: $directorLastName, position: 'Директор'})
        MATCH (p:Performance {title: $performanceTitle})
        CREATE (e)-[:Утверждает]->(p)
        """
        self.connection.execute_query(
            query,
            {
                "directorFirstName": director_first_name,
                "directorLastName": director_last_name,
                "performanceTitle": performance_title
            }
        )

    # Связь: Спектакль включен в показ
    def create_performance_has_show(self, performance_title, show_date, show_time, hall):
        query = """
        MATCH (p:Performance {title: $performanceTitle})
        MATCH (s:Show {date: date($showDate), time: $showTime, hall: $hall})
        CREATE (p)-[:Включен_в_показ]->(s)
        """
        self.connection.execute_query(
            query,
            {
                "performanceTitle": performance_title,
                "showDate": show_date,
                "showTime": show_time,
                "hall": hall
            }
        )

    # Связь: Билет продан на показ
    def create_ticket_for_show(self, ticket_row, ticket_seat, show_date, show_time, hall):
        query = """
        MATCH (t:Ticket {row: $ticketRow, seat: $ticketSeat})
        MATCH (s:Show {date: date($showDate), time: $showTime, hall: $hall})
        CREATE (t)-[:Продан_на_показ]->(s)
        """
        self.connection.execute_query(
            query,
            {
                "ticketRow": ticket_row,
                "ticketSeat": ticket_seat,
                "showDate": show_date,
                "showTime": show_time,
                "hall": hall
            }
        )

# Класс сервиса для управления операциями базы данных театра.
class TheatreDatabaseService:
    def __init__(self, connection):
        self.connection = connection
        self.actor_model = Actor(connection)
        self.employee_model = Employee(connection)
        self.performance_model = Performance(connection)
        self.show_model = Show(connection)
        self.ticket_model = Ticket(connection)
        self.relationship = Relationship(connection)

    def clear_database(self):
        # Сначала удаляем связи, затем узлы
        query_relationships = "MATCH ()-[r]->() DELETE r"
        self.connection.execute_query(query_relationships)

        self.ticket_model.clear()
        self.show_model.clear()
        self.performance_model.clear()
        self.employee_model.clear()
        self.actor_model.clear()
        print("Database cleared")

    def create_sample_data(self):
        # Создание актеров
        self.actor_model.create(1, "Бен", "Барнс", "1981-08-20",
                                "Member of the Order of the British Empire (MBE)",
                                ["Laurence Olivier Award", "Critics' Circle Theatre Award"])
        self.actor_model.create(2, "Кира", "Найтли", "1985-03-26",
                                "Officer of the Order of the British Empire (OBE)",
                                ["Laurence Olivier Award", "Empire Award"])
        self.actor_model.create(3, "Кейт", "Уинслет", "1975-10-05",
                                "Commander of the Order of the British Empire (CBE)",
                                ["BAFTA", "Golden Globe", "Laurence Olivier Award"])
        self.actor_model.create(4, "Эмили", "Блант", "1983-02-23",
                                "Member of the Order of the British Empire (MBE)",
                                ["Golden Globe", "Screen Actors Guild Award"])
        self.actor_model.create(5, "Хью", "Лори", "1959-06-11",
                                "Commander of the Order of the British Empire (CBE)",
                                ["Golden Globe", "Screen Actors Guild Award", "BAFTA"])
        self.actor_model.create(6, "Николас", "Холт", "1989-12-07", None, [])
        self.actor_model.create(7, "Киллиан", "Мерфи", "1976-05-25",
                                "Officer of the Order of the British Empire (OBE)",
                                ["BAFTA", "Golden Globe", "Laurence Olivier Award"])

        # Создание сотрудников
        self.employee_model.create(1, "Директор", "Джеймс", "Томпсон")
        self.employee_model.create(2, "Режиссер-постановщик", "Майкл", "Харрисон")
        self.employee_model.create(3, "Художник-постановщик", "Сара", "Коннор")
        self.employee_model.create(4, "Дирижер-постановщик", "Роберт", "Уильямс")
        self.employee_model.create(5, "Заведующий труппой", "Эмма", "Уотсон")
        self.employee_model.create(6, "Кассир", "Оливия", "Браун")
        self.employee_model.create(7, "Администратор", "Уильям", "Джонс")
        self.employee_model.create(8, "Художник по свету", "Дэвид", "Миллер")
        self.employee_model.create(9, "Звукорежиссер", "Кристофер", "Дэвис")
        self.employee_model.create(10, "Заведующий костюмерной", "Элизабет", "Тейлор")

        # Создание спектаклей
        self.performance_model.create(1, "Гамлет", "Уильям Шекспир", "Трагедия", "Взрослый", "2023-03-15")
        self.performance_model.create(2, "Вишневый сад", "Антон Чехов", "Комедия", "Взрослый", "2022-11-20")
        self.performance_model.create(3, "Мастер и Маргарита", "Михаил Булгаков", "Мистическая драма", "Взрослый",
                                      "2024-01-25")
        self.performance_model.create(4, "Золушка", "Шарль Перро", "Сказка", "Детский", "2023-12-10")
        self.performance_model.create(5, "Аида", "Джузеппе Верди", "Опера", "Взрослый", "2021-09-05")
        self.performance_model.create(6, "Щелкунчик", "Петр Чайковский", "Балет", "Детский", "2022-12-18")
        self.performance_model.create(7, "Три сестры", "Антон Чехов", "Драма", "Взрослый", "2023-05-30")
        self.performance_model.create(8, "Собачье сердце", "Михаил Булгаков", "Сатира", "Взрослый", "2024-02-14")

        # Создание показов
        self.show_model.create(1, "2026-05-15", "19:00", "Основная сцена")
        self.show_model.create(2, "2026-05-16", "19:00", "Основная сцена")
        self.show_model.create(3, "2026-05-17", "18:00", "Малая сцена")
        self.show_model.create(4, "2026-05-18", "19:00", "Основная сцена")
        self.show_model.create(5, "2026-05-20", "19:00", "Основная сцена")
        self.show_model.create(6, "2026-05-22", "18:00", "Малая сцена")
        self.show_model.create(7, "2026-05-23", "14:00", "Основная сцена")
        self.show_model.create(8, "2026-05-24", "19:00", "Основная сцена")

        # Создание билетов
        self.ticket_model.create(1, 5, 12, 1500, "2026-04-01")
        self.ticket_model.create(2, 5, 13, 1500, "2026-04-01")
        self.ticket_model.create(3, 3, 8, 2000, "2026-04-03")
        self.ticket_model.create(4, 3, 9, 2000, "2026-04-03")
        self.ticket_model.create(5, 2, 5, 1200, "2026-04-05")
        self.ticket_model.create(6, 2, 6, 1200, "2026-04-05")
        self.ticket_model.create(7, 10, 5, 1200, "2026-04-05")
        self.ticket_model.create(8, 2, 15, 2500, "2026-04-07")
        self.ticket_model.create(9, 7, 3, 1800, "2026-04-10")
        self.ticket_model.create(10, 4, 10, 2200, "2026-04-12")

    # Создание всех связей между узлами
    def create_sample_relationships(self):
        # Актер -> Играет роль -> Спектакль
        self.relationship.create_actor_plays_role("Киллиан", "Мерфи", "Гамлет", "Гамлет")
        self.relationship.create_actor_plays_role("Кира", "Найтли", "Вишневый сад", "Раневская")
        self.relationship.create_actor_plays_role("Бен", "Барнс", "Мастер и Маргарита", "Мастер")
        self.relationship.create_actor_plays_role("Кейт", "Уинслет", "Аида", "Аида")
        self.relationship.create_actor_plays_role("Эмили", "Блант", "Три сестры", "Маша")
        self.relationship.create_actor_plays_role("Хью", "Лори", "Собачье сердце", "Шариков")

        # Сотрудник -> Работает над -> Спектакль
        self.relationship.create_employee_works_on("Джеймс", "Томпсон", "Гамлет")
        self.relationship.create_employee_works_on("Сара", "Коннор", "Вишневый сад")
        self.relationship.create_employee_works_on("Майкл", "Харрисон", "Гамлет")
        self.relationship.create_employee_works_on("Роберт", "Уильямс", "Аида")

        # Директор -> Утверждает -> Спектакль
        self.relationship.create_director_approves("Джеймс", "Томпсон", "Гамлет")
        self.relationship.create_director_approves("Джеймс", "Томпсон", "Вишневый сад")
        self.relationship.create_director_approves("Джеймс", "Томпсон", "Мастер и Маргарита")
        self.relationship.create_director_approves("Джеймс", "Томпсон", "Золушка")

        # Спектакль -> Включен в -> Показ
        self.relationship.create_performance_has_show("Гамлет", "2026-05-15", "19:00", "Основная сцена")
        self.relationship.create_performance_has_show("Вишневый сад", "2026-05-16", "19:00", "Основная сцена")
        self.relationship.create_performance_has_show("Мастер и Маргарита", "2026-05-17", "18:00", "Малая сцена")
        self.relationship.create_performance_has_show("Аида", "2026-05-18", "19:00", "Основная сцена")
        self.relationship.create_performance_has_show("Щелкунчик", "2026-05-20", "19:00", "Основная сцена")
        self.relationship.create_performance_has_show("Три сестры", "2026-05-22", "18:00", "Малая сцена")

        # Билет -> Продан на -> Показ
        self.relationship.create_ticket_for_show(5, 12, "2026-05-15", "19:00", "Основная сцена")
        self.relationship.create_ticket_for_show(5, 13, "2026-05-15", "19:00", "Основная сцена")
        self.relationship.create_ticket_for_show(3, 8, "2026-05-16", "19:00", "Основная сцена")
        self.relationship.create_ticket_for_show(3, 9, "2026-05-16", "19:00", "Основная сцена")
        self.relationship.create_ticket_for_show(2, 5, "2026-05-17", "18:00", "Малая сцена")
        self.relationship.create_ticket_for_show(2, 6, "2026-05-17", "18:00", "Малая сцена")

    # Выполнение всех 10 запросов и вывод результатов
    def execute_all_queries(self):
        print("\n1. Все актеры и их звания:")
        query1 = """
        MATCH (a:Actor) 
        RETURN a.firstName AS firstName, a.lastName AS lastName, a.rank AS rank
        """
        result = self.connection.execute_query(query1)
        table = PrettyTable(["Имя", "Фамилия", "Звание"])
        for record in result:
            table.add_row([record["firstName"], record["lastName"], record["rank"] or "-"])
        print(table)

        print("\n2. Спектакли жанра 'Комедия':")
        query2 = """
        MATCH (p:Performance)
        WHERE p.genre = 'Комедия'
        RETURN p.title AS title, p.author AS author, p.premiereDate AS premiereDate
        """
        result = self.connection.execute_query(query2)
        table = PrettyTable(["Название", "Автор", "Дата премьеры"])
        for record in result:
            table.add_row([record["title"], record["author"], record["premiereDate"]])
        print(table)

        print("\n3. Актеры в спектакле 'Вишневый сад':")
        query3 = """
        MATCH (a:Actor)-[r:Играет_роль]->(p:Performance)
        WHERE p.title = 'Вишневый сад'
        RETURN a.firstName AS firstName, a.lastName AS lastName, r.roleName AS role
        """
        result = self.connection.execute_query(query3)
        table = PrettyTable(["Имя", "Фамилия", "Роль"])
        for record in result:
            table.add_row([record["firstName"], record["lastName"], record["role"]])
        print(table)

        print("\n4. Спектакли, над которыми работал Джеймс Томпсон:")
        query4 = """
        MATCH (e:Employee {firstName: 'Джеймс', lastName: 'Томпсон'})-[:Работает_над]->(p:Performance)
        RETURN p.title AS title, p.genre AS genre, p.premiereDate AS premiereDate
        """
        result = self.connection.execute_query(query4)
        table = PrettyTable(["Название", "Жанр", "Дата премьеры"])
        for record in result:
            table.add_row([record["title"], record["genre"], record["premiereDate"]])
        print(table)

        print("\n5. Показы спектакля 'Гамлет':")
        query5 = """
        MATCH (p:Performance {title: 'Гамлет'})-[:Включен_в_показ]->(s:Show)
        RETURN s.date AS date, s.time AS time, s.hall AS hall
        ORDER BY s.date
        """
        result = self.connection.execute_query(query5)
        table = PrettyTable(["Дата", "Время", "Зал"])
        for record in result:
            table.add_row([record["date"], record["time"], record["hall"]])
        print(table)

        print("\n6. Сумма проданных билетов на каждый показ:")
        query6 = """
        MATCH (t:Ticket)-[:Продан_на_показ]->(s:Show)
        RETURN s.date AS date, s.time AS time, s.hall AS hall, SUM(t.price) AS total
        ORDER BY s.date
        """
        result = self.connection.execute_query(query6)
        table = PrettyTable(["Дата", "Время", "Зал", "Общая сумма"])
        for record in result:
            table.add_row([record["date"], record["time"], record["hall"], record["total"]])
        print(table)

        print("\n7. Спектакли без показов:")
        query7 = """
        MATCH (p:Performance)
        WHERE NOT EXISTS {
            MATCH (p)-[:Включен_в_показ]->(:Show)
        }
        RETURN p.title AS title, p.author AS author
        """
        result = self.connection.execute_query(query7)
        table = PrettyTable(["Название", "Автор"])
        for record in result:
            table.add_row([record["title"], record["author"]])
        print(table)

        print("\n8. Актеры с высшими званиями (Commander/CBE):")
        query8 = """
        MATCH (a:Actor)
        WHERE a.rank CONTAINS 'Commander' OR a.rank CONTAINS 'CBE'
        RETURN a.firstName AS firstName, a.lastName AS lastName, a.rank AS rank
        """
        result = self.connection.execute_query(query8)
        table = PrettyTable(["Имя", "Фамилия", "Звание"])
        for record in result:
            table.add_row([record["firstName"], record["lastName"], record["rank"]])
        print(table)

        print("\n9. Показы в Основной сцене:")
        query9 = """
        MATCH (s:Show {hall: 'Основная сцена'})
        RETURN s.date AS date, s.time AS time
        ORDER BY s.date
        """
        result = self.connection.execute_query(query9)
        table = PrettyTable(["Дата", "Время"])
        for record in result:
            table.add_row([record["date"], record["time"]])
        print(table)

        print("\n10. Актеры, отсортированные по количеству наград:")
        query10 = """
        MATCH (a:Actor)
        RETURN a.firstName AS firstName, a.lastName AS lastName, SIZE(a.awards) AS awardsCount
        ORDER BY awardsCount DESC
        """
        result = self.connection.execute_query(query10)
        table = PrettyTable(["Имя", "Фамилия", "Количество наград"])
        for record in result:
            table.add_row([record["firstName"], record["lastName"], record["awardsCount"]])
        print(table)

    def show_all_data(self):
        self.actor_model.show_all()
        self.employee_model.show_all()
        self.performance_model.show_all()
        self.show_model.show_all()
        self.ticket_model.show_all()


def main():
    try:
        # Подключение к Neo4j
        with Neo4jConnection(NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD) as connection:
            # Инициализация сервиса
            theatre_service = TheatreDatabaseService(connection)

            # Выполнение всех запросов
            theatre_service.execute_all_queries()

            # Очистка и заполнение БД
            theatre_service.clear_database()
            theatre_service.create_sample_data()
            theatre_service.create_sample_relationships()

            # Просмотр всех данных
            theatre_service.show_all_data()

    except Exception as e:
        print(f"Произошла ошибка: {e}")

if __name__ == "__main__":
    main()