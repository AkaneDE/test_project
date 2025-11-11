
"""
Запуск:
    python app.py 1              # создать таблицу
    python app.py 2 "Ivanov Petr Sergeevich" 2009-07-12 Male # вставить одну запись
    python app.py 3              # вывести все уникальные строки
    python app.py 4              # автоматически заполнить 1000000 записей и 100 спец "F" мужских
    python app.py 5              # выборка: male и фамилия начинается на 'F' + время
    python app.py 6              # оптимизация (создание индексов)
"""

import os
import sys
import time
import random
import datetime
from dataclasses import dataclass
from typing import List, Tuple

import psycopg2
from psycopg2.extras import execute_values

# Конфигурация подключения читает переменные окружения:
PGHOST = os.environ.get("PGHOST", "localhost")
PGPORT = os.environ.get("PGPORT", "5433")
PGDATABASE = os.environ.get("PGDATABASE", "employees_db")
PGUSER = os.environ.get("PGUSER", "postgres")
PGPASSWORD = os.environ.get("PGPASSWORD", "")

DSN = f"host={PGHOST} port={PGPORT} dbname={PGDATABASE} user={PGUSER} password={PGPASSWORD}"

BATCH_SIZE = 10000  # размер пакета для массовой вставки


def get_conn():
    conn = psycopg2.connect(DSN)
    # Установка кодировки клиент-сессии в UTF8
    conn.set_client_encoding("UTF8")
    return conn


@dataclass
class Employee:
    surname: str
    given_name: str
    patronymic: str
    date_of_birth: datetime.date
    gender: str
    # Расчёт возраста
    def age(self, today: datetime.date = None) -> int:
        if today is None:
            today = datetime.date.today()
        born = self.date_of_birth
        years = today.year - born.year
        if (today.month, today.day) < (born.month, born.day):
            years -= 1
        return years

    def to_tuple(self) -> Tuple:
        return (self.surname, self.given_name, self.patronymic, self.date_of_birth, self.gender)

    # Вставка одной записи в таблицу
    def save(self, conn):
        sql = """
        INSERT INTO employees (surname, given_name, patronymic, date_of_birth, gender)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (surname, given_name, patronymic, date_of_birth) DO NOTHING
        """
        with conn.cursor() as cur:
            cur.execute(sql, self.to_tuple())
        conn.commit()

    #Пакетно вставляет список объектов employees.
    @classmethod
    def bulk_insert(cls, conn, employees: List['Employee'], batch_size: int = BATCH_SIZE):
        sql = """
        INSERT INTO employees (surname, given_name, patronymic, date_of_birth, gender)
        VALUES %s
        ON CONFLICT (surname, given_name, patronymic, date_of_birth) DO NOTHING
        """
        tuples = [e.to_tuple() for e in employees]
        with conn.cursor() as cur:
            for i in range(0, len(tuples), batch_size):
                batch = tuples[i:i + batch_size]
                execute_values(cur, sql, batch, page_size=batch_size)
            conn.commit()


# Разбить ФИО
def parse_fullname(fullname: str) -> Tuple[str, str, str]:
    parts = fullname.strip().split()
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    elif len(parts) == 2:
        # если только фамилия + имя — пустой отчество
        return parts[0], parts[1], ""
    elif len(parts) > 3:
        # считаем первые три части как фамилия, имя, все остальное как отчество
        return parts[0], parts[1], " ".join(parts[2:])
    else:
        raise ValueError("Fullname must contain at least surname and given name.")

# Парсинг даты
def parse_date(datestr: str) -> datetime.date:
    return datetime.datetime.strptime(datestr, "%Y-%m-%d").date()


# Режимы
# Создание таблицы
def mode_create_table():
    sql = """
    CREATE TABLE IF NOT EXISTS employees (
        id SERIAL PRIMARY KEY,
        surname TEXT NOT NULL,
        given_name TEXT NOT NULL,
        patronymic TEXT NOT NULL,
        date_of_birth DATE NOT NULL,
        gender TEXT NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
        UNIQUE (surname, given_name, patronymic, date_of_birth)
    );
    """
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()
    conn.close()
    print("Таблица 'employees' создана (или уже существует).")

# Вставка одной записи
def mode_insert_single(fullname: str, dob_str: str, gender: str):
    surname, given, patronymic = parse_fullname(fullname)
    dob = parse_date(dob_str)
    emp = Employee(surname, given, patronymic, dob, gender)
    conn = get_conn()
    emp.save(conn)
    conn.close()
    print(f"Вставлено: {emp.surname} {emp.given_name} {emp.patronymic} {emp.date_of_birth} {emp.gender}")

# Вывод всех значений, отсортированных по возрастанию по ФИО
def mode_list_all():
    conn = get_conn()
    sql = """
    SELECT surname, given_name, patronymic, date_of_birth, gender
    FROM employees
    GROUP BY surname, given_name, patronymic, date_of_birth, gender
    ORDER BY surname, given_name, patronymic;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    conn.close()
    today = datetime.date.today()
    for r in rows:
        surname, given, patronymic, dob, gender = r
        emp = Employee(surname, given, patronymic, dob, gender)
        print(f"{surname} {given} {patronymic}\t{dob}\t{gender}\t{emp.age(today)}")
    print(f"Всего: {len(rows)}")


# Функция для генерации случайных ФИО
SURNAMES_BY_LETTER = {}
FIRST_NAMES = ["Ivan", "Petr", "Alex", "John", "Michael", "David", "Robert", "William", "James", "Thomas",
               "Andrey", "Nikolay", "Sergey", "Vladimir", "Roman", "Igor"]
PATRONYMICS = ["Ivanovich", "Petrovich", "Sergeevich", "Alexandrovich", "Mikhailovich", "Dmitrievich",
               "Vladimirovich", "Nikolaevich", "Romanovich", "Igorevich"]

LETTERS = [chr(i) for i in range(ord('A'), ord('Z') + 1)]


def prepare_surnames():
    global SURNAMES_BY_LETTER
    for L in LETTERS:
        # генерируем 100 фамилий на букву L
        arr = [f"{L}surname{n}" for n in range(1, 201)]
        SURNAMES_BY_LETTER[L] = arr


def gen_random_employee(letter: str = None, gender: str = None) -> Employee:
    if not SURNAMES_BY_LETTER:
        prepare_surnames()
    if letter is None:
        letter = random.choice(LETTERS)
    surname = random.choice(SURNAMES_BY_LETTER[letter])
    given = random.choice(FIRST_NAMES)
    patronymic = random.choice(PATRONYMICS)
    age_years = random.randint(18, 70)
    today = datetime.date.today()
    start_year = today.year - age_years
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    dob = datetime.date(start_year, month, day)
    if gender is None:
        gender = random.choice(["Male", "Female"])
    return Employee(surname, given, patronymic, dob, gender)


def mode_bulk_generate(total: int = 1_000_000):
    """
    Генерация total записей:
      - распределение пола ~ равномерно
      - распределение начальной буквы фамилии — равномерно
    Также ДОБАВЛЯЕТ 100 записей: мужских с фамилией, начинающейся на "F".
    """
    print(f"Массовая генерация: всего={total} (плюс 100 специальных записей с мужскими фамилиями на 'F')")
    prepare_surnames()
    conn = get_conn()

    batch = []
    letters_cycle = LETTERS.copy()
    for i in range(total):
        letter = letters_cycle[i % len(letters_cycle)]
        # alternate gender for balance
        gender = "Male" if (i % 2 == 0) else "Female"
        emp = gen_random_employee(letter=letter, gender=gender)
        batch.append(emp)
        if len(batch) >= BATCH_SIZE:
            Employee.bulk_insert(conn, batch, batch_size=BATCH_SIZE)
            print(f"Вставлено {i+1} / {total}")
            batch.clear()
    if batch:
        Employee.bulk_insert(conn, batch, batch_size=BATCH_SIZE)
        print(f"Вставлено {total} ")

    # 100 специальных записей: male и фамилия начинается с "F"
    special = []
    for i in range(100):
        surname = f"F_special_{i+1}"
        given = random.choice(FIRST_NAMES)
        patronymic = random.choice(PATRONYMICS)
        year = random.randint(datetime.date.today().year - 60, datetime.date.today().year - 18)
        month = random.randint(1, 12)
        day = random.randint(1, 28)
        dob = datetime.date(year, month, day)
        special.append(Employee(surname, given, patronymic, dob, "Male"))
    Employee.bulk_insert(conn, special, batch_size=BATCH_SIZE)
    conn.close()
    print("Генерация завершена")


def mode_select_male_F_measure():
    """
    Пункт 5: результат выборки по критерию: пол мужской, фамилия начинается с "F".
    Вывод результатов и время выполнения запроса.
    """
    conn = get_conn()
    sql = """
    SELECT surname, given_name, patronymic, date_of_birth, gender
    FROM employees
    WHERE gender = %s AND surname LIKE %s
    ORDER BY surname;
    """
    params = ("Male", "F%")
    with conn.cursor() as cur:
        t0 = time.perf_counter()
        cur.execute(sql, params)
        rows = cur.fetchall()
        t1 = time.perf_counter()
    elapsed = t1 - t0
    conn.close()
    print(f"Запрос вернул {len(rows)} строк. Затрачено: {elapsed:.6f} секунд.")
    # Вывод части результатов
    for r in rows[:20]:
        surname, given, patronymic, dob, gender = r
        emp = Employee(surname, given, patronymic, dob, gender)
        print(f"{surname} {given} {patronymic}\t{dob}\t{emp.age()}")
    return elapsed


def mode_optimize_create_indexes():
    """
    Пункт 6: создаёт индексы для ускорения запроса пункта 5.
    """
    conn = get_conn()
    sqls = [
        "CREATE INDEX IF NOT EXISTS idx_gender_surname ON employees (gender, surname);",
        "CREATE INDEX IF NOT EXISTS idx_gender_lower_surname ON employees (gender, lower(surname));",
    ]
    with conn.cursor() as cur:
        for s in sqls:
            try:
                cur.execute(s)
            except Exception as e:
                print("Ошибка оператора создания индекса (игнорируется):", e)
        conn.commit()
    conn.close()
    print("Индексы созданы.")


# -------------------------
# CLI
# -------------------------
def print_usage():
    print("Режимы:")
    print("  python app.py 1")
    print('  python app.py 2 "Ivanov Petr Sergeevich" 2009-07-12 Male')
    print("  python app.py 3")
    print("  python app.py 4")
    print("  python app.py 5")
    print("  python app.py 6")


def main(argv):
    if len(argv) < 2:
        print_usage()
        return
    mode = argv[1]
    if mode == "1":
        mode_create_table()
    elif mode == "2":
        if len(argv) < 5:
            print("Режим 2 требует ФИО, дату (YYYY-MM-DD) и пол")
            print_usage()
            return
        fullname = argv[2]
        dob = argv[3]
        gender = argv[4]
        mode_insert_single(fullname, dob, gender)
    elif mode == "3":
        mode_list_all()
    elif mode == "4":
        mode_bulk_generate(total=1_000_000)
    elif mode == "5":
        elapsed = mode_select_male_F_measure()
        print(f"Время выполнения (в секундах): {elapsed:.6f}")
    elif mode == "6":
        mode_optimize_create_indexes()
        print("Оптимизация выполнена. Запустите 5 режим снова, чтобы сравнить результаты.")
    else:
        print("Неизвестный режим.")
        print_usage()


if __name__ == "__main__":
    main(sys.argv)
