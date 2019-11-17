import psycopg2 as psycopg2


class StudentsArchive:
    def __init__(self):
        self.connection = psycopg2.connect(
            dbname="adpy8_08_db",
            user="a_vinogradov",
            password="pass",
            host="localhost",
            port="5433"
        )

    def create_db(self):
        """
        создает таблицы
        """
        with self.connection as conn:
            with conn.cursor() as curs:
                curs.execute("""CREATE TABLE IF NOT EXISTS student(
                    id serial PRIMARY KEY,
                    name varchar(100),
                    gpa numeric (10,2),
                    birth timestamp with time zone);
                    """)
                curs.execute("""CREATE TABLE IF NOT EXISTS course(
                    id serial PRIMARY KEY,
                    name varchar(100))
                    """)
                curs.execute("""CREATE TABLE IF NOT EXISTS student_course(
                    id serial PRIMARY KEY,
                    student_id INTEGER REFERENCES student(id),
                    course_id INTEGER REFERENCES course(id));
                    """)
        print('Таблицы созданы успешно\n')

    def clear_db(self):
        """
        Удаление таблиц базы для исправления грубых ошибок
        """
        with self.connection as conn:
            with conn.cursor() as curs:
                curs.execute(
                    "DROP TABLE student, course, student_course")

    def add_course(self, course: str):
        self.course = course
        if not self.course:
            self.course = input("Введите название курса: ")
        with self.connection as conn:
            with conn.cursor() as curs:
                curs.execute("insert into course (name) values (%s)",
                             (self.course,))

    def add_courses(self, courses: list):
        self.courses = courses
        with self.connection as conn:
            with conn.cursor() as curs:
                for course in self.courses:
                    curs.execute(
                        "insert into course (name) values (%s)",
                        (course,))

    def add_students(self, course_id: int, students: dict):
        """
        создает студентов и записывает их на курс
        """
        self.course_id = course_id
        self.students = students
        with self.connection as conn:
            with conn.cursor() as curs:
                for student in self.students:
                    name = self.students.get(student)['name']
                    gpa = self.students.get(student)['gpa']
                    birth = self.students.get(student)['birth']
                    curs.execute("insert into student (name, gpa, "
                                 "birth) values (%s, %s, %s)",
                                 (name, gpa, birth))
                    curs.execute("""select student,id from student 
                                where student.name = (%s)""",
                                 (name,))
                    id_info = curs.fetchall()
                    for row in id_info:
                        curs.execute("""insert into student_course 
                                    (student_id, course_id) values (%s, %s)
                                    """, (row[1], self.course_id))

    def add_student(self, students: dict):  # просто создает студента
        self.students = students
        with self.connection as conn:
            with conn.cursor() as curs:
                for student in self.students:
                    name = self.students.get(student)['name']
                    gpa = self.students.get(student)['gpa']
                    birth = self.students.get(student)['birth']
                    print()
                    curs.execute("insert into student (name, gpa, "
                                 "birth) values (%s, %s, %s)",
                                 (name, gpa, birth))

    def link_students_courses(self, course_id: int, students: list):
        """
        создает студентов и записывает их на курс
        """
        self.course_id = course_id
        self.students = students
        with self.connection as conn:
            with conn.cursor() as curs:
                for student_id in self.students:
                    curs.execute("""insert into student_course 
                                    (student_id, course_id) values (%s, %s)
                                    """,
                                 (student_id, self.course_id))

    def get_student(self, student_id: int):
        """
        Отображает информацию о студенте по его student_id

        :param student_id: int, числовой идентификатор записи студента
        """
        self.student_id = student_id
        print(f"\nСтуденческая карточка №{student_id}:\n")
        with self.connection as conn:
            with conn.cursor() as curs:
                curs.execute(
                    "select * from student where student.id = (%s)",
                    (self.student_id,))
                request = curs.fetchall()
                for row in request:
                    print("Имя:", row[1])
                    print("Средний балл:", row[2])
                    print("Дата рождения:", row[3])

    def get_students(self, course_id):
        """
        возвращает студентов определенного курса
        """
        self.course_id = course_id
        with self.connection as conn:
            with conn.cursor() as curs:
                curs.execute("select * from course where id = (%s)",
                             (self.course_id,))
                course_data = curs.fetchall()
                for row in course_data:
                    print(f"\nСтуденты на курсе «{row[1]}»:")
                curs.execute("""select s.id, s.name, c.name from student_course sc
                    join student s on s.id = sc.student_id
                    join course c on c.id = sc.course_id
                    where sc.course_id = (%s)
                            """, (self.course_id,))
                student_records = curs.fetchall()
                for row in student_records:
                    print(f"- {row[1]}, студкарточка № {row[0]}")

    def inspect_student_db(self):
        """
        Печать в консоль всей информации из таблицы student
        """
        with self.connection as conn:
            with conn.cursor() as curs:
                curs.execute("select * from student")
                student_records = curs.fetchall()
                for row in student_records:
                    print("Id = ", row[0])
                    print("Name = ", row[1])
                    print("Gpa  = ", row[2])
                    print("Birth = ", row[3])

    def inspect_courses_db(self):
        """
        Печать в консоль всей информации из таблицы course
        """
        with self.connection as conn:
            with conn.cursor() as curs:
                curs.execute("select * from course")
                courses_records = curs.fetchall()
                for row in courses_records:
                    print("Id = ", row[0])
                    print("Name = ", row[1])

    def inspect_student_course_db(self):
        """
        Печать в консоль всей информации из таблицы student_course
        """
        with self.connection as conn:
            with conn.cursor() as curs:
                curs.execute("select * from student_course")
                s_c_records = curs.fetchall()
                for row in s_c_records:
                    print("Id = ", row[0])
                    print("student_id = ", row[1])
                    print("course_id = ", row[2])


if __name__ == '__main__':
    # Переменные для тестов

    students = {
        'Дмитрий Петров': {'name': 'Дмитрий Петров',
                           'gpa': 4.7,
                           'birth': '1982-04-21'},
        'Вячеслав Иванов': {'name': 'Вячеслав Иванов',
                            'gpa': 4.2,
                            'birth': '1986-04-11'},
        'Артур Карандашов': {'name': 'Артур Карандашов',
                             'gpa': 4.3,
                             'birth': '1989-07-01'},
    }

    students_2 = {
        'Василий Ушкуйник': {'name': 'Василий Ушкуйник',
                             'gpa': 4.4,
                             'birth': '1981-01-12'},
        'Федор Печатников': {'name': 'Федор Печатников',
                             'gpa': 4.1,
                             'birth': '1983-10-07'},

    }

    courses_list = ["Современный змеиный язык",
                    "Пандовая математика",
                    "Конспектирование Юпитера", "Словарное дело",
                    "Сопромозг"]

    web_school = StudentsArchive()

    # Функция полной очистки базы
    # web_school.clear_db()

    # Создаём базу с заданными таблицами
    web_school.create_db()

    # Добавляем информацию по студентам и
    # web_school.add_student(students)
    # web_school.add_courses(courses_list)
    # web_school.add_course("Основы таблицеведения")
    # web_school.add_students(6, students_2)

    # записываем студентов на курсы
    # web_school.link_students_courses(1, [1, 2, 3])
    # web_school.link_students_courses(2, [2, 3])
    # web_school.link_students_courses(3, [1, 3])
    # web_school.link_students_courses(4, [1])

    # вывод содержимого таблиц в консоль
    print("\nСодержимое таблицы student:")
    web_school.inspect_student_db()
    print("\nСодержимое таблицы course:")
    web_school.inspect_courses_db()
    print("\nСодержимое таблицы student_course:")
    web_school.inspect_student_course_db()

    # звпрашиваем информацию по конкретным студентам
    web_school.get_student(1)
    web_school.get_student(3)

    # информация по всем студентам на курсе
    web_school.get_students(1)
    web_school.get_students(2)
    web_school.get_students(6)
