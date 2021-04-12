import psycopg2

#in CREATE_MOVIES_TABLE, "SERIAL is added for autoincrement id although Sqlite  automatically does"
CREATE_MOVIES_TABLE = """CREATE TABLE IF NOT EXISTS movies (
    id SERIAL PRIMARY KEY,
    name TEXT,
    release_timestamp REAL
);"""



def create_tables():
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(CREATE_MOVIES_TABLE)


connection = psycopg2.connect(
    host="localhost",
    database="moviedb",
    user="postgres",
    password="1828330")
cursor = connection.cursor()
cursor.execute(CREATE_MOVIES_TABLE)
cursor.execute("INSERT INTO movies (name,release_timestamp) VALUES ('The Matrix','2001')")
connection.commit()
cursor.execute("SELECT * FROM movies")
res = cursor.fetchall()

print(res)
connection.close()
