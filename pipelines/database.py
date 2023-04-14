import sqlite3
from sqlite3 import Error
from .utils import print_error
import sys
import csv
from urllib.parse import urlparse

def domain_of_url(url):
    return urlparse(url).netloc

class SqliteDB:
    def __init__(self):
        try:
            self.connection = sqlite3.connect(r'pipelines.db', isolation_level=None)
            self.cursor = self.connection.cursor()
        except (Exception, Error) as error:
            print_error(f"Ошибка sqlite3: {error}")
            sys.exit(1)

    def run_query(self, query):
        try:
            self.cursor.execute(query)
        except (Exception, Error) as error:
            print_error(f"Ошибка sqlite3: {error}")
            sys.exit(1)

    def load_data_to_table(self, input_file, table):
        with open(input_file, 'r') as infile:
            reader = csv.reader(infile)
            read = []

            for row in reader:
                read.append(row)

            fieldnames = read[0]
            values = read[1:]

        str_fields = ""
        for i, f in enumerate(fieldnames):
            str_fields += f + " "
            if values[0][i].isdigit():
                str_fields += "INT PRIMARY KEY "
            else:
                str_fields += "TEXT NOT NULL "

            if i < len(fieldnames) - 1:
                str_fields += ", "

        vals = []
        for v in values:
            vals.append(tuple(v))


        self.run_query(f"create table if not exists {table} ({str_fields});")


        self.cursor.executemany(
            f'''insert into {table}({','.join(fieldnames)}) 
                        VALUES({','.join('?' * len(fieldnames))});''',
            vals)

    def create_table_as_task(self, table, query):
        self.connection.create_function("domain_of_url", 1, domain_of_url)
        self.run_query("create table if not exists " + table + " AS " + query)


    def copy_data_to_file(self, table, output_file):
        self.run_query(f"select * from {table};")

        row = self.cursor.fetchone()
        field_names = list(row)

        res = self.cursor.fetchall()
        with open(output_file, "w") as f:
            writer = csv.writer(f)
            writer.writerow(field_names)
            writer.writerows(res)

    def close_connection(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()

