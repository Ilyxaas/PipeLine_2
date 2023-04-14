from .database import SqliteDB

db = SqliteDB()


class BaseTask:
    """Base Pipeline Task"""

    def run(self):
        raise RuntimeError('Do not run BaseTask!')

    def short_description(self):
        pass

    def __str__(self):
        task_type = self.__class__.__name__
        return f'{task_type}: {self.short_description()}'


class LoadFile(BaseTask):
    """Load file to table"""

    def __init__(self, input_file, table):
        self.table = table
        self.input_file = input_file

    def short_description(self):
        return f'{self.input_file} -> {self.table}'

    def run(self):
        db.load_data_to_table(self.input_file, self.table)

        print(f"Load file `{self.input_file}` to table `{self.table}`")


class CTAS(BaseTask):
    """SQL Create Table As Task"""

    def __init__(self, table, sql_query, title=None):
        self.table = table
        self.sql_query = sql_query
        self.title = title or table

    def short_description(self):
        return f'{self.title}'

    def run(self):
        db.create_table_as_task(self.table, self.sql_query)

        print(f"Create table `{self.table}` as SELECT:\n{self.sql_query}")


class CopyToFile(BaseTask):
    """Copy table data to CSV file"""

    def __init__(self, table, output_file):
        self.table = table
        self.output_file = output_file

    def short_description(self):
        return f'{self.table} -> {self.output_file}'

    def run(self):
        db.copy_data_to_file(self.table, self.output_file)
        print(f"Copy table `{self.table}` to file `{self.output_file}`")


class RunSQL(BaseTask):
    """Run custom SQL query"""

    def __init__(self, sql_query, title=None):
        self.title = title
        self.sql_query = sql_query

    def short_description(self):
        return f'{self.title}'

    def run(self):
        db.run_query(self.sql_query)
        print(f"Run SQL ({self.title}):\n{self.sql_query}")
