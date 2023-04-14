from pipelines import tasks, Pipeline


NAME = 'test_project'
VERSION = '2023'


TASKS = [
    tasks.LoadFile(input_file='example_pipeline/data/original.csv', table='original'),
    tasks.CTAS(
        table='norm',
        sql_query='''
            select *, domain_of_url(url)
            from original
        '''
    ),
    tasks.CopyToFile(
        table='norm',
        output_file='example_pipeline/data/norm.csv',
    ),

    # clean up:
    tasks.RunSQL('drop table original;', "Удаление исходной таблицы"),
    tasks.RunSQL('drop table norm;', "Удаление полученной таблицы"),
]


pipeline = Pipeline(
    name=NAME,
    version=VERSION,
    tasks=TASKS
)


if __name__ == "__main__":
    # 1: Run as script
    pipeline.run()

    # 2: Run as CLI
    # > pipelines run
