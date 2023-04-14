import click

from .load import load_pipeline


@click.group()
def cli():
    # print('Yes')
    pass


@cli.command()
def explore():
    click.echo(f"Explore")


@cli.command()
def list():
    pipeline = load_pipeline()
    pipeline.list()


@cli.command()
def run():
    pipeline = load_pipeline()
    pipeline.run()


def main():
    cli()
