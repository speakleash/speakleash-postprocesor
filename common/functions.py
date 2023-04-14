import datetime
from rich import print as rich_print

def log(message, level="INFO"):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rich_print(f"[white]{timestamp}[/white] [red]{level}[/red] > [green]{message}[/green]")
