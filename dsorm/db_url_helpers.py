import re
from pathlib import Path
from urllib.parse import urlparse

from dsorm.dialect import get_sqldialect

URL_PATTERN = re.compile(r"^[a-zA-Z]+\+[\w]+://")


def get_absolute_path(file_path):
    # Expand ~ (tilde) to the home directory
    expanded_path = Path(file_path).expanduser()

    # Get the absolute path
    absolute_path = Path.cwd() / expanded_path

    return absolute_path.resolve()

def db_url_to_dialect(DATABASE_URL):
    dialect = urlparse(DATABASE_URL).scheme.upper()
    return get_sqldialect(dialect)


def normalize_database_url(database):
    # Regular expression pattern for matching database URLs

    # Check if the input matches the URL pattern
    if URL_PATTERN.match(database):
        return database

    # Otherwise, treat the input as a sqlite filepath
    database_path = get_absolute_path(database)
    return "sqlite+aiosqlite://" + str(database_path)


if __name__ == "__main__":
    examples = [
        "sqlite+aiosqlite:///temp.db",
        "postgresql+asyncpg://localhost/example",
        "temp.db",
        "~/temp.db",
    ]

    [print(normalize_database_url(url)) for url in examples]
