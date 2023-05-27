import asyncio
import tempfile
from pathlib import Path

import databases
from dsorm import dbclass

import sqlite3

# +-------------------------------------------------+
# |                PRE EXAMPLE SETUP                |
# +-------------------------------------------------+

# Due to a quirk of databases, :memory: is not easily used.
temp_file = tempfile.NamedTemporaryFile(delete=True)
db_path = temp_file.name

try:
    database = databases.Database(f"sqlite+aiosqlite:///{db_path}")
    asyncio.run(database.connect())


    # +-------------------------------------------------+
    # |                EXAMPLE STARTS HERE               |
    # +-------------------------------------------------+


    # The db class decorator takes at minimum an instance of databases.Database
    @dbclass(db=database)
    class User:
        _db = database


    # data is added as an init keyword
    new_user = User(
        data={
            "first_name": "John",
            "last_name": "Doe",
            "email": "example@gmail.com",
        }
    )
    asyncio.run(new_user.save())

    # load the same user from the db
    loaded_user = asyncio.run(User.load(new_user.id))

    print(loaded_user.id)
    print(f"{loaded_user.first_name} {loaded_user.last_name}")
    print(loaded_user.email)


    # +-------------------------------------------------+
    # |                POST EXAMPLE CLEANUP             |
    # +-------------------------------------------------+

    asyncio.run(database.disconnect())

    for file in temp_path.glob("*"):
        file.unlink()
    temp_path.rmdir()