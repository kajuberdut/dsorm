from enum import Enum

from dsorm import ID_COLUMN, Database

AUTHOR_NAME = "JK Rowling"
BOOK_NAME = "Harry Potter"


class Location(Enum):
    US = 1
    UK = 2


db = Database.from_dict(
    {
        "db_path": ":memory:",
        "tables": {
            "location": Location,
            "book": {"id": ID_COLUMN, "name": str, "author_id": int},
            "author": {
                "refdata": {"name": AUTHOR_NAME},
            },
            "publisher": {
                "refdata": [
                    {"name": "Bloomsbury"},
                    {"name": "Scholastic Press"},
                ],
            },
            "book_publisher": {
                "id": ID_COLUMN,
                "location_id": int,
                "book_id": int,
                "publisher_id": int,
            },
        },
        "constraints": {
            "book.author_id": "author.id",
            "book_publisher.book_id": "book.id",
            "book_publisher.location_id": "location.value",
            "book_publisher.publisher_id": "publisher.id",
        },
    }
)


book, author, book_publisher, publisher, location = (
    db.table("book"),
    db.table("author"),
    db.table("book_publisher"),
    db.table("publisher"),
    db.table("location"),
)

book.insert(
    data={"name": BOOK_NAME, "author_id": db.id("author", "name", AUTHOR_NAME)}
).execute()
book_id = db.id("book", "name", BOOK_NAME)

book_publisher.insert(
    data=[
        {
            "location_id": Location.UK,
            "book_id": book_id,
            "publisher_id": db.id("publisher", "name", "Bloomsbury"),
        },
        {
            "location_id": Location.US,
            "book_id": book_id,
            "publisher_id": db.id("publisher", "name", "Scholastic Press"),
        },
    ]
).execute()

# Join Example
s = (
    book.select(where={book["name"]: BOOK_NAME}, column=["book.name as book_name"])
    .join(author, columns=["author.name as author_name"])
    .join(join_table=book_publisher)
    .join(
        join_table=publisher,
        on={"book_publisher.publisher_id": "publisher.id"},
        columns=["publisher.name as publisher_name"],
    )
    .join(
        join_table=location,
        on={"book_publisher.location_id": "location.id"},
        columns=["location.name as location_name"],
    )
)


print(s.execute())
# [ {     'book_name': 'Harry Potter'
#       , 'author_name': 'JK Rowling'
#       , 'publisher_name': 'Scholastic Press'
#       , 'location_name': 'US'
#   }
#   , {   'book_name': 'Harry Potter'
#       , 'author_name': 'JK Rowling'
#       , 'publisher_name': 'Bloomsbury'
#       , 'location_name': 'UK'
#   }
# ]


print(s.sql())
"""
SELECT [book].[name] as book_name
     , [author].[name] AS author_name
     , [publisher].[name] AS publisher_name
     , [location].[name] AS location_name
FROM [book]
JOIN [author] ON [book].[author_id] = [author].[id]
JOIN [book_publisher] ON [book_publisher].[book_id] = [book].[id]
JOIN [publisher] ON [book_publisher].[publisher_id] = [publisher].[id]
JOIN [location] ON [book_publisher].[location_id] = [location].[id]
WHERE [book].[name] = 'Harry Potter'
"""
