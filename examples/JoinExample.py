from dsorm import ID_COLUMN, Database

AUTHOR_NAME = "JK Rowling"
BOOK_NAME = "Harry Potter"

db = Database.from_dict(
    {
        "db_path": ":memory:",
        "tables": {
            "book": {"id": ID_COLUMN, "name": str, "author_id": int},
            "author": {"id": ID_COLUMN, "name": str},
            "publisher": {"id": ID_COLUMN, "name": str},
            "location": {"id": ID_COLUMN, "name": str},
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
            "book_publisher.location_id": "location.id",
            "book_publisher.publisher_id": "publisher.id",
        },
        "data": {
            "author": {"id": 1, "name": AUTHOR_NAME},
            "book": {"id": 1, "name": BOOK_NAME, "author_id": 1},
            "publisher": [
                {"id": 1, "name": "Bloomsbury"},
                {"id": 2, "name": "Scholastic Press"},
            ],
            "location": [{"id": 1, "name": "US"}, {"id": 2, "name": "UK"}],
            "book_publisher": [
                {
                    "location_id": 1,
                    "book_id": 1,
                    "publisher_id": 2,
                },
                {
                    "location_id": 2,
                    "book_id": 1,
                    "publisher_id": 1,
                },
            ],
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

# Join Example
s = (
    book.select(where={book["name"]: BOOK_NAME})
    .join(author, columns=["author.name as author_name"])
    .join(join_table=book_publisher)
    .join(join_table=publisher, on={"book_publisher.publisher_id": "publisher.id"}, columns=["publisher.name as publisher_name"])
    .join(join_table=location, on={"book_publisher.location_id": "location.id"}, columns=["location.name as location_name"])
)


print(s.execute())
# [ {     'id': 1
#       , 'name': 'Harry Potter'
#       , 'author_id': 1
#       , 'author_name': 'JK Rowling'
#       , 'publisher_name': 'Scholastic Press'
#       , 'location_name': 'US'
#   }
#   , {     'id': 1
#       , 'name': 'Harry Potter'
#       , 'author_id': 1
#       , 'author_name': 'JK Rowling'
#       , 'publisher_name': 'Bloomsbury'
#       , 'location_name': 'UK'
#   }
# ]


print(s.sql())
"""
SELECT [book].[id]
     , [book].[name]
     , [book].[author_id]
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
