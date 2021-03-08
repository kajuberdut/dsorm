from dsorm import ID_COLUMN, Database, Join

AUTHOR_NAME = "JK Rowling"
BOOK_NAME = "Harry Potter"

db = Database.from_dict({
    "db_path": ":memory:",
    "tables": {
        "book": {"id": ID_COLUMN, "name": str, "author_id": int},
        "author": {"id": ID_COLUMN, "name": str},
    },
    "constraints": {"book.author_id": "author.id"},
    "data": {
        "author": {"id": 1, "name": AUTHOR_NAME},
        "book": {"name": BOOK_NAME, "author_id": 1},
    },
})

# Join Example
s = db.table("book").select(where={db.table("book")["name"]: BOOK_NAME})
s["JOIN"] = Join(
    table=db.table("author"), on={db.table("book")["author_id"]: db.table("author")["id"]}
)
s.add_column("author.name as author_name")

print(s.execute())
# [{'id': 1, 'name': 'Harry Potter', 'author_id': 1, 'author_name': 'JK Rowling'}]

print(s.sql())
"""
SELECT [book].[id], [book].[name], [book].[author_id], [author].[name] AS author_name
FROM  [book]
JOIN  [author] ON [book].[author_id] = [author].[id]
WHERE [book].[name] = 'Harry Potter'
"""
