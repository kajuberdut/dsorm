from dsorm import ID_COLUMN, Database, Table

AUTHOR_NAME = "JK Rowling"
BOOK_NAME = "Harry Potter"

# Table Setup
book = Table.from_dict("book", {"id": ID_COLUMN, "name": str, "author_id": int})
author = Table.from_dict("author", {"id": ID_COLUMN, "name": str})
book.constraints.append(author.fkey(book["author_id"]))
Database(db_path=":memory:", is_default=True).init_db()

# Data Setup
author.insert({"name": AUTHOR_NAME}).execute()
jk_id = author.select(where={"name": AUTHOR_NAME}).execute()[0]["id"]
book.insert({"name": BOOK_NAME, "author_id": jk_id}).execute()

# Join Example
s = book.select(where={book["name"]: BOOK_NAME})
s["JOIN"] = "JOIN author ON book.author_id = author.id"
s.add_column("author.name as author_name")

print(s.execute())
