from dsorm import Statement, Database

# Statement is a core component of dsORM
# Statements underly all select/insert/delete actions
# You don't need to directly manipulate statements
# But you certainly can

stmt = Statement()

# The components of a statement are ordered by their Statement.Order value
# So you can set them up in any order you like
# The generic "Statement" only has 3 orders, BEGINNING, STATEMENT, and AFTER
# Here is a little bit of a contrived example showing composing those order

# Again, this is a contrived example
# We can directly assign components using their Order values
# Order is a subclass of Enum so (#) or ["name"] both work
cte = Statement(
    components={
        Statement.Order(1): "WITH RECURSIVE random_data AS (",
        Statement.Order(3): ")",
    }
)

# Also Statements have a __setitem__ to make setting components easy
# Here we set the middle order item of cte to another statement
# This is a recursive CTE
cte[2] = Statement(
    components={
        Statement.Order.BEGINNING: "SELECT 1 AS i, Random() AS r",
        Statement.Order.MIDDLE: "UNION ALL",
        Statement.Order.END: """SELECT i+1 as i, Random() as r
    FROM random_data
    WHERE i <= 5
""",
    }
)

# The process order these are assigned in of course doesn't matter
# So we'll write our select from the CTE
stmt = Statement()
stmt["MIDDLE"] = "SELECT i, r \n FROM random_data"

# And then attach our cte from above
stmt["BEGINNING"] = cte

# Finally, to show that, insane as it is, this works:
print(stmt.sql())
# WITH RECURSIVE random_data AS (
#     SELECT 1 AS i, Random() AS r
# UNION ALL
#     SELECT i+1 as i, Random() as r
#     FROM random_data
#     WHERE i <= 5
# )
# SELECT i, r
# FROM random_data

print(Database(db_path=":memory:").execute(stmt))
# [{'i': 1, 'r': 1948344763645089057}, {'i': 2, 'r': 6157987235773962892}...
