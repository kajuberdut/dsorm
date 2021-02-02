from dsorm import Statement, Database

# Statement is a core component of dsORM
# Statements underly all select/insert/delete actions
# You don't need to directly manipulate statements
# But you certainly can

stmt = Statement()

# The components of a statement are ordered by their Statement.Order value
# So you can set them up in any order you like
# The generic "Statement" only has 3 orders, BEFORE, STATEMENT, and AFTER
# Here is a little bit of a contrived example showing composing those order
stmt["STATEMENT"] = "SELECT i, r"
stmt["AFTER"] = "FROM random_data"
stmt["BEFORE"] = Statement(
    components={
        Statement.Order.BEFORE: "WITH RECURSIVE random_data AS (",
        Statement.Order.STATEMENT: """SELECT 1 AS i, Random() AS r
UNION ALL
    SELECT i+1 as i, Random() as r
    FROM random_data
    WHERE i <= 5
""",
        Statement.Order.AFTER: ")",
    }
)

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
