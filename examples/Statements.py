# This example is not up to date.

from dsorm import Statement

# Statement is a core component of dsORM
# Statements underly all select/insert/delete actions
# You don't need to directly manipulate statements
# But you certainly can

stmt = Statement(db_path=":memory:")

# The components of a statement are ordered by their Statement.Order value
# So you can set them up in any order you like
stmt[Statement.Order.SELECT] = "SELECT i, r"
stmt[Statement.Order.FROM] = "FROM random_data"
stmt[
    Statement.Order.CTE
] = """WITH RECURSIVE random_data AS (
    SELECT 1 AS i, Random() AS r
UNION ALL
    SELECT i+1 as i, Random() as r
    FROM random_data
    WHERE i <= 5
) """

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

print(stmt.execute())
# [{'i': 1, 'r': 1948344763645089057}, {'i': 2, 'r': 6157987235773962892}...
