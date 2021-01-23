from dsorm import Where

example = Where(where={"": Where(where={"thing": "stuff", "OR": Where(where={1: 2})})})
print(example.sql())
