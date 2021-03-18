from dsorm import ID_COLUMN, Database, Table, TypeMaster, PickleHandler

# See: https://docs.python.org/3/library/pickle.html
# As the documentation says: Warning The pickle module is not secure. Only unpickle data you trust.
# Because unpickling data from some sources is unsafe, pickling is not enabled by default
# you must first call:
TypeMaster.allow_pickle()

config = Table.from_object(
    table_name="config",
    object={
        "id": ID_COLUMN,
        "user_id": int,
        "config": dict,
    },
)
print(config.sql())
# CREATE TABLE IF NOT EXISTS config (id INTEGER PRIMARY KEY, user_id INTEGER, config BLOB);

Database(db_path=":memory:", is_default=True).init_db()
stmt = config.insert(
    data={"user_id": "1", "config": {"setting_1": 1, "setting_2": "red"}},
)
print(stmt.sql())
# INSERT INTO [config] (user_id, config)
# VALUES ('1', x'80049525000000000000007d94288c0973657474696e675f31944b018c0973657474696e675f32948c0372656494752e')
stmt.execute()
print(config.select().execute())
# [{'id': 1, 'user_id': 1, 'config': {'setting_1': 1, 'setting_2': 'red'}}]


# Pickling covers list, dict, and tuple by default but you can easily extend it to cover any class.
class CopyCat:
    def __init__(self, cat_says: str):
        self._says = cat_says

    def __repr__(self):
        return f"Cat says: {self._says}"


class PickledCat(PickleHandler):
    python_type = CopyCat


PickledCat.register()


cats = Table.from_object(
    table_name="allcats",
    object={"cat_id": {"python_type": int, "pkey": True}, "cat": CopyCat},
)
cats.execute()
cats.insert([{"cat": CopyCat("meow")}, {"cat": CopyCat("meowth that's right")}]).execute()
print(cats.select().execute())
# [{'cat_id': 1, 'cat': Cat says: meow}, {'cat_id': 2, 'cat': Cat says: meowth that's right}]
