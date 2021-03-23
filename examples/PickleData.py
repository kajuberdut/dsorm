from dsorm.dsorm import Database
from dsorm import ID_COLUMN, Table, TypeMaster, pickle_type_handler

db = Database.memory()

# See: https://docs.python.org/3/library/pickle.html
# As the documentation says: Warning The pickle module is not secure. Only unpickle data you trust.
# Because unpickling data from some sources is unsafe, pickling is not enabled by default
# you must first call:
TypeMaster.allow_pickle()

config_table = Table.from_object(
    table_name="config",
    object={
        "id": ID_COLUMN,
        "user_id": int,
        "config": dict,
    },
)
stmt = config_table.insert(
    data={"user_id": 1, "config": {"setting_1": 1, "setting_2": "red"}},
)
print(stmt.sql())
# INSERT INTO [config] (user_id, config)
# VALUES ('1', x'80049525000000000000007d94288c0973657474696e675f31944b018c0973657474696e675f32948c0372656494752e')
stmt.execute()
print(config_table.select().execute())
# [{'id': 1, 'user_id': 1, 'config': {'setting_1': 1, 'setting_2': 'red'}}]


# Pickling covers list, dict, and tuple by default but you can easily extend it to cover any class.
class CopyCat:
    def __init__(self, cat_says: str):
        self._says = cat_says

    def __repr__(self):
        return f"Cat says: {self._says}"


pickle_type_handler(CopyCat)


cats = Table.from_object(
    table_name="allcats",
    object={"cat_id": ID_COLUMN, "cat": CopyCat},
)
cats.execute()
cats.insert(
    [{"cat": CopyCat("meow")}, {"cat": CopyCat("meowth that's right")}]
).execute()
print(cats.select().execute())
# [{'cat_id': 1, 'cat': Cat says: meow}, {'cat_id': 2, 'cat': Cat says: meowth that's right}]
