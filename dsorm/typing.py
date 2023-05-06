class DBClassMeta(type):
    def __instancecheck__(self, instance):
        return hasattr(instance, "_is_db_class")


class DBClass(metaclass=DBClassMeta):
    pass
