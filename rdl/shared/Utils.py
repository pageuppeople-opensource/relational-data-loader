import importlib


def create_type_instance(type_name):
    module = importlib.import_module(type_name)
    class_ = getattr(module, type_name)
    instance = class_()
    return instance
