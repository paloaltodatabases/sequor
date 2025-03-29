from collections import namedtuple

VariableEntry = namedtuple("VariableEntry", ["type", "value"])

class VariableBindings:
    def __init__(self):
        self._bindings = {}

    def set(self, name, value, var_type="text"):
        self._bindings[name] = VariableEntry(type=var_type, value=value)

    def get(self, name):
        return self._bindings[name].value

    def get_type(self, name):
        return self._bindings[name].type
