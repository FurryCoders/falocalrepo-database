from typing import Optional
from typing import Union

from .exceptions import UnknownSelector
from .types import Value

Selector = dict[str, Union[dict[str, Union[Value, list[Value]]], 'Selector', list['Selector']]]
SELECTOR_NOT = NOT = "$not"
SELECTOR_AND = AND = "$and"
SELECTOR_OR = OR = "$or"
SELECTOR_EQ = EQ = "$eq"
SELECTOR_NE = NE = "$ne"
SELECTOR_GT = GT = "$gt"
SELECTOR_LT = LT = "$lt"
SELECTOR_GE = GE = "$ge"
SELECTOR_LE = LE = "$le"
SELECTOR_IN = IN = "$in"
SELECTOR_INSTR = INSTR = "$instr"
SELECTOR_BETWEEN = BETWEEN = "$between"
SELECTOR_LIKE = LIKE = "$like"
SELECTOR_GLOB = GLOB = "$glob"


def flatten(list_old: list) -> list:
    list_new = []
    for i in list_old:
        list_new.extend(flatten(i)) if isinstance(i, list) else list_new.append(i)
    return list_new


def selector_to_sql(selector: Selector) -> tuple[str, list[Value]]:
    sql, values = "", []
    assert isinstance(selector, dict), "selector needs to be of type dict"
    for key, value in selector.items():
        if key in (AND, OR):
            assert isinstance(value, list) and all(isinstance(v, dict) for v in value)
        elif key in (NOT, EQ, NE, GT, LT, GE, LE, IN, INSTR, BETWEEN, LIKE, GLOB):
            assert isinstance(value, dict)
        else:
            raise UnknownSelector(key)

        if key == SELECTOR_NOT:
            sql_, values_ = selector_to_sql(value)
            sql = f"not ({sql})"
            values.extend(flatten(values_))
        if key == SELECTOR_AND:
            sql_values = list(map(selector_to_sql, value))
            sql = f"({' and '.join([s for s, _ in sql_values])})"
            values.extend(flatten([v for _, v in sql_values]))
        elif key == SELECTOR_OR:
            sql_values = list(map(selector_to_sql, value))
            sql = f"({' or '.join([s for s, _ in sql_values])})"
            values.extend(flatten([v for _, v in sql_values]))
        elif key == SELECTOR_EQ:
            sql = f"{(k := [*value.keys()][0])} = ?"
            values.append(value[k])
        elif key == SELECTOR_NE:
            sql = f"{(k := [*value.keys()][0])} != ?"
            values.append(value[k])
        elif key == SELECTOR_GT:
            sql = f"{(k := [*value.keys()][0])} > ?"
            values.append(value[k])
        elif key == SELECTOR_LT:
            sql = f"{(k := [*value.keys()][0])} < ?"
            values.append(value[k])
        elif key == SELECTOR_GE:
            sql = f"{(k := [*value.keys()][0])} >= ?"
            values.append(value[k])
        elif key == SELECTOR_LE:
            sql = f"{(k := [*value.keys()][0])} <= ?"
            values.append(value[k])
        elif key == SELECTOR_IN:
            sql = f"{(k := [*value.keys()][0])} in " + \
                  f"({','.join(['?'] * len(vs if isinstance(vs := value[k], list) else [vs]))})"
            values.extend(vs)
        elif key == SELECTOR_INSTR:
            sql = f"instr({(k := [*value.keys()][0])}, ?)"
            values.append(value[k])
        elif key == SELECTOR_BETWEEN:
            assert isinstance((v := value[(k := [*value.keys()][0])]), list)
            sql = f"{k} between ? and ?"
            values.extend(v[0:2])
        elif key in (SELECTOR_LIKE, SELECTOR_GLOB):
            assert isinstance((v := value[(k := [*value.keys()][0])]), str)
            sql = f"{k} like ?" if key == SELECTOR_LIKE else f"{k} glob ?"
            values.append(v)

    return sql, values


class SelectorBuilder:
    def __init__(self, field: str = None):
        self.field: Optional[str] = field

    def __sub__(self, other: Selector) -> Selector:  # NOT
        return {SELECTOR_NOT: other}

    def __and__(self, other: list[Selector]) -> Selector:  # AND
        return {SELECTOR_AND: other}

    def __or__(self, other: list[Selector]) -> Selector:  # OR
        return {SELECTOR_OR: other}

    def __eq__(self, value: Value) -> Selector:  # EQ
        return {SELECTOR_EQ: {self.field: value}}

    def __ne__(self, value: Value) -> Selector:  # NE
        return {SELECTOR_NE: {self.field: value}}

    def __gt__(self, value: Value) -> Selector:  # GT
        return {SELECTOR_GT: {self.field: value}}

    def __lt__(self, value: Value) -> Selector:  # LT
        return {SELECTOR_LT: {self.field: value}}

    def __ge__(self, value: Value) -> Selector:  # GE
        return {SELECTOR_GE: {self.field: value}}

    def __le__(self, value: Value) -> Selector:  # LE
        return {SELECTOR_LE: {self.field: value}}

    def __truediv__(self, value: Value) -> Selector:  # IN
        return {SELECTOR_IN: {self.field: value}}

    def __floordiv__(self, value: Value) -> Selector:  # INSTR
        return {SELECTOR_INSTR: {self.field: value}}

    def __xor__(self, values: list[Value]) -> Selector:  # BETWEEN
        return {SELECTOR_BETWEEN: {self.field: values[:2]}}

    def __mod__(self, value: Value) -> Selector:  # LIKE
        return {SELECTOR_LIKE: {self.field: value}}

    def __mul__(self, value: Value) -> Selector:  # GLOB
        return {SELECTOR_GLOB: {self.field: value}}
