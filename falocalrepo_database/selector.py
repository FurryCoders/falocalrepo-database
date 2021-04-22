from re import sub
from typing import Union

from .exceptions import UnknownSelector
from .types import Value


class ValuePlaceholder:
    def __str__(self):
        return "?"


Selector = dict[str, Union[dict[str, Union[Value, ValuePlaceholder]], list['Selector']]]
SELECTOR_AND = AND = "$and"
SELECTOR_OR = OR = "$or"
SELECTOR_EQ = EQ = "$eq"
SELECTOR_NE = NE = "$ne"
SELECTOR_GT = GT = "$gt"
SELECTOR_LT = LT = "$lt"
SELECTOR_GE = GE = "$ge"
SELECTOR_LE = LE = "$le"
SELECTOR_IN = IN = "$in"
SELECTOR_BETWEEN = BETWEEN = "$between"
SELECTOR_LIKE = LIKE = "$like"
SELECTOR_GLOB = GLOB = "$glob"


def sql_value(value: Value) -> str:
    if value is ValuePlaceholder or isinstance(value, ValuePlaceholder):
        return str(ValuePlaceholder())
    elif value is None:
        return "null"
    elif isinstance(value, str):
        value = sub(r"(?<!\\)'", "\\'", value)
        return f"'{value}'"
    else:
        return str(value)


def selector_to_sql(selector: Selector) -> str:
    sql: str = ""
    assert isinstance(selector, dict), "selector needs to be of type dict"
    for key, value in selector.items():
        if key in (AND, OR):
            assert isinstance(value, list) and all(isinstance(v, dict) for v in value)
        elif key in (EQ, NE, GT, LT, GE, LE, IN, BETWEEN, LIKE, GLOB):
            assert isinstance(value, dict)
        else:
            raise UnknownSelector(key)

        if key == SELECTOR_AND:
            sql = " and ".join(map(lambda s: f"({selector_to_sql(s)})", value))
        elif key == SELECTOR_OR:
            sql = " or ".join(map(lambda s: f"({selector_to_sql(s)})", value))
        elif key == SELECTOR_EQ:
            sql = f"{(k := [*value.keys()][0])} = {sql_value(value[k])}"
        elif key == SELECTOR_NE:
            sql = f"{(k := [*value.keys()][0])} != {sql_value(value[k])}"
        elif key == SELECTOR_GT:
            sql = f"{(k := [*value.keys()][0])} > {sql_value(value[k])}"
        elif key == SELECTOR_LT:
            sql = f"{(k := [*value.keys()][0])} < {sql_value(value[k])}"
        elif key == SELECTOR_GE:
            sql = f"{(k := [*value.keys()][0])} >= {sql_value(value[k])}"
        elif key == SELECTOR_LE:
            sql = f"{(k := [*value.keys()][0])} <= {sql_value(value[k])}"
        elif key == SELECTOR_IN:
            sql = f"{(k := [*value.keys()][0])} in " + \
                  f"({','.join(map(sql_value, v if isinstance(v := value[k], list) else [v]))})"
        elif key == SELECTOR_BETWEEN:
            assert isinstance((v := value[(k := [*value.keys()][0])]), list)
            sql = f"{k} between {v[0]} and {v[1]}"
        elif key in (SELECTOR_LIKE, SELECTOR_GLOB):
            assert isinstance((v := value[(k := [*value.keys()][0])]), (str, ValuePlaceholder)) or v is ValuePlaceholder
            sql = f"{k} like {sql_value(v)}" if key == SELECTOR_LIKE else f"{k} glob {sql_value(v)}"
    return sql
