from datetime import datetime
from json import dumps
from json import loads
from types import GenericAlias
from typing import Any
from typing import Callable
from typing import Optional
from typing import Protocol
from typing import Type
from typing import TypeVar
from typing import Union
from typing import get_args
from typing import get_origin

from .types import Value

T = TypeVar("T")
NoDefault = TypeVar("NoDefault")

date_format: str = "%Y-%m-%dT%H:%M"


def format_list(obj: list[Value], *, sort: bool = False) -> str:
    return "".join(f"|{e}|" for e in (sorted(obj) if sort else obj))


def parse_list(obj: str) -> list[str]:
    return [e for e in obj.removeprefix("|").removesuffix("|").split("||") if e]


def type_to_sql(t: type) -> str:
    t_: type = get_origin(t) if type(t) is GenericAlias else t

    if t_ is Any:
        return "text"
    elif t_ is int:
        return "integer"
    elif t_ is float:
        return "real"
    elif t_ is str:
        return "text"
    elif t_ is bool:
        return "boolean"
    elif t_ is datetime:
        return "datetime"
    elif t_ in (list, tuple, set, dict):
        return "text"
    else:
        raise TypeError(t, "not allowed")


def sql_to_type(t: str | Type[Any]) -> Type[T]:
    if t is Any:
        return str
    elif (t := t.lower()) in ("integer", "int"):
        return int
    elif t == "real":
        return float
    elif t == "boolean":
        return bool
    elif t in ("date", "datetime"):
        return datetime
    elif t == "text":
        return str
    else:
        raise TypeError(f"unknown SQLite type {t!r}")


def default_formatter(t: Type[T]) -> Callable[[T], Value]:
    t_: type = get_origin(t) if type(t) is GenericAlias else t

    if t_ in (Any, int, float, str, bool):
        return lambda v: v
    elif t_ is datetime:
        return lambda v: v.strftime(date_format)
    elif t_ in (list, tuple):
        return format_list
    elif t_ is set:
        return lambda v: format_list(v, sort=True)
    elif t_ is dict:
        return dumps
    else:
        raise TypeError(t, "not allowed")


def default_parser(t: Type[T]) -> Callable[[Value], T]:
    sub_type: Optional[type] = None
    t_: type = t

    if type(t) is GenericAlias:
        t_ = get_origin(t)
        sub_type = get_args(t)[0]

    if t_ is Any:
        return lambda v: v
    elif t_ in (int, float, str, bool):
        return t_
    elif t_ is datetime:
        return lambda v: datetime.strptime(v, date_format)
    elif t_ in (list, tuple, set):
        return (lambda v: t_(map(sub_type, parse_list(v)))) if sub_type else (lambda v: t_(parse_list(v)))
    elif t_ is dict:
        return loads
    else:
        raise TypeError(t, "not allowed")


class _Column(Protocol[T]):
    name: str
    type: Type[T]
    not_null: bool
    unique: bool
    key: bool
    _check: str
    to_entry: Callable[[T], Value]
    from_entry: Callable[[Value], T]
    default: Union[T, None, Type[NoDefault]]


class Column(_Column):
    def __init__(self, name: str, type_: Type[T] | str, sql_type: str = None, not_null: bool = True,
                 unique: bool = False, key: bool = False, check: str = None, default: T = NoDefault,
                 to_entry: Callable[[T], Value] = None, from_entry: Callable[[Value], T] = None):
        self.name: str = name
        self.type: Type[T] = type_ if isinstance(type_, type) else sql_to_type(type_)
        self._sql_type: str | None = sql_type
        self.not_null: bool = not_null
        self.unique: bool = unique
        self.key: bool = key
        self._check: str = check
        self.to_entry: Callable[[T], Value] = to_entry if to_entry is not None else default_formatter(self.type)
        self.from_entry: Callable[[Value], T] = from_entry if from_entry is not None else default_parser(self.type)
        self.default: Union[T, None, Type[NoDefault]] = default

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name!r}, {self.type})"

    @property
    def check(self) -> str:
        return self._check.format(name=self.name) if self._check else ""

    @property
    def sql_type(self) -> str:
        return self._sql_type or type_to_sql(self.type)

    def create_statement(self) -> str:
        elements: list[str] = [self.name, self.sql_type]
        if self.unique:
            elements.append("unique")
        if self.not_null:
            elements.append("not null")
        if self.check:
            elements.append(f"check ({self.check})")

        return " ".join(elements)
