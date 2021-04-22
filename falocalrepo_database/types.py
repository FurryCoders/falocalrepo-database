from typing import Union

Key = Union[str, int, float]
Value = Union[str, int, float, None]
Entry = dict[str, Union[list[str], Value]]
