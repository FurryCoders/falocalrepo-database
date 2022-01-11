from pathlib import Path
from re import match
from re import split
from re import sub

from chardet import detect as detect_encoding
from filetype import guess_extension as filetype_guess_extension
from psutil import AccessDenied
from psutil import NoSuchProcess
from psutil import Process
from psutil import process_iter

from .__version__ import __version__
from .exceptions import MultipleConnections
from .exceptions import VersionError

__all__ = [
    "compare_version",
    "find_connections",
    "clean_username",
    "guess_extension",
    "tiered_path",
    "format_value",
    "query_to_sql",
]


def compare_version(version_a: str, *, major: bool = True, minor: bool = True, patch: bool = True,
                    version_b: str = __version__) -> VersionError | None:
    if not version_a:
        return VersionError("version missing")
    elif version_a == version_b:
        return None
    elif (v_a := version_a.split("."))[0] != (v_b := version_b.split("."))[0]:
        return VersionError(f"major version is not latest: {version_a} != {version_b}") if major else None
    elif v_a[1] != v_b[1]:
        return VersionError(f"minor version is not latest: {version_a} != {version_b}") if minor else None
    elif v_a[2] != v_b[2]:
        return VersionError(f"patch version is not latest: {version_a} != {version_b}") if patch else None


def find_connections(path: Path, raise_for_limit: bool = False, limit: int = 0) -> list:
    ps: list[Process] = []
    path_: str = str(path.resolve())
    for process in process_iter():
        try:
            if process.is_running() and any(path_ == f.path for f in process.open_files()):
                ps.append(process)
            if len(ps) > limit and raise_for_limit:
                raise MultipleConnections(f"Multiple connections to database: {ps}")
        except (NoSuchProcess, AccessDenied):
            pass
    return ps


def clean_username(username: str) -> str:
    return str(sub(r"[^a-zA-Z0-9\-.~]", "", username.lower().strip()))


def guess_extension(file: bytes | None, default: str = "") -> str:
    if (default := default.lower()) == "jpg":
        default = "jpeg"

    if not file:
        return default
    elif (file_type := filetype_guess_extension(file)) is None:
        return default or ("" if detect_encoding(file[:2048]).get("encoding", None) is not None else "txt")
    elif (ext := str(file_type)) in (exts := ("zip", "octet-stream")):
        return default if default not in exts else ext
    else:
        return ext


def tiered_path(id_: int | str, depth: int = 5, width: int = 2) -> Path:
    assert isinstance(id_, int) or (isinstance(id_, str) and id_.isdigit()), "id not an integer"
    assert isinstance(depth, int) and depth > 0, "depth must be greater than 0"
    assert isinstance(width, int) and width > 0, "depth must be greater than 0"

    id_str: str = str(int(id_)).zfill(depth * width)
    return Path(*[id_str[n:n + width] for n in range(0, depth * width, width)])


def format_value(value: str, *, like: bool = False) -> str:
    value = sub(r"(?<!\\)((?:\\\\)+)?([%_^$])", r"\1\\\2", m.group(1)) if (m := match(r'^"(.*)"$', value)) else value
    value = value.lstrip("^") if match(r"^[%^].*", value) else "%" + value if like else value
    value = value.rstrip("$") if match(r".*(?<!\\)((?:\\\\)+)?[%$]$", value) else value + "%" if like else value
    return value


def query_to_sql(query: str, default_field: str, likes: list[str] = None, aliases: dict[str, str] = None
                 ) -> tuple[list[str], list[str]]:
    if not query:
        return [], []

    likes, aliases = likes or [], aliases or {}
    elements: list[str] = []
    values: list[str] = []

    query = sub(r"(^[&| ]+|((?<!\\)[&|]| )+$)", "", query)
    query = sub(r"( *[&|])+(?= *[&|] *[@()])", "", query)

    field, prev = default_field, ""
    for elem in filter(bool, map(str.strip, split(r'((?<!\\)(?:"|!")(?:[^"]|(?<=\\)")*"|(?<!\\)[()&|]| +)', query))):
        if m := match(r"^@(\w+)$", elem):
            field = m.group(1).lower()
            continue
        elif elem == "&":
            elements.append("and")
        elif elem == "|":
            elements.append("or")
        elif elem in ("(", ")"):
            elements.append("and") if elem == "(" and prev not in ("", "&", "|", "(") else None
            elements.append(elem)
        elif elem:
            not_, elem = match(r"^(!)?(.*)$", elem).groups()
            if not elem:
                continue
            elements.append("and") if prev not in ("", "&", "|", "(") else None
            elements.append(f"({aliases.get(field, field)}{' not' * bool(not_)} like ? escape '\\')")
            values.append(format_value(elem, like=field in likes))
        prev = elem

    return elements, values
