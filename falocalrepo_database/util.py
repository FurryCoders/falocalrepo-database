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

# noinspection SpellCheckingInspection
_encodings: list[str] = ["ASCII", "BIG5", "BIG5HKSCS", "CP037", "CP424", "CP437", "CP500", "CP737", "CP775", "CP850",
                         "CP852", "CP855", "CP856", "CP857", "CP860", "CP861", "CP862", "CP863", "CP864", "CP865",
                         "CP866", "CP869", "CP874", "CP875", "CP932", "CP949", "CP950", "CP1006", "CP1026", "CP1140",
                         "CP1250", "CP1251", "CP1252", "CP1253", "CP1254", "CP1255", "CP1256", "CP1257", "CP1258",
                         "EUC-JP", "EUC-JIS-2004", "EUC-JISX0213", "EUC-KR", "GB2312", "GBK", "GB18030", "HZ",
                         "ISO2022-JP", "ISO2022-JP-1", "ISO2022-JP-2", "ISO2022-JP-2004", "ISO2022-JP-3",
                         "ISO2022-JP-EXT", "ISO2022-KR", "LATIN-1", "ISO8859-2", "ISO8859-3", "ISO8859-4", "ISO8859-5",
                         "ISO8859-6", "ISO8859-7", "ISO8859-8", "ISO8859-9", "ISO8859-10", "ISO8859-13", "ISO8859-14",
                         "ISO8859-15", "ISO8859-16", "JOHAB", "KOI8-R", "KOI8-U", "MAC-CYRILLIC", "MAC-GREEK",
                         "MAC-ICELAND", "MAC-LATIN2", "MAC-ROMAN", "MAC-TURKISH", "PTCP154", "SHIFT-JIS",
                         "SHIFT-JIS-2004", "SHIFT-JISX0213", "UTF-32", "UTF-32-BE", "UTF-32-LE", "UTF-16", "UTF-16-BE",
                         "UTF-16-LE", "UTF-7", "UTF-8", "UTF-8-SIG"]


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
    return str(sub(r"[^a-zA-Z\d\-.~]", "", username.lower().strip()))


def check_plain_text(file: bytes) -> bool:
    result: dict = detect_encoding(file[:2048])
    if str(result.get("encoding", "") or "").upper() in _encodings and result.get("confidence", 0) > .9:
        return True
    else:
        return False


def guess_extension(file: bytes | None, default: str = "") -> str:
    if (default := default.lower()) == "jpg":
        default = "jpeg"

    if not file:
        return default
    elif (file_type := filetype_guess_extension(file)) is None:
        return "txt" if check_plain_text(file) else default
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
