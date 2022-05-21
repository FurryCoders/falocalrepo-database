from datetime import datetime
from enum import Enum
from enum import EnumMeta

from .column import Column
from .column import parse_list
from .util import clean_username

__all__ = [
    "users_table",
    "submissions_table",
    "journals_table",
    "comments_table",
    "settings_table",
    "history_table",
    "UsersColumns",
    "SubmissionsColumns",
    "JournalsColumns",
    "CommentsColumns",
    "SettingsColumns",
    "HistoryColumns",
]

users_table: str = "USERS"
submissions_table: str = "SUBMISSIONS"
journals_table: str = "JOURNALS"
comments_table: str = "COMMENTS"
settings_table: str = "SETTINGS"
history_table: str = "HISTORY"


class ColumnsEnum(Enum):
    @classmethod
    def as_list(cls: EnumMeta) -> list[Column]:
        return [c.value for c in cls]


class UsersColumns(ColumnsEnum):
    USERNAME = Column("USERNAME", str, unique=True, key=True, check="length({name}) > 0", to_entry=clean_username)
    FOLDERS = Column("FOLDERS", set)
    ACTIVE = Column("ACTIVE", bool)
    USERPAGE = Column("USERPAGE", str, to_entry=str.strip)


class SubmissionsColumns(ColumnsEnum):
    ID = Column("ID", int, unique=True, key=True, check="{name} > 0")
    AUTHOR = Column("AUTHOR", str, check="length({name}) > 0")
    TITLE = Column("TITLE", str)
    DATE = Column("DATE", datetime)
    DESCRIPTION = Column("DESCRIPTION", str)
    TAGS = Column("TAGS", list)
    CATEGORY = Column("CATEGORY", str)
    SPECIES = Column("SPECIES", str)
    GENDER = Column("GENDER", str)
    RATING = Column("RATING", str)
    TYPE = Column("TYPE", str, to_entry=str.lower, check="{name} in ('image', 'music', 'text', 'flash')")
    FILEURL = Column("FILEURL", list[str], from_entry=lambda v: parse_list(v))
    FILEEXT = Column("FILEEXT", list[str], from_entry=lambda v: parse_list(v))
    FILESAVED = Column("FILESAVED", int, check="{name} in (0, 1, 2, 3, 4, 5, 6, 7)")
    FAVORITE = Column("FAVORITE", set)
    MENTIONS = Column("MENTIONS", set)
    FOLDER = Column("FOLDER", str, to_entry=str.lower, check="{name} in ('gallery', 'scraps')")
    USERUPDATE = Column("USERUPDATE", bool)


class JournalsColumns(ColumnsEnum):
    ID = Column("ID", int, unique=True, key=True, check="{name} > 0")
    AUTHOR = Column("AUTHOR", str, check="length({name}) > 0")
    TITLE = Column("TITLE", str)
    DATE = Column("DATE", datetime)
    CONTENT = Column("CONTENT", str)
    MENTIONS = Column("MENTIONS", set)
    USERUPDATE = Column("USERUPDATE", bool)


class CommentsColumns(ColumnsEnum):
    ID = Column("ID", int, key=True, check="{name} > 0")
    PARENT_TABLE = Column("PARENT_TABLE", str, key=True, check=f"{'{name}'} in ('{submissions_table}', '{journals_table}')")
    PARENT_ID = Column("PARENT_ID", int, key=True, check="{name} > 0")
    REPLY_TO = Column("REPLY_TO", int, not_null=False, check="{name} == null or {name} > 0")
    AUTHOR = Column("AUTHOR", str, check="length({name}) > 0")
    DATE = Column("DATE", datetime, to_entry=lambda v: v.strftime("%Y-%m-%dT%H:%M:%S"),
                  from_entry=lambda v: datetime.strptime(v, "%Y-%m-%dT%H:%M:%S"))
    TEXT = Column("TEXT", str)


class SettingsColumns(ColumnsEnum):
    SETTING = Column("SETTING", str, unique=True, key=True, check="length({name}) > 0")
    SVALUE = Column("SVALUE", str, not_null=False, check="{name} == null or length({name}) > 0")


class HistoryColumns(ColumnsEnum):
    TIME = Column("TIME", datetime, unique=True, key=True, to_entry=lambda v: v.strftime("%Y-%m-%dT%H:%M:%S.%f"),
                  from_entry=lambda v: datetime.strptime(v, "%Y-%m-%dT%H:%M:%S.%f"))
    EVENT = Column("EVENT", str)
