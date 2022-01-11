from datetime import datetime
from enum import Enum
from enum import EnumMeta

from .column import Column
from .util import clean_username

__all__ = [
    "users_table",
    "submissions_table",
    "journals_table",
    "settings_table",
    "history_table",
    "UsersColumns",
    "SubmissionsColumns",
    "JournalsColumns",
    "SettingsColumns",
    "HistoryColumns",
]

users_table: str = "USERS"
submissions_table: str = "SUBMISSIONS"
journals_table: str = "JOURNALS"
settings_table: str = "SETTINGS"
history_table: str = "HISTORY"


class ColumnsEnum(Enum):
    @classmethod
    def as_list(cls: EnumMeta) -> list[Column]:
        return [c.value for c in cls]


class UsersColumns(ColumnsEnum):
    USERNAME = Column("USERNAME", str, unique=True, key=True, check="length({name}) > 0", to_entry=clean_username)
    FOLDERS = Column("FOLDERS", set)
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
    FILEURL = Column("FILEURL", str)
    FILEEXT = Column("FILEEXT", str)
    FILESAVED = Column("FILESAVED", int, check="{name} in (0, 1, 2, 3)")
    FAVORITE = Column("FAVORITE", set)
    MENTIONS = Column("MENTIONS", set)
    FOLDER = Column("FOLDER", str, to_entry=str.lower, check="{name} in ('gallery', 'scraps')")
    USERUPDATE = Column("USERUPDATE", str, check="{name} in (0, 1)")


class JournalsColumns(ColumnsEnum):
    ID = Column("ID", int, unique=True, key=True, check="ID > 0")
    AUTHOR = Column("AUTHOR", str, check="length({name}) > 0")
    TITLE = Column("TITLE", str)
    DATE = Column("DATE", datetime)
    CONTENT = Column("CONTENT", str)
    MENTIONS = Column("MENTIONS", set)
    USERUPDATE = Column("USERUPDATE", int, check="{name} in (0, 1)")


class SettingsColumns(ColumnsEnum):
    SETTING = Column("SETTING", str, unique=True, key=True, check="length({name}) > 0")
    SVALUE = Column("SVALUE", str, not_null=False, check="{name} == null or length({name}) > 0")


class HistoryColumns(ColumnsEnum):
    TIME = Column("TIME", datetime, unique=True, key=True, to_entry=lambda v: v.strftime("%Y-%m-%dT%H:%M:%S.%f"),
                  from_entry=lambda v: datetime.strptime(v, "%Y-%m-%dT%H:%M:%S.%f"))
    EVENT = Column("EVENT", str)
