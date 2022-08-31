from datetime import datetime

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


class UsersColumns:
    USERNAME = Column("USERNAME", str, unique=True, key=True, check="length({name}) > 0", to_entry=clean_username)
    FOLDERS = Column("FOLDERS", set)
    ACTIVE = Column("ACTIVE", bool)
    USERPAGE = Column("USERPAGE", str, to_entry=str.strip)

    @classmethod
    def as_list(cls) -> list[Column]:
        return [
            cls.USERNAME,
            cls.FOLDERS,
            cls.ACTIVE,
            cls.USERPAGE,
        ]


class SubmissionsColumns:
    ID = Column("ID", int, unique=True, key=True, check="{name} > 0")
    AUTHOR = Column("AUTHOR", str, check="length({name}) > 0")
    TITLE = Column("TITLE", str)
    DATE = Column("DATE", datetime)
    DESCRIPTION = Column("DESCRIPTION", str)
    FOOTER = Column("FOOTER", str)
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

    @classmethod
    def as_list(cls) -> list[Column]:
        return [
            cls.ID,
            cls.AUTHOR,
            cls.TITLE,
            cls.DATE,
            cls.DESCRIPTION,
            cls.FOOTER,
            cls.TAGS,
            cls.CATEGORY,
            cls.SPECIES,
            cls.GENDER,
            cls.RATING,
            cls.TYPE,
            cls.FILEURL,
            cls.FILEEXT,
            cls.FILESAVED,
            cls.FAVORITE,
            cls.MENTIONS,
            cls.FOLDER,
            cls.USERUPDATE,
        ]


class JournalsColumns:
    ID = Column("ID", int, unique=True, key=True, check="{name} > 0")
    AUTHOR = Column("AUTHOR", str, check="length({name}) > 0")
    TITLE = Column("TITLE", str)
    DATE = Column("DATE", datetime)
    CONTENT = Column("CONTENT", str)
    HEADER = Column("HEADER", str)
    FOOTER = Column("FOOTER", str)
    MENTIONS = Column("MENTIONS", set)
    USERUPDATE = Column("USERUPDATE", bool)

    @classmethod
    def as_list(cls) -> list[Column]:
        return [
            cls.ID,
            cls.AUTHOR,
            cls.TITLE,
            cls.DATE,
            cls.CONTENT,
            cls.HEADER,
            cls.FOOTER,
            cls.MENTIONS,
            cls.USERUPDATE,
        ]


class CommentsColumns:
    ID = Column("ID", int, key=True, check="{name} > 0")
    PARENT_TABLE = Column("PARENT_TABLE", str, key=True,
                          check=f"{'{name}'} in ('{submissions_table}', '{journals_table}')")
    PARENT_ID = Column("PARENT_ID", int, key=True, check="{name} > 0")
    REPLY_TO = Column("REPLY_TO", int, not_null=False, check="{name} == null or {name} > 0")
    AUTHOR = Column("AUTHOR", str, check="length({name}) > 0")
    DATE = Column("DATE", datetime, to_entry=lambda v: v.strftime("%Y-%m-%dT%H:%M:%S"),
                  from_entry=lambda v: datetime.strptime(v, "%Y-%m-%dT%H:%M:%S"))
    TEXT = Column("TEXT", str)

    @classmethod
    def as_list(cls) -> list[Column]:
        return [
            cls.ID,
            cls.PARENT_TABLE,
            cls.PARENT_ID,
            cls.REPLY_TO,
            cls.AUTHOR,
            cls.DATE,
            cls.TEXT,
        ]


class SettingsColumns:
    SETTING = Column("SETTING", str, unique=True, key=True, check="length({name}) > 0")
    SVALUE = Column("SVALUE", str, not_null=False, check="{name} == null or length({name}) > 0")

    @classmethod
    def as_list(cls) -> list[Column]:
        return [
            cls.SETTING,
            cls.SVALUE,
        ]


class HistoryColumns:
    TIME = Column("TIME", datetime, unique=True, key=True, to_entry=lambda v: v.strftime("%Y-%m-%dT%H:%M:%S.%f"),
                  from_entry=lambda v: datetime.strptime(v, "%Y-%m-%dT%H:%M:%S.%f"))
    EVENT = Column("EVENT", str)

    @classmethod
    def as_list(cls) -> list[Column]:
        return [
            cls.TIME,
            cls.EVENT,
        ]
