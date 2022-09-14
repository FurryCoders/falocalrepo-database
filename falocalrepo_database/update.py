from datetime import datetime
from json import loads
from operator import itemgetter
from pathlib import Path
from re import compile as re_compile
from re import Pattern
from re import sub
from sqlite3 import connect
from sqlite3 import Connection
from sqlite3 import DatabaseError
from sqlite3 import OperationalError
from typing import Callable
from typing import Collection
from typing import Optional
from typing import Union

__all__ = [
    "compare_versions",
    "update_database",
]


# noinspection SqlResolve,SqlNoDataSourceInspection
def get_version(conn: Connection) -> str:
    try:
        # Database version 3.0.0 and above
        return conn.execute("SELECT SVALUE FROM SETTINGS WHERE SETTING = 'VERSION'").fetchone()[0]
    except OperationalError:
        return ""


def compare_versions(a: str, b: str) -> int:
    a_split = list(map(int, a.split("-", 1)[0].split(".")))
    b_split = list(map(int, b.split("-", 1)[0].split(".")))
    a_split.extend([0] * (3 - len(a_split)))
    b_split.extend([0] * (3 - len(b_split)))

    for a_, b_ in zip(a_split, b_split):
        if a_ > b_:
            return 1
        elif a_ < b_:
            return -1
    return 0


def insert(conn: Connection, table: str, keys: Collection[str], values: Collection[Union[int, str]],
           replace: bool = True):
    conn.execute(
        f"""INSERT OR {"REPLACE" if replace else "IGNORE"} INTO {table}
        ({",".join(keys)})
        VALUES ({",".join(["?"] * len(values))})""",
        values
    )


# noinspection SqlNoDataSourceInspection
def count(conn: Connection, table: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchall()[0][0]


def database_path(conn: Connection) -> Path | None:
    name: str
    filename: Optional[str]
    for _, name, filename in conn.execute("PRAGMA database_list"):
        if name == "main" and filename is not None:
            return Path(filename)

    return None


# noinspection SqlResolve,SqlNoDataSourceInspection,DuplicatedCode
def make_database_5(conn: Connection) -> Connection:
    conn.execute("""create table USERS
    (USERNAME text unique not null check (length(USERNAME) > 0),
    FOLDERS text not null,
    USERPAGE text not null,
    primary key (USERNAME));""")

    conn.execute("""create table SUBMISSIONS
    (ID integer unique not null check (ID > 0),
    AUTHOR text not null check (length(AUTHOR) > 0),
    TITLE text not null,
    DATE datetime not null,
    DESCRIPTION text not null,
    TAGS text not null,
    CATEGORY text not null,
    SPECIES text not null,
    GENDER text not null,
    RATING text not null,
    TYPE text not null check (TYPE in ('image', 'music', 'text', 'flash')),
    FILEURL text not null,
    FILEEXT text not null,
    FILESAVED integer not null check (FILESAVED in (0, 1, 2, 3)),
    FAVORITE text not null,
    MENTIONS text not null,
    FOLDER text not null check (FOLDER in ('gallery', 'scraps')),
    USERUPDATE text not null check (USERUPDATE in (0, 1)),
    primary key (ID));""")

    conn.execute("""create table JOURNALS
    (ID integer unique not null check (ID > 0),
    AUTHOR text not null check (length(AUTHOR) > 0),
    TITLE text not null,
    DATE datetime not null,
    CONTENT text not null,
    MENTIONS text not null,
    USERUPDATE integer not null check (USERUPDATE in (0, 1)),
    primary key (ID))""")

    conn.execute("""create table SETTINGS
    (SETTING text unique not null check (length(SETTING) > 0),
    SVALUE text check (SVALUE == null or length(SVALUE) > 0),
    primary key (SETTING));""")

    conn.execute("insert or ignore into SETTINGS (SETTING, SVALUE) values (?, ?)", ["FILESFOLDER", "FA.files"])
    conn.execute("insert or ignore into SETTINGS (SETTING, SVALUE) values (?, ?)", ["VERSION", "5.0.0"])

    conn.execute("""create table HISTORY
    (TIME datetime unique not null,
    EVENT text not null,
    primary key (TIME));""")

    conn.commit()

    return conn


# noinspection SqlResolve,SqlNoDataSourceInspection,DuplicatedCode
def make_database_5_1(conn: Connection) -> Connection:
    conn.execute("""create table USERS
    (USERNAME text unique not null check (length(USERNAME) > 0),
    FOLDERS text not null,
    ACTIVE boolean not null,
    USERPAGE text not null,
    primary key (USERNAME));""")

    conn.execute("""create table SUBMISSIONS
    (ID integer unique not null check (ID > 0),
    AUTHOR text not null check (length(AUTHOR) > 0),
    TITLE text not null,
    DATE datetime not null,
    DESCRIPTION text not null,
    TAGS text not null,
    CATEGORY text not null,
    SPECIES text not null,
    GENDER text not null,
    RATING text not null,
    TYPE text not null check (TYPE in ('image', 'music', 'text', 'flash')),
    FILEURL text not null,
    FILEEXT text not null,
    FILESAVED integer not null check (FILESAVED in (0, 1, 2, 3)),
    FAVORITE text not null,
    MENTIONS text not null,
    FOLDER text not null check (FOLDER in ('gallery', 'scraps')),
    USERUPDATE boolean not null,
    primary key (ID));""")

    conn.execute("""create table JOURNALS
    (ID integer unique not null check (ID > 0),
    AUTHOR text not null check (length(AUTHOR) > 0),
    TITLE text not null,
    DATE datetime not null,
    CONTENT text not null,
    MENTIONS text not null,
    USERUPDATE integer not null check (USERUPDATE in (0, 1)),
    primary key (ID))""")

    conn.execute("""create table SETTINGS
    (SETTING text unique not null check (length(SETTING) > 0),
    SVALUE text check (SVALUE == null or length(SVALUE) > 0),
    primary key (SETTING));""")

    conn.execute("insert or ignore into SETTINGS (SETTING, SVALUE) values (?, ?)", ["FILESFOLDER", "FA.files"])
    conn.execute("insert or ignore into SETTINGS (SETTING, SVALUE) values (?, ?)", ["VERSION", "5.1.0"])

    conn.execute("""create table HISTORY
    (TIME datetime unique not null,
    EVENT text not null,
    primary key (TIME));""")

    conn.commit()

    return conn


# noinspection SqlResolve,SqlNoDataSourceInspection,DuplicatedCode
def make_database_5_2(conn: Connection) -> Connection:
    conn.execute("""create table USERS
    (USERNAME text unique not null check (length(USERNAME) > 0),
    FOLDERS text not null,
    ACTIVE boolean not null,
    USERPAGE text not null,
    primary key (USERNAME));""")

    conn.execute("""create table SUBMISSIONS
    (ID integer unique not null check (ID > 0),
    AUTHOR text not null check (length(AUTHOR) > 0),
    TITLE text not null,
    DATE datetime not null,
    DESCRIPTION text not null,
    TAGS text not null,
    CATEGORY text not null,
    SPECIES text not null,
    GENDER text not null,
    RATING text not null,
    TYPE text not null check (TYPE in ('image', 'music', 'text', 'flash')),
    FILEURL text not null,
    FILEEXT text not null,
    FILESAVED integer not null check (FILESAVED in (0, 1, 2, 3)),
    FAVORITE text not null,
    MENTIONS text not null,
    FOLDER text not null check (FOLDER in ('gallery', 'scraps')),
    USERUPDATE boolean not null,
    primary key (ID));""")

    conn.execute("""create table JOURNALS
    (ID integer unique not null check (ID > 0),
    AUTHOR text not null check (length(AUTHOR) > 0),
    TITLE text not null,
    DATE datetime not null,
    CONTENT text not null,
    MENTIONS text not null,
    USERUPDATE integer not null check (USERUPDATE in (0, 1)),
    primary key (ID))""")

    conn.execute("""create table COMMENTS
    (ID integer unique not null check (ID > 0),
    PARENT_TABLE text not null check (PARENT_TABLE in ('SUBMISSIONS', 'JOURNALS')),
    PARENT_ID integer not null check (PARENT_ID > 0),
    REPLY_TO integer check (REPLY_TO == null or REPLY_TO > 0),
    AUTHOR text not null check (length(AUTHOR) > 0),
    DATE datetime not null,
    TEXT text not null,
    primary key (ID))""")

    conn.execute("""create table SETTINGS
    (SETTING text unique not null check (length(SETTING) > 0),
    SVALUE text check (SVALUE == null or length(SVALUE) > 0),
    primary key (SETTING));""")

    conn.execute("insert or ignore into SETTINGS (SETTING, SVALUE) values (?, ?)", ["FILESFOLDER", "FA.files"])
    conn.execute("insert or ignore into SETTINGS (SETTING, SVALUE) values (?, ?)", ["VERSION", "5.2.0"])

    conn.execute("""create table HISTORY
    (TIME datetime unique not null,
    EVENT text not null,
    primary key (TIME));""")

    conn.commit()

    return conn


# noinspection SqlResolve,SqlNoDataSourceInspection,DuplicatedCode
def make_database_5_2_2(conn: Connection) -> Connection:
    conn.execute("""create table USERS
    (USERNAME text unique not null check (length(USERNAME) > 0),
    FOLDERS text not null,
    ACTIVE boolean not null,
    USERPAGE text not null,
    primary key (USERNAME));""")

    conn.execute("""create table SUBMISSIONS
    (ID integer unique not null check (ID > 0),
    AUTHOR text not null check (length(AUTHOR) > 0),
    TITLE text not null,
    DATE datetime not null,
    DESCRIPTION text not null,
    TAGS text not null,
    CATEGORY text not null,
    SPECIES text not null,
    GENDER text not null,
    RATING text not null,
    TYPE text not null check (TYPE in ('image', 'music', 'text', 'flash')),
    FILEURL text not null,
    FILEEXT text not null,
    FILESAVED integer not null check (FILESAVED in (0, 1, 2, 3)),
    FAVORITE text not null,
    MENTIONS text not null,
    FOLDER text not null check (FOLDER in ('gallery', 'scraps')),
    USERUPDATE boolean not null,
    primary key (ID));""")

    conn.execute("""create table JOURNALS
    (ID integer unique not null check (ID > 0),
    AUTHOR text not null check (length(AUTHOR) > 0),
    TITLE text not null,
    DATE datetime not null,
    CONTENT text not null,
    MENTIONS text not null,
    USERUPDATE integer not null check (USERUPDATE in (0, 1)),
    primary key (ID))""")

    conn.execute("""create table COMMENTS
    (ID integer not null check (ID > 0),
    PARENT_TABLE text not null check (PARENT_TABLE in ('SUBMISSIONS', 'JOURNALS')),
    PARENT_ID integer not null check (PARENT_ID > 0),
    REPLY_TO integer check (REPLY_TO == null or REPLY_TO > 0),
    AUTHOR text not null check (length(AUTHOR) > 0),
    DATE datetime not null,
    TEXT text not null,
    primary key (ID, PARENT_TABLE, PARENT_ID))""")

    conn.execute("""create table SETTINGS
    (SETTING text unique not null check (length(SETTING) > 0),
    SVALUE text check (SVALUE == null or length(SVALUE) > 0),
    primary key (SETTING));""")

    conn.execute("insert or ignore into SETTINGS (SETTING, SVALUE) values (?, ?)", ["FILESFOLDER", "FA.files"])
    conn.execute("insert or ignore into SETTINGS (SETTING, SVALUE) values (?, ?)", ["VERSION", "5.2.2"])

    conn.execute("""create table HISTORY
    (TIME datetime unique not null,
    EVENT text not null,
    primary key (TIME));""")

    conn.commit()

    return conn


# noinspection SqlResolve,SqlNoDataSourceInspection,DuplicatedCode
def make_database_5_3(conn: Connection) -> Connection:
    conn.execute("""create table USERS
    (USERNAME text unique not null check (length(USERNAME) > 0),
    FOLDERS text not null,
    ACTIVE boolean not null,
    USERPAGE text not null,
    primary key (USERNAME));""")

    conn.execute("""create table SUBMISSIONS
    (ID integer unique not null check (ID > 0),
    AUTHOR text not null check (length(AUTHOR) > 0),
    TITLE text not null,
    DATE datetime not null,
    DESCRIPTION text not null,
    TAGS text not null,
    CATEGORY text not null,
    SPECIES text not null,
    GENDER text not null,
    RATING text not null,
    TYPE text not null check (TYPE in ('image', 'music', 'text', 'flash')),
    FILEURL text not null,
    FILEEXT text not null,
    FILESAVED integer not null check (FILESAVED in (0, 1, 2, 3, 4, 5, 6, 7)),
    FAVORITE text not null,
    MENTIONS text not null,
    FOLDER text not null check (FOLDER in ('gallery', 'scraps')),
    USERUPDATE boolean not null,
    primary key (ID));""")

    conn.execute("""create table JOURNALS
    (ID integer unique not null check (ID > 0),
    AUTHOR text not null check (length(AUTHOR) > 0),
    TITLE text not null,
    DATE datetime not null,
    CONTENT text not null,
    MENTIONS text not null,
    USERUPDATE integer not null check (USERUPDATE in (0, 1)),
    primary key (ID))""")

    conn.execute("""create table COMMENTS
    (ID integer not null check (ID > 0),
    PARENT_TABLE text not null check (PARENT_TABLE in ('SUBMISSIONS', 'JOURNALS')),
    PARENT_ID integer not null check (PARENT_ID > 0),
    REPLY_TO integer check (REPLY_TO == null or REPLY_TO > 0),
    AUTHOR text not null check (length(AUTHOR) > 0),
    DATE datetime not null,
    TEXT text not null,
    primary key (ID, PARENT_TABLE, PARENT_ID))""")

    conn.execute("""create table SETTINGS
    (SETTING text unique not null check (length(SETTING) > 0),
    SVALUE text check (SVALUE == null or length(SVALUE) > 0),
    primary key (SETTING));""")

    conn.execute("insert or ignore into SETTINGS (SETTING, SVALUE) values (?, ?)", ["FILESFOLDER", "FA.files"])
    conn.execute("insert or ignore into SETTINGS (SETTING, SVALUE) values (?, ?)", ["BACKUPFOLDER", "FA.backup"])
    conn.execute("insert or ignore into SETTINGS (SETTING, SVALUE) values (?, ?)", ["VERSION", "5.3.0"])

    conn.execute("""create table HISTORY
    (TIME datetime unique not null,
    EVENT text not null,
    primary key (TIME));""")

    conn.commit()

    return conn


# noinspection SqlResolve,SqlNoDataSourceInspection,DuplicatedCode
def make_database_5_4(conn: Connection) -> Connection:
    conn.execute("""create table USERS
    (USERNAME text unique not null check (length(USERNAME) > 0),
    FOLDERS text not null,
    ACTIVE boolean not null,
    USERPAGE text not null,
    primary key (USERNAME));""")

    conn.execute("""create table SUBMISSIONS
    (ID integer unique not null check (ID > 0),
    AUTHOR text not null check (length(AUTHOR) > 0),
    TITLE text not null,
    DATE datetime not null,
    DESCRIPTION text not null,
    FOOTER text not null,
    TAGS text not null,
    CATEGORY text not null,
    SPECIES text not null,
    GENDER text not null,
    RATING text not null,
    TYPE text not null check (TYPE in ('image', 'music', 'text', 'flash')),
    FILEURL text not null,
    FILEEXT text not null,
    FILESAVED integer not null check (FILESAVED in (0, 1, 2, 3, 4, 5, 6, 7)),
    FAVORITE text not null,
    MENTIONS text not null,
    FOLDER text not null check (FOLDER in ('gallery', 'scraps')),
    USERUPDATE boolean not null,
    primary key (ID));""")

    conn.execute("""create table JOURNALS
    (ID integer unique not null check (ID > 0),
    AUTHOR text not null check (length(AUTHOR) > 0),
    TITLE text not null,
    DATE datetime not null,
    CONTENT text not null,
    HEADER text not null,
    FOOTER text not null,
    MENTIONS text not null,
    USERUPDATE integer not null check (USERUPDATE in (0, 1)),
    primary key (ID))""")

    conn.execute("""create table COMMENTS
    (ID integer not null check (ID > 0),
    PARENT_TABLE text not null check (PARENT_TABLE in ('SUBMISSIONS', 'JOURNALS')),
    PARENT_ID integer not null check (PARENT_ID > 0),
    REPLY_TO integer check (REPLY_TO == null or REPLY_TO > 0),
    AUTHOR text not null check (length(AUTHOR) > 0),
    DATE datetime not null,
    TEXT text not null,
    primary key (ID, PARENT_TABLE, PARENT_ID))""")

    conn.execute("""create table SETTINGS
    (SETTING text unique not null check (length(SETTING) > 0),
    SVALUE text check (SVALUE == null or length(SVALUE) > 0),
    primary key (SETTING));""")

    conn.execute("insert or ignore into SETTINGS (SETTING, SVALUE) values (?, ?)", ["FILESFOLDER", "FA.files"])
    conn.execute("insert or ignore into SETTINGS (SETTING, SVALUE) values (?, ?)", ["BACKUPFOLDER", "FA.backup"])
    conn.execute("insert or ignore into SETTINGS (SETTING, SVALUE) values (?, ?)", ["VERSION", "5.4.0"])

    conn.execute("""create table HISTORY
    (TIME datetime unique not null,
    EVENT text not null,
    primary key (TIME));""")

    conn.commit()

    return conn


# noinspection SqlResolve,SqlNoDataSourceInspection,DuplicatedCode
def update_5_0(conn: Connection, _db_path: Path, db_new_path: Path):
    make_database_5(connect(db_new_path)).close()
    conn.execute("attach database ? as db_new", [str(db_new_path)])
    conn.execute("insert into db_new.USERS (USERNAME, FOLDERS, USERPAGE)"
                 "select USERNAME, FOLDERS, '' from USERS")
    conn.execute("insert into db_new.SUBMISSIONS (ID,AUTHOR,TITLE,DATE,DESCRIPTION,TAGS,CATEGORY,SPECIES,GENDER,RATING,"
                 "TYPE,FILEURL,FILEEXT,FAVORITE,MENTIONS,FOLDER,USERUPDATE,FILESAVED)"
                 "select ID,AUTHOR,TITLE,DATE,DESCRIPTION,TAGS,CATEGORY,SPECIES,GENDER,RATING,TYPE,FILEURL,FILEEXT,"
                 "FAVORITE,MENTIONS,FOLDER,USERUPDATE,(((FILESAVED >= 10) * 2 ) + (FILESAVED % 10 == 1))"
                 "from SUBMISSIONS")
    conn.execute("insert into db_new.JOURNALS select * from JOURNALS")
    conn.execute("insert or replace into db_new.SETTINGS select * from SETTINGS"
                 " where SETTING not in ('VERSION', 'HISTORY')")
    history: list[tuple[float, str]] = loads(
        conn.execute("select SVALUE from SETTINGS where SETTING = 'HISTORY'").fetchone()[0] or "[]")
    for time, event in sorted(history, key=itemgetter(0)):
        conn.execute("insert into db_new.HISTORY (TIME, EVENT) values (?, ?)",
                     (datetime.fromtimestamp(time).strftime("%Y-%m-%dT%H:%M:%S.%f"), event))


# noinspection SqlResolve,SqlNoDataSourceInspection,DuplicatedCode
def update_5_0_10(conn: Connection, _db_path: Path, db_new_path: Path) -> list[str]:
    messages: list[str] = []
    make_database_5(connect(db_new_path)).close()
    conn.execute("attach database ? as db_new", [str(db_new_path)])
    conn.execute("insert into db_new.USERS select* from USERS")
    conn.execute("insert into db_new.SUBMISSIONS select * from SUBMISSIONS")
    conn.execute("insert into db_new.JOURNALS select * from JOURNALS")
    conn.execute("insert into db_new.HISTORY select * from HISTORY")
    conn.execute("insert or replace into db_new.SETTINGS select * from SETTINGS"
                 " where SETTING != 'VERSION'")
    conn.execute("update db_new.SETTINGS set SVALUE = '5.0.10' where SETTING = 'VERSION'")
    users_favorites: list[str] = [
        u for [u] in conn.execute("select USERNAME from USERS where FOLDERS like '%favorites%'").fetchall()]
    modified: int = 0
    for i, fs_raw in conn.execute("select ID, FAVORITE from SUBMISSIONS where FAVORITE like '%|_|%'").fetchall():
        fs: list[str] = list(filter(bool, fs_raw.split("|")))
        fs_filtered = list(filter(users_favorites.__contains__, fs))
        if fs != fs_filtered:
            conn.execute("update db_new.SUBMISSIONS set FAVORITE = ? where ID = ?",
                         ("".join(f"|{f}|" for f in fs_filtered), i))
            modified += 1
    if modified:
        messages.append(f"{modified} submissions modified.")
    return messages


# noinspection SqlResolve,SqlNoDataSourceInspection,DuplicatedCode
def update_5_1(conn: Connection, _db_path: Path, db_new_path: Path) -> list[str]:
    make_database_5_1(connect(db_new_path)).close()
    conn.execute("attach database ? as db_new", [str(db_new_path)])
    conn.execute("insert into db_new.USERS (USERNAME, FOLDERS, USERPAGE, ACTIVE)"
                 "select USERNAME, replace(FOLDERS, '!', ''), USERPAGE, FOLDERS not like '%!%' from USERS")
    conn.execute("insert into db_new.SUBMISSIONS select * from SUBMISSIONS")
    conn.execute("insert into db_new.JOURNALS select * from JOURNALS")
    conn.execute("insert into db_new.HISTORY select * from HISTORY")
    conn.execute("insert or replace into db_new.SETTINGS select * from SETTINGS where SETTING != 'VERSION'")
    conn.execute("update db_new.SETTINGS set SVALUE = '5.1.0' where SETTING = 'VERSION'")
    return []


# noinspection SqlResolve,SqlNoDataSourceInspection,DuplicatedCode,SqlWithoutWhere
def update_5_1_2(conn: Connection, _db_path: Path, db_new_path: Path) -> list[str]:
    make_database_5_1(connect(db_new_path)).close()
    conn.execute("attach database ? as db_new", [str(db_new_path)])
    conn.execute("insert into db_new.USERS select * from USERS")
    conn.execute("insert into db_new.SUBMISSIONS select * from SUBMISSIONS")
    conn.execute("insert into db_new.JOURNALS select * from JOURNALS")
    conn.execute("insert into db_new.HISTORY select * from HISTORY")
    conn.execute("insert or replace into db_new.SETTINGS select * from SETTINGS where SETTING != 'VERSION'")
    conn.execute("update db_new.SETTINGS set SVALUE = '5.1.2' where SETTING = 'VERSION'")
    conn.execute("""
        update db_new.SUBMISSIONS 
        set CATEGORY = replace(replace(replace(CATEGORY, '/ ', '/'), ' /', '/'), '/', ' / '),
            SPECIES = replace(replace(replace(SPECIES, '/ ', '/'), ' /', '/'), '/', ' / ')""")
    return []


# noinspection SqlResolve,SqlNoDataSourceInspection,DuplicatedCode,SqlWithoutWhere
def update_5_2(conn: Connection, _db_path: Path, db_new_path: Path) -> list[str]:
    make_database_5_2(connect(db_new_path)).close()
    conn.execute("attach database ? as db_new", [str(db_new_path)])
    conn.execute("insert into db_new.USERS select * from USERS")
    conn.execute("insert into db_new.SUBMISSIONS select * from SUBMISSIONS")
    conn.execute("insert into db_new.JOURNALS select * from JOURNALS")
    conn.execute("insert into db_new.HISTORY select * from HISTORY")
    conn.execute("insert or replace into db_new.SETTINGS select * from SETTINGS where SETTING != 'VERSION'")
    conn.execute("update db_new.SETTINGS set SVALUE = '5.2.0' where SETTING = 'VERSION'")
    return []


# noinspection SqlResolve,SqlNoDataSourceInspection,DuplicatedCode,SqlWithoutWhere
def update_5_2_2(conn: Connection, _db_path: Path, db_new_path: Path) -> list[str]:
    make_database_5_2_2(connect(db_new_path)).close()
    conn.execute("attach database ? as db_new", [str(db_new_path)])
    conn.execute("insert into db_new.USERS select * from USERS")
    conn.execute("insert into db_new.SUBMISSIONS select * from SUBMISSIONS")
    conn.execute("insert into db_new.JOURNALS select * from JOURNALS")
    conn.execute("insert into db_new.COMMENTS select * from COMMENTS")
    conn.execute("insert into db_new.HISTORY select * from HISTORY")
    conn.execute("insert or replace into db_new.SETTINGS select * from SETTINGS where SETTING != 'VERSION'")
    conn.execute("update db_new.SETTINGS set SVALUE = '5.2.2' where SETTING = 'VERSION'")
    return []


# noinspection SqlResolve,SqlNoDataSourceInspection,DuplicatedCode,SqlWithoutWhere
def update_5_3(conn: Connection, _db_path: Path, db_new_path: Path) -> list[str]:
    make_database_5_3(connect(db_new_path)).close()
    conn.execute("attach database ? as db_new", [str(db_new_path)])
    conn.execute("insert into db_new.USERS select * from USERS")
    conn.execute("insert into db_new.SUBMISSIONS select * from SUBMISSIONS")
    conn.execute("insert into db_new.JOURNALS select * from JOURNALS")
    conn.execute("insert into db_new.COMMENTS select * from COMMENTS")
    conn.execute("insert into db_new.HISTORY select * from HISTORY")
    conn.execute("insert or replace into db_new.SETTINGS select * from SETTINGS where SETTING != 'VERSION'")
    conn.execute("update db_new.SETTINGS set SVALUE = '5.3.0' where SETTING = 'VERSION'")
    conn.execute("update db_new.SUBMISSIONS set FILEURL = ('|' || FILEURL || '|')")
    conn.execute("update db_new.SUBMISSIONS set FILEEXT = ('|' || FILEEXT || '|') where FILESAVED & 2")
    conn.execute("update db_new.SUBMISSIONS set FILESAVED = FILESAVED + 4 where FILESAVED & 2")
    return []


# noinspection SqlResolve,SqlNoDataSourceInspection,DuplicatedCode,SqlWithoutWhere
def update_5_3_4(conn: Connection, _db_path: Path, db_new_path: Path) -> list[str]:
    make_database_5_3(connect(db_new_path)).close()
    conn.execute("attach database ? as db_new", [str(db_new_path)])
    conn.execute("insert into db_new.USERS select * from USERS")
    conn.execute("insert into db_new.SUBMISSIONS select * from SUBMISSIONS")
    conn.execute("insert into db_new.JOURNALS select * from JOURNALS")
    conn.execute("insert into db_new.COMMENTS select * from COMMENTS")
    conn.execute("insert into db_new.HISTORY select * from HISTORY")
    conn.execute("insert or replace into db_new.SETTINGS select * from SETTINGS where SETTING != 'VERSION'")
    conn.execute("update db_new.SETTINGS set SVALUE = '5.3.4' where SETTING = 'VERSION'")

    def tiered_path(i: int | str, depth: int = 5, width: int = 2) -> Path:
        id_str: str = str(int(i)).zfill(depth * width)
        return Path(*[id_str[n_:n_ + width] for n_ in range(0, depth * width, width)])

    files_folder: Path = Path(conn.execute("select SVALUE from SETTINGS where SETTING = 'FILESFOLDER'").fetchone()[0])
    files_folder = files_folder if files_folder.is_absolute() else (db_new_path.parent / files_folder)
    submissions = conn.execute("""select ID, FILEEXT from db_new.SUBMISSIONS
        where FILEEXT like '%|||%' or FILEEXT like '%||' order by ID""")
    submissions_fixed: int = 0

    for [id_, exts_raw] in submissions:
        exts = exts_raw.removeprefix("|").removesuffix("|").split("||")
        folder: Path = files_folder / tiered_path(id_)
        for n, ext in [(n, e) for n, e in enumerate(exts) if "|" in e]:
            ext_new = ext.removesuffix("|")
            file = folder / f"submission{n if n else ''}.{ext}"
            if file.is_file():
                file.replace(file.with_suffix(f'.{ext_new}' if ext_new else ''))
            exts[n] = ext_new
        conn.execute("update db_new.SUBMISSIONS set FILEEXT = ? where ID = ?", [f"|{'|'.join(exts)}|", id_])
        submissions_fixed += 1
    return [f"{submissions_fixed} submissions extensions fixed"]


# noinspection SqlResolve,SqlNoDataSourceInspection,DuplicatedCode,SqlWithoutWhere
def update_5_4_0(conn: Connection, _db_path: Path, db_new_path: Path) -> list[str]:
    footer_regexp: Pattern = re_compile(r'<div[^>]*class="[^"]*submission-footer[^>]+>(.*)</div>$')

    def clean_html(html: str) -> str:
        html = sub(r" *\n *", "\n", html)
        html = sub(r"[\r\n]", "", html)
        html = sub(r" {2,}", " ", html)
        return html.strip()

    def get_footer(description: str) -> tuple[str, str]:
        description = clean_html(description)
        footer: str = ""
        if match := footer_regexp.search(description):
            footer = sub(r"^<hr/?>", "", match.group(1).strip()).strip()
            description = sub(footer_regexp, "", description)
        return description, footer

    make_database_5_4(connect(db_new_path)).close()
    conn.execute("attach database ? as db_new", [str(db_new_path)])
    conn.execute("insert into db_new.USERS select * from USERS")
    conn.execute(
        "insert into db_new.SUBMISSIONS (ID,AUTHOR,TITLE,DATE,DESCRIPTION,TAGS,CATEGORY,SPECIES,GENDER,"
        "RATING,TYPE,FILEURL,FILEEXT,FAVORITE,MENTIONS,FOLDER,USERUPDATE,FILESAVED,FOOTER)"
        "select ID,AUTHOR,TITLE,DATE,DESCRIPTION,TAGS,CATEGORY,SPECIES,GENDER,RATING,TYPE,FILEURL,"
        "FILEEXT,FAVORITE,MENTIONS,FOLDER,USERUPDATE,FILESAVED,'' from SUBMISSIONS")
    conn.execute(
        "insert into db_new.JOURNALS (ID,AUTHOR,TITLE,DATE,CONTENT,MENTIONS,USERUPDATE,HEADER,FOOTER)"
        "select ID,AUTHOR,TITLE,DATE,CONTENT,MENTIONS,USERUPDATE,'','' from JOURNALS")
    conn.execute("insert into db_new.COMMENTS select * from COMMENTS")
    conn.execute("insert into db_new.HISTORY select * from HISTORY")
    conn.execute("insert or replace into db_new.SETTINGS select * from SETTINGS where SETTING != 'VERSION'")
    conn.execute("update db_new.SETTINGS set SVALUE = '5.4.0' where SETTING = 'VERSION'")

    footers_extracted: int = 0
    for i, d in conn.execute("select ID, DESCRIPTION from db_new.SUBMISSIONS order by ID"):
        d, f = get_footer(d)
        footers_extracted += 1 if f else 0
        conn.execute("update db_new.SUBMISSIONS set DESCRIPTION = ?, FOOTER = ? where ID = ?", [d, f, i])

    for i, c in conn.execute("select ID, CONTENT from db_new.JOURNALS order by ID"):
        conn.execute("update db_new.JOURNALS set CONTENT = ? where ID = ?", [clean_html(c), i])

    return [f"{footers_extracted} submission footers extracted"]


def update_wrapper(conn: Connection, update_function: Callable[[Connection, Path, Path], list[str] | None],
                   version_old: str, version_new: str) -> Connection:
    print(f"Updating {version_old} to {version_new}... ", end="", flush=True)
    db_path: Path = p if (p := database_path(conn)) else Path("FA.db")
    db_new_path: Path = db_path.with_name(f".new_{db_path.name}")
    db_new_path.unlink(missing_ok=True)
    try:
        messages: list[str] = update_function(conn, db_path, db_new_path) or []
        conn.commit()
        conn.close()
        conn = None
        orig_name: str = db_path.name
        db_path.replace(db_path := db_path.with_name(f"v{version_old.replace('.', '_')}_{db_path.name}"))
        db_new_path.replace(db_new_path := db_new_path.with_name(orig_name))
        print("Complete")
        for message in messages:
            print(f"  {message}")
        print("  Previous version moved to:", db_path, end="", flush=True)
        return connect(db_new_path)
    except BaseException as err:
        conn.close()
        conn = None
        db_new_path.unlink(missing_ok=True)
        raise err
    finally:
        if conn is not None:
            conn.commit()
            conn.close()
        print()


# noinspection SqlResolve,SqlNoDataSourceInspection
def update_patch(conn: Connection, version: str, target_version: str) -> Connection:
    print(f"Patching {version} to {target_version}... ", end="", flush=True)
    conn.execute("UPDATE SETTINGS SET SVALUE = ? WHERE SETTING = 'VERSION'", [target_version])
    conn.commit()
    print("Complete")
    return conn


def update_database(conn: Connection, version: str) -> Connection:
    if not (db_version := get_version(conn)):
        raise DatabaseError("Cannot read version from database.")
    elif (v := compare_versions(db_version, version)) == 0:
        return conn
    elif v > 0:
        raise DatabaseError("Database version is newer than program.")
    elif compare_versions(db_version, "4.19.0") < 0:
        raise DatabaseError("Update does not support versions lower than 4.19.0.")
    elif compare_versions(db_version, v := "5.0.0") < 0:
        conn = update_wrapper(conn, update_5_0, db_version, v)  # 4.19.x to 5.0.0
    elif compare_versions(db_version, v := "5.0.10") < 0:
        conn = update_wrapper(conn, update_5_0_10, db_version, v)  # 5.0.x to 5.0.10
    elif compare_versions(db_version, v := "5.1.0") < 0:
        conn = update_wrapper(conn, update_5_1, db_version, v)  # 5.0.10 to 5.1.0
    elif compare_versions(db_version, v := "5.1.2") < 0:
        conn = update_wrapper(conn, update_5_1_2, db_version, v)  # 5.1.0-5.1.1 to 5.1.2
    elif compare_versions(db_version, v := "5.2.0") < 0:
        conn = update_wrapper(conn, update_5_2, db_version, v)  # 5.1.2 to 5.2.0
    elif compare_versions(db_version, v := "5.2.2") < 0:
        conn = update_wrapper(conn, update_5_2_2, db_version, v)  # 5.2.0-5.2.1 to 5.2.2
    elif compare_versions(db_version, v := "5.3.0") < 0:
        conn = update_wrapper(conn, update_5_3, db_version, v)  # 5.2.2 to 5.3.0
    elif compare_versions(db_version, v := "5.3.4") < 0:
        conn = update_wrapper(conn, update_5_3_4, db_version, v)  # 5.3.0 to 5.3.4
    elif compare_versions(db_version, v := "5.4.0") < 0:
        conn = update_wrapper(conn, update_5_4_0, db_version, v)  # 5.3.4 to 5.4.0
    elif compare_versions(db_version, version) < 0:
        return update_patch(conn, db_version, version)  # Update to the latest patch

    return update_database(conn, version)
