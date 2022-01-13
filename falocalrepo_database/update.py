from datetime import datetime
from json import loads
from operator import itemgetter
from pathlib import Path
from sqlite3 import Connection
from sqlite3 import DatabaseError
from sqlite3 import OperationalError
from sqlite3 import connect
from typing import Callable
from typing import Collection
from typing import Optional
from typing import Union


__all__ = [
    "compare_versions",
    "update_database",
]


# noinspection SqlResolve
def get_version(conn: Connection) -> str:
    try:
        # Database version 3.0.0 and above
        return conn.execute("SELECT SVALUE FROM SETTINGS WHERE SETTING = 'VERSION'").fetchone()[0]
    except OperationalError:
        # Database version 2.7.0
        return conn.execute("SELECT VALUE FROM SETTINGS WHERE FIELD = 'VERSION'").fetchone()[0]


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


def count(conn: Connection, table: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchall()[0][0]


def database_path(conn: Connection) -> Path | None:
    name: str
    filename: Optional[str]
    for _, name, filename in conn.execute("PRAGMA database_list"):
        if name == "main" and filename is not None:
            return Path(filename)

    return None


def update_wrapper(conn: Connection, update_function: Callable[[Connection, Path, Path], None], version_old: str,
                   version_new: str) -> Connection:
    print(f"Updating {version_old} to {version_new}... ", end="", flush=True)
    db_path: Path = p if (p := database_path(conn)) else Path("FA.db")
    db_new_path: Path = db_path.with_name(f".new_{db_path.name}")
    db_new_path.unlink(missing_ok=True)
    try:
        update_function(conn, db_path, db_new_path)
        conn.commit()
        conn.close()
        conn = None
        orig_name: str = db_path.name
        db_path.replace(db_path := db_path.with_name(f"v{version_old.replace('.', '_')}_{db_path.name}"))
        db_new_path.replace(db_new_path := db_new_path.with_name(orig_name))
        print("Complete")
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


# noinspection SqlResolve,DuplicatedCode
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


# noinspection SqlResolve,DuplicatedCode
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
    elif compare_versions(db_version, version) < 0:
        return update_patch(conn, db_version, version)  # Update to the latest patch

    return update_database(conn, version)
