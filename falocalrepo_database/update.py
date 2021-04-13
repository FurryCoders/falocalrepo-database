from glob import glob
from json import dumps as json_dumps
from json import loads as json_loads
from os import makedirs
from os import remove
from os.path import basename
from os.path import dirname
from os.path import isdir
from os.path import isfile
from os.path import join
from re import Pattern
from re import compile as re_compile
from re import findall
from re import sub
from shutil import copy
from shutil import move
from shutil import rmtree
from sqlite3 import Connection
from sqlite3 import DatabaseError
from sqlite3 import OperationalError
from sqlite3 import connect as connect_database
from typing import Callable
from typing import Collection
from typing import Optional
from typing import Union


def get_version(db: Connection) -> str:
    try:
        # Database version 3.0.0 and above
        return db.execute(f"SELECT SVALUE FROM SETTINGS WHERE SETTING = 'VERSION'").fetchone()[0]
    except OperationalError:
        # Database version 2.7.0
        return db.execute(f"SELECT VALUE FROM SETTINGS WHERE FIELD = 'VERSION'").fetchone()[0]


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


def insert(db: Connection, table: str, keys: Collection[str], values: Collection[Union[int, str]],
           replace: bool = True):
    db.execute(
        f"""INSERT OR {"REPLACE" if replace else "IGNORE"} INTO {table}
        ({",".join(keys)})
        VALUES ({",".join(["?"] * len(values))})""",
        values
    )


def count(db: Connection, table: str) -> int:
    return db.execute(f"SELECT COUNT(*) FROM {table}").fetchall()[0][0]


def database_path(db: Connection) -> Optional[str]:
    name: str
    filename: Optional[str]
    for _, name, filename in db.execute("PRAGMA database_list"):
        if name == "main" and filename is not None:
            return filename

    return None


def clean_username(user: str) -> str:
    return sub(r"[^a-z0-9.~-]", "", user.lower())


def tiered_path(id_: Union[int, str], depth: int = 5, width: int = 2) -> str:
    id_str: str = str(int(id_)).zfill(depth * width)
    return join(*[id_str[n:n + width] for n in range(0, depth * width, width)])


def make_database_3(db: Connection) -> Connection:
    # Create submissions table
    db.execute(
        """CREATE TABLE IF NOT EXISTS SUBMISSIONS
        (ID INT UNIQUE NOT NULL,
        AUTHOR TEXT NOT NULL,
        TITLE TEXT,
        UDATE DATE NOT NULL,
        DESCRIPTION TEXT,
        TAGS TEXT,
        CATEGORY TEXT,
        SPECIES TEXT,
        GENDER TEXT,
        RATING TEXT,
        FILELINK TEXT,
        FILEEXT TEXT,
        FILESAVED INT,
        PRIMARY KEY (ID ASC));"""
    )

    # Create users table
    db.execute(
        """CREATE TABLE IF NOT EXISTS USERS
        (USERNAME TEXT UNIQUE NOT NULL,
        FOLDERS TEXT NOT NULL,
        GALLERY TEXT,
        SCRAPS TEXT,
        FAVORITES TEXT,
        EXTRAS TEXT,
        PRIMARY KEY (USERNAME ASC));"""
    )

    # Create settings table
    db.execute(
        """CREATE TABLE IF NOT EXISTS SETTINGS
        (SETTING TEXT UNIQUE,
        SVALUE TEXT,
        PRIMARY KEY (SETTING ASC));"""
    )

    db.commit()

    # Add settings
    insert(db, "SETTINGS", ["SETTING", "SVALUE"], ["USRN", "0"], False)
    insert(db, "SETTINGS", ["SETTING", "SVALUE"], ["SUBN", "0"], False)
    insert(db, "SETTINGS", ["SETTING", "SVALUE"], ["LASTUPDATE", "0"], False)
    insert(db, "SETTINGS", ["SETTING", "SVALUE"], ["LASTSTART", "0"], False)
    insert(db, "SETTINGS", ["SETTING", "SVALUE"], ["COOKIES", "{}"], False)
    insert(db, "SETTINGS", ["SETTING", "SVALUE"], ["FILESFOLDER", "FA.files"], False)
    insert(db, "SETTINGS", ["SETTING", "SVALUE"], ["VERSION", "3.0.0"], False)

    db.commit()

    return db


def make_database_3_1(db: Connection) -> Connection:
    # Create submissions table
    db.execute(
        """CREATE TABLE IF NOT EXISTS SUBMISSIONS
        (ID INT UNIQUE NOT NULL,
        AUTHOR TEXT NOT NULL,
        TITLE TEXT,
        UDATE DATE NOT NULL,
        DESCRIPTION TEXT,
        TAGS TEXT,
        CATEGORY TEXT,
        SPECIES TEXT,
        GENDER TEXT,
        RATING TEXT,
        FILELINK TEXT,
        FILEEXT TEXT,
        FILESAVED INT,
        PRIMARY KEY (ID ASC));"""
    )

    # Create users table
    db.execute(
        """CREATE TABLE IF NOT EXISTS USERS
        (USERNAME TEXT UNIQUE NOT NULL,
        FOLDERS TEXT NOT NULL,
        GALLERY TEXT,
        SCRAPS TEXT,
        FAVORITES TEXT,
        MENTIONS TEXT,
        PRIMARY KEY (USERNAME ASC));"""
    )

    # Create settings table
    db.execute(
        """CREATE TABLE IF NOT EXISTS SETTINGS
        (SETTING TEXT UNIQUE,
        SVALUE TEXT,
        PRIMARY KEY (SETTING ASC));"""
    )

    db.commit()

    # Add settings
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["USRN", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["SUBN", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["JRNN", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["LASTUPDATE", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["LASTSTART", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["COOKIES", "{}"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["FILESFOLDER", "FA.files"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["VERSION", "3.1.0"])

    db.commit()

    return db


def make_database_3_2(db: Connection) -> Connection:
    db.execute(
        f"""CREATE TABLE IF NOT EXISTS USERS
        (USERNAME TEXT UNIQUE NOT NULL,
        FOLDERS TEXT NOT NULL,
        GALLERY TEXT,
        SCRAPS TEXT,
        FAVORITES TEXT,
        MENTIONS TEXT,
        JOURNALS TEXT,
        PRIMARY KEY (USERNAME ASC));"""
    )

    db.execute(
        f"""CREATE TABLE IF NOT EXISTS SUBMISSIONS
        (ID INT UNIQUE NOT NULL,
        AUTHOR TEXT NOT NULL,
        TITLE TEXT,
        UDATE DATE NOT NULL,
        DESCRIPTION TEXT,
        TAGS TEXT,
        CATEGORY TEXT,
        SPECIES TEXT,
        GENDER TEXT,
        RATING TEXT,
        FILELINK TEXT,
        FILEEXT TEXT,
        FILESAVED INT,
        PRIMARY KEY (ID ASC));"""
    )

    db.execute(
        f"""CREATE TABLE IF NOT EXISTS JOURNALS
        (ID INT UNIQUE NOT NULL,
        AUTHOR TEXT NOT NULL,
        TITLE TEXT,
        UDATE DATE NOT NULL,
        CONTENT TEXT,
        PRIMARY KEY (ID ASC));"""
    )

    db.execute(
        f"""CREATE TABLE IF NOT EXISTS SETTINGS
        (SETTING TEXT UNIQUE,
        SVALUE TEXT,
        PRIMARY KEY (SETTING ASC));"""
    )

    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["USRN", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["SUBN", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["JRNN", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["LASTUPDATE", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["LASTSTART", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["COOKIES", "{}"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["FILESFOLDER", "FA.files"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["VERSION", "3.2.0"])

    db.commit()

    return db


def make_database_3_3(db: Connection) -> Connection:
    db.execute(
        f"""CREATE TABLE IF NOT EXISTS USERS
        (USERNAME TEXT UNIQUE NOT NULL,
        FOLDERS TEXT NOT NULL,
        GALLERY TEXT,
        SCRAPS TEXT,
        FAVORITES TEXT,
        MENTIONS TEXT,
        JOURNALS TEXT,
        PRIMARY KEY (USERNAME ASC));"""
    )

    db.execute(
        f"""CREATE TABLE IF NOT EXISTS SUBMISSIONS
        (ID INT UNIQUE NOT NULL,
        AUTHOR TEXT NOT NULL,
        TITLE TEXT,
        UDATE DATE NOT NULL,
        DESCRIPTION TEXT,
        TAGS TEXT,
        CATEGORY TEXT,
        SPECIES TEXT,
        GENDER TEXT,
        RATING TEXT,
        FILELINK TEXT,
        FILEEXT TEXT,
        FILESAVED INT,
        PRIMARY KEY (ID ASC));"""
    )

    db.execute(
        f"""CREATE TABLE IF NOT EXISTS JOURNALS
        (ID INT UNIQUE NOT NULL,
        AUTHOR TEXT NOT NULL,
        TITLE TEXT,
        UDATE DATE NOT NULL,
        CONTENT TEXT,
        PRIMARY KEY (ID ASC));"""
    )

    db.execute(
        f"""CREATE TABLE IF NOT EXISTS SETTINGS
        (SETTING TEXT UNIQUE,
        SVALUE TEXT,
        PRIMARY KEY (SETTING ASC));"""
    )

    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["USRN", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["SUBN", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["JRNN", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["HISTORY", "[]"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["COOKIES", "{}"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["FILESFOLDER", "FA.files"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["VERSION", "3.3.0"])

    db.commit()

    return db


def make_database_4(db: Connection) -> Connection:
    db.execute(
        f"""CREATE TABLE IF NOT EXISTS USERS
        (USERNAME TEXT UNIQUE NOT NULL CHECK (length(USERNAME) > 0),
        FOLDERS TEXT NOT NULL,
        GALLERY TEXT NOT NULL,
        SCRAPS TEXT NOT NULL,
        FAVORITES TEXT NOT NULL,
        MENTIONS TEXT NOT NULL,
        JOURNALS TEXT NOT NULL,
        PRIMARY KEY (USERNAME ASC));"""
    )

    db.execute(
        f"""CREATE TABLE IF NOT EXISTS SUBMISSIONS
        (ID INT UNIQUE NOT NULL CHECK (ID > 0),
        AUTHOR TEXT NOT NULL CHECK (length(AUTHOR) > 0),
        TITLE TEXT NOT NULL,
        DATE DATE NOT NULL CHECK (DATE==strftime('%Y-%m-%d',DATE)),
        DESCRIPTION TEXT NOT NULL,
        TAGS TEXT NOT NULL,
        CATEGORY TEXT NOT NULL,
        SPECIES TEXT NOT NULL,
        GENDER TEXT NOT NULL,
        RATING TEXT NOT NULL,
        FILELINK TEXT NOT NULL,
        FILEEXT TEXT NOT NULL,
        FILESAVED INT NOT NULL CHECK (FILESAVED in (0, 1)),
        PRIMARY KEY (ID ASC));"""
    )

    db.execute(
        f"""CREATE TABLE IF NOT EXISTS JOURNALS
        (ID INT UNIQUE NOT NULL CHECK (ID > 0),
        AUTHOR TEXT NOT NULL CHECK (length(AUTHOR) > 0),
        TITLE TEXT NOT NULL,
        DATE DATE NOT NULL CHECK (DATE==strftime('%Y-%m-%d',DATE)),
        CONTENT TEXT NOT NULL,
        PRIMARY KEY (ID ASC));"""
    )

    db.execute(
        f"""CREATE TABLE IF NOT EXISTS SETTINGS
        (SETTING TEXT UNIQUE NOT NULL CHECK (length(SETTING) > 0),
        SVALUE TEXT NOT NULL CHECK (length(SVALUE) > 0),
        PRIMARY KEY (SETTING ASC));"""
    )

    # Add settings
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["USRN", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["SUBN", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["JRNN", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["HISTORY", "[]"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["COOKIES", "{}"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["FILESFOLDER", "FA.files"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["VERSION", "4.0.0"])

    db.commit()

    return db


def make_database_4_3(db: Connection) -> Connection:
    db.execute(
        f"""CREATE TABLE IF NOT EXISTS USERS
        (USERNAME TEXT UNIQUE NOT NULL CHECK (length(USERNAME) > 0),
        FOLDERS TEXT NOT NULL,
        GALLERY TEXT NOT NULL,
        SCRAPS TEXT NOT NULL,
        FAVORITES TEXT NOT NULL,
        MENTIONS TEXT NOT NULL,
        JOURNALS TEXT NOT NULL,
        PRIMARY KEY (USERNAME ASC));"""
    )

    db.execute(
        f"""CREATE TABLE IF NOT EXISTS SUBMISSIONS
        (ID INT UNIQUE NOT NULL CHECK (ID > 0),
        AUTHOR TEXT NOT NULL CHECK (length(AUTHOR) > 0),
        TITLE TEXT NOT NULL,
        DATE DATE NOT NULL CHECK (DATE==strftime('%Y-%m-%d',DATE)),
        DESCRIPTION TEXT NOT NULL,
        TAGS TEXT NOT NULL,
        CATEGORY TEXT NOT NULL,
        SPECIES TEXT NOT NULL,
        GENDER TEXT NOT NULL,
        RATING TEXT NOT NULL,
        FILELINK TEXT NOT NULL,
        FILEEXT TEXT NOT NULL,
        FILESAVED INT NOT NULL CHECK (FILESAVED in (0, 1)),
        PRIMARY KEY (ID ASC));"""
    )

    db.execute(
        f"""CREATE TABLE IF NOT EXISTS JOURNALS
        (ID INT UNIQUE NOT NULL CHECK (ID > 0),
        AUTHOR TEXT NOT NULL CHECK (length(AUTHOR) > 0),
        TITLE TEXT NOT NULL,
        DATE DATE NOT NULL CHECK (DATE==strftime('%Y-%m-%d',DATE)),
        CONTENT TEXT NOT NULL,
        PRIMARY KEY (ID ASC));"""
    )

    db.execute(
        f"""CREATE TABLE IF NOT EXISTS SETTINGS
        (SETTING TEXT UNIQUE NOT NULL CHECK (length(SETTING) > 0),
        SVALUE TEXT NOT NULL CHECK (length(SVALUE) > 0),
        PRIMARY KEY (SETTING ASC));"""
    )

    # Add settings
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["HISTORY", "[]"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["COOKIES", "{}"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["FILESFOLDER", "FA.files"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["VERSION", "4.3.0"])

    db.commit()

    return db


def make_database_4_4(db: Connection) -> Connection:
    db.execute(
        f"""CREATE TABLE IF NOT EXISTS USERS
        (USERNAME TEXT UNIQUE NOT NULL CHECK (length(USERNAME) > 0),
        FOLDERS TEXT NOT NULL,
        GALLERY TEXT NOT NULL,
        SCRAPS TEXT NOT NULL,
        FAVORITES TEXT NOT NULL,
        MENTIONS TEXT NOT NULL,
        JOURNALS TEXT NOT NULL,
        PRIMARY KEY (USERNAME ASC));"""
    )

    db.execute(
        f"""CREATE TABLE IF NOT EXISTS SUBMISSIONS
        (ID INT UNIQUE NOT NULL CHECK (ID > 0),
        AUTHOR TEXT NOT NULL CHECK (length(AUTHOR) > 0),
        TITLE TEXT NOT NULL,
        DATE DATE NOT NULL CHECK (DATE==strftime('%Y-%m-%d',DATE)),
        DESCRIPTION TEXT NOT NULL,
        TAGS TEXT NOT NULL,
        CATEGORY TEXT NOT NULL,
        SPECIES TEXT NOT NULL,
        GENDER TEXT NOT NULL,
        RATING TEXT NOT NULL,
        FILELINK TEXT NOT NULL,
        FILEEXT TEXT NOT NULL,
        FILESAVED INT NOT NULL CHECK (FILESAVED in (0, 1)),
        FAVORITE TEXT NOT NULL,
        PRIMARY KEY (ID ASC));"""
    )

    db.execute(
        f"""CREATE TABLE IF NOT EXISTS JOURNALS
        (ID INT UNIQUE NOT NULL CHECK (ID > 0),
        AUTHOR TEXT NOT NULL CHECK (length(AUTHOR) > 0),
        TITLE TEXT NOT NULL,
        DATE DATE NOT NULL CHECK (DATE==strftime('%Y-%m-%d',DATE)),
        CONTENT TEXT NOT NULL,
        PRIMARY KEY (ID ASC));"""
    )

    db.execute(
        f"""CREATE TABLE IF NOT EXISTS SETTINGS
        (SETTING TEXT UNIQUE NOT NULL CHECK (length(SETTING) > 0),
        SVALUE TEXT NOT NULL CHECK (length(SVALUE) > 0),
        PRIMARY KEY (SETTING ASC));"""
    )

    # Add settings
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["HISTORY", "[]"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["COOKIES", "{}"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["FILESFOLDER", "FA.files"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["VERSION", "4.4.0"])

    db.commit()

    return db


def make_database_4_5(db: Connection) -> Connection:
    db.execute(
        f"""CREATE TABLE IF NOT EXISTS USERS
        (USERNAME TEXT UNIQUE NOT NULL CHECK (length(USERNAME) > 0),
        FOLDERS TEXT NOT NULL,
        PRIMARY KEY (USERNAME ASC));"""
    )

    db.execute(
        """CREATE TABLE IF NOT EXISTS SUBMISSIONS
        (ID INT UNIQUE NOT NULL CHECK (ID > 0),
        AUTHOR TEXT NOT NULL CHECK (length(AUTHOR) > 0),
        TITLE TEXT NOT NULL,
        DATE DATE NOT NULL CHECK (DATE==strftime('%Y-%m-%d',DATE)),
        DESCRIPTION TEXT NOT NULL,
        TAGS TEXT NOT NULL,
        CATEGORY TEXT NOT NULL,
        SPECIES TEXT NOT NULL,
        GENDER TEXT NOT NULL,
        RATING TEXT NOT NULL,
        FILELINK TEXT NOT NULL,
        FILEEXT TEXT NOT NULL,
        FILESAVED INT NOT NULL CHECK (FILESAVED in (0, 1)),
        FAVORITE TEXT NOT NULL,
        MENTIONS TEXT NOT NULL,
        FOLDER TEXT NOT NULL CHECK (FOLDER = 'gallery' OR FOLDER = 'scraps'),
        PRIMARY KEY (ID ASC));"""
    )

    db.execute(
        """CREATE TABLE IF NOT EXISTS JOURNALS
        (ID INT UNIQUE NOT NULL CHECK (ID > 0),
        AUTHOR TEXT NOT NULL CHECK (length(AUTHOR) > 0),
        TITLE TEXT NOT NULL,
        DATE DATE NOT NULL CHECK (DATE==strftime('%Y-%m-%d',DATE)),
        CONTENT TEXT NOT NULL,
        PRIMARY KEY (ID ASC));""",
    )

    db.execute(
        """CREATE TABLE IF NOT EXISTS SETTINGS
        (SETTING TEXT UNIQUE NOT NULL CHECK (length(SETTING) > 0),
        SVALUE TEXT NOT NULL CHECK (length(SVALUE) > 0),
        PRIMARY KEY (SETTING ASC));"""
    )

    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["HISTORY", "[]"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["COOKIES", "{}"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["FILESFOLDER", "FA.files"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["VERSION", "4.5.0"])

    db.commit()

    return db


def make_database_4_6(db: Connection) -> Connection:
    db.execute(
        f"""CREATE TABLE IF NOT EXISTS USERS
        (USERNAME TEXT UNIQUE NOT NULL CHECK (length(USERNAME) > 0),
        FOLDERS TEXT NOT NULL,
        PRIMARY KEY (USERNAME ASC));"""
    )

    db.execute(
        """CREATE TABLE IF NOT EXISTS SUBMISSIONS
        (ID INT UNIQUE NOT NULL CHECK (ID > 0),
        AUTHOR TEXT NOT NULL CHECK (length(AUTHOR) > 0),
        TITLE TEXT NOT NULL,
        DATE DATE NOT NULL CHECK (DATE==strftime('%Y-%m-%d',DATE)),
        DESCRIPTION TEXT NOT NULL,
        TAGS TEXT NOT NULL,
        CATEGORY TEXT NOT NULL,
        SPECIES TEXT NOT NULL,
        GENDER TEXT NOT NULL,
        RATING TEXT NOT NULL,
        FILELINK TEXT NOT NULL,
        FILEEXT TEXT NOT NULL,
        FILESAVED INT NOT NULL CHECK (FILESAVED in (0, 1)),
        FAVORITE TEXT NOT NULL,
        MENTIONS TEXT NOT NULL,
        FOLDER TEXT NOT NULL CHECK (FOLDER = 'gallery' OR FOLDER = 'scraps'),
        USERUPDATE INT NOT NULL CHECK (USERUPDATE in (0, 1)),
        PRIMARY KEY (ID ASC));"""
    )

    db.execute(
        """CREATE TABLE IF NOT EXISTS JOURNALS
        (ID INT UNIQUE NOT NULL CHECK (ID > 0),
        AUTHOR TEXT NOT NULL CHECK (length(AUTHOR) > 0),
        TITLE TEXT NOT NULL,
        DATE DATE NOT NULL CHECK (DATE==strftime('%Y-%m-%d',DATE)),
        CONTENT TEXT NOT NULL,
        USERUPDATE INT NOT NULL CHECK (USERUPDATE in (0, 1)),
        PRIMARY KEY (ID ASC));""",
    )

    db.execute(
        """CREATE TABLE IF NOT EXISTS SETTINGS
        (SETTING TEXT UNIQUE NOT NULL CHECK (length(SETTING) > 0),
        SVALUE TEXT NOT NULL CHECK (length(SVALUE) > 0),
        PRIMARY KEY (SETTING ASC));"""
    )

    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["HISTORY", "[]"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["COOKIES", "{}"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["FILESFOLDER", "FA.files"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["VERSION", "4.6.0"])

    db.commit()

    return db


def make_database_4_7(db: Connection) -> Connection:
    db.execute(
        f"""CREATE TABLE IF NOT EXISTS USERS
        (USERNAME TEXT UNIQUE NOT NULL CHECK (length(USERNAME) > 0),
        FOLDERS TEXT NOT NULL,
        PRIMARY KEY (USERNAME ASC));"""
    )

    db.execute(
        """CREATE TABLE IF NOT EXISTS SUBMISSIONS
        (ID INT UNIQUE NOT NULL CHECK (ID > 0),
        AUTHOR TEXT NOT NULL CHECK (length(AUTHOR) > 0),
        TITLE TEXT NOT NULL,
        DATE DATE NOT NULL CHECK (DATE==strftime('%Y-%m-%d',DATE)),
        DESCRIPTION TEXT NOT NULL,
        TAGS TEXT NOT NULL,
        CATEGORY TEXT NOT NULL,
        SPECIES TEXT NOT NULL,
        GENDER TEXT NOT NULL,
        RATING TEXT NOT NULL,
        FILELINK TEXT NOT NULL,
        FILEEXT TEXT NOT NULL,
        FILESAVED INT NOT NULL CHECK (FILESAVED in (0, 1)),
        FAVORITE TEXT NOT NULL,
        MENTIONS TEXT NOT NULL,
        FOLDER TEXT NOT NULL CHECK (FOLDER = 'gallery' OR FOLDER = 'scraps'),
        USERUPDATE INT NOT NULL CHECK (USERUPDATE in (0, 1)),
        PRIMARY KEY (ID ASC));"""
    )

    db.execute(
        """CREATE TABLE IF NOT EXISTS JOURNALS
        (ID INT UNIQUE NOT NULL CHECK (ID > 0),
        AUTHOR TEXT NOT NULL CHECK (length(AUTHOR) > 0),
        TITLE TEXT NOT NULL,
        DATE DATE NOT NULL CHECK (DATE==strftime('%Y-%m-%d',DATE)),
        CONTENT TEXT NOT NULL,
        MENTIONS TEXT NOT NULL,
        USERUPDATE INT NOT NULL CHECK (USERUPDATE in (0, 1)),
        PRIMARY KEY (ID ASC));""",
    )

    db.execute(
        """CREATE TABLE IF NOT EXISTS SETTINGS
        (SETTING TEXT UNIQUE NOT NULL CHECK (length(SETTING) > 0),
        SVALUE TEXT NOT NULL CHECK (length(SVALUE) > 0),
        PRIMARY KEY (SETTING ASC));"""
    )

    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["HISTORY", "[]"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["COOKIES", "{}"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["FILESFOLDER", "FA.files"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["VERSION", "4.7.0"])

    db.commit()

    return db


def make_database_4_8(db: Connection) -> Connection:
    db.execute(
        f"""CREATE TABLE IF NOT EXISTS USERS
        (USERNAME TEXT UNIQUE NOT NULL CHECK (length(USERNAME) > 0),
        FOLDERS TEXT NOT NULL,
        PRIMARY KEY (USERNAME ASC));"""
    )

    db.execute(
        """CREATE TABLE IF NOT EXISTS SUBMISSIONS
        (ID INT UNIQUE NOT NULL CHECK (ID > 0),
        AUTHOR TEXT NOT NULL CHECK (length(AUTHOR) > 0),
        TITLE TEXT NOT NULL,
        DATE DATE NOT NULL CHECK (DATE==strftime('%Y-%m-%d',DATE)),
        DESCRIPTION TEXT NOT NULL,
        TAGS TEXT NOT NULL,
        CATEGORY TEXT NOT NULL,
        SPECIES TEXT NOT NULL,
        GENDER TEXT NOT NULL,
        RATING TEXT NOT NULL,
        FILEURL TEXT NOT NULL,
        FILEEXT TEXT NOT NULL,
        FILESAVED INT NOT NULL CHECK (FILESAVED in (0, 1)),
        FAVORITE TEXT NOT NULL,
        MENTIONS TEXT NOT NULL,
        FOLDER TEXT NOT NULL CHECK (FOLDER = 'gallery' OR FOLDER = 'scraps'),
        USERUPDATE INT NOT NULL CHECK (USERUPDATE in (0, 1)),
        PRIMARY KEY (ID ASC));"""
    )

    db.execute(
        """CREATE TABLE IF NOT EXISTS JOURNALS
        (ID INT UNIQUE NOT NULL CHECK (ID > 0),
        AUTHOR TEXT NOT NULL CHECK (length(AUTHOR) > 0),
        TITLE TEXT NOT NULL,
        DATE DATE NOT NULL CHECK (DATE==strftime('%Y-%m-%d',DATE)),
        CONTENT TEXT NOT NULL,
        MENTIONS TEXT NOT NULL,
        USERUPDATE INT NOT NULL CHECK (USERUPDATE in (0, 1)),
        PRIMARY KEY (ID ASC));""",
    )

    db.execute(
        """CREATE TABLE IF NOT EXISTS SETTINGS
        (SETTING TEXT UNIQUE NOT NULL CHECK (length(SETTING) > 0),
        SVALUE TEXT NOT NULL CHECK (length(SVALUE) > 0),
        PRIMARY KEY (SETTING ASC));"""
    )

    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["HISTORY", "[]"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["COOKIES", "{}"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["FILESFOLDER", "FA.files"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["VERSION", "4.8.0"])

    db.commit()

    return db


def make_database_4_9(db: Connection) -> Connection:
    db.execute(
        f"""CREATE TABLE IF NOT EXISTS USERS
        (USERNAME TEXT UNIQUE NOT NULL CHECK (length(USERNAME) > 0),
        FOLDERS TEXT NOT NULL,
        PRIMARY KEY (USERNAME ASC));"""
    )

    db.execute(
        """CREATE TABLE IF NOT EXISTS SUBMISSIONS
        (ID INT UNIQUE NOT NULL CHECK (ID > 0),
        AUTHOR TEXT NOT NULL CHECK (length(AUTHOR) > 0),
        TITLE TEXT NOT NULL,
        DATE DATE NOT NULL CHECK (DATE==strftime('%Y-%m-%d',DATE)),
        DESCRIPTION TEXT NOT NULL,
        TAGS TEXT NOT NULL,
        CATEGORY TEXT NOT NULL,
        SPECIES TEXT NOT NULL,
        GENDER TEXT NOT NULL,
        RATING TEXT NOT NULL,
        TYPE TEXT NOT NULL CHECK (TYPE IN ('image', 'music', 'text', 'flash')),
        FILEURL TEXT NOT NULL,
        FILEEXT TEXT NOT NULL,
        FILESAVED INT NOT NULL CHECK (FILESAVED in (0, 1)),
        FAVORITE TEXT NOT NULL,
        MENTIONS TEXT NOT NULL,
        FOLDER TEXT NOT NULL CHECK (FOLDER IN ('gallery', 'scraps')),
        USERUPDATE INT NOT NULL CHECK (USERUPDATE in (0, 1)),
        PRIMARY KEY (ID ASC));"""
    )

    db.execute(
        """CREATE TABLE IF NOT EXISTS JOURNALS
        (ID INT UNIQUE NOT NULL CHECK (ID > 0),
        AUTHOR TEXT NOT NULL CHECK (length(AUTHOR) > 0),
        TITLE TEXT NOT NULL,
        DATE DATE NOT NULL CHECK (DATE==strftime('%Y-%m-%d',DATE)),
        CONTENT TEXT NOT NULL,
        MENTIONS TEXT NOT NULL,
        USERUPDATE INT NOT NULL CHECK (USERUPDATE in (0, 1)),
        PRIMARY KEY (ID ASC));""",
    )

    db.execute(
        """CREATE TABLE IF NOT EXISTS SETTINGS
        (SETTING TEXT UNIQUE NOT NULL CHECK (length(SETTING) > 0),
        SVALUE TEXT NOT NULL CHECK (length(SVALUE) > 0),
        PRIMARY KEY (SETTING ASC));"""
    )

    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["HISTORY", "[]"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["COOKIES", "{}"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["FILESFOLDER", "FA.files"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["VERSION", "4.9.0"])

    db.commit()

    return db


def make_database_4_11(db: Connection) -> Connection:
    db.execute(
        f"""CREATE TABLE IF NOT EXISTS USERS
        (USERNAME TEXT UNIQUE NOT NULL CHECK (length(USERNAME) > 0),
        FOLDERS TEXT NOT NULL,
        PRIMARY KEY (USERNAME ASC));"""
    )

    db.execute(
        """CREATE TABLE IF NOT EXISTS SUBMISSIONS
        (ID INT UNIQUE NOT NULL CHECK (ID > 0),
        AUTHOR TEXT NOT NULL CHECK (length(AUTHOR) > 0),
        TITLE TEXT NOT NULL,
        DATE DATE NOT NULL CHECK (DATE==strftime('%Y-%m-%d',DATE)),
        DESCRIPTION TEXT NOT NULL,
        TAGS TEXT NOT NULL,
        CATEGORY TEXT NOT NULL,
        SPECIES TEXT NOT NULL,
        GENDER TEXT NOT NULL,
        RATING TEXT NOT NULL,
        TYPE TEXT NOT NULL CHECK (TYPE IN ('image', 'music', 'text', 'flash')),
        FILEURL TEXT NOT NULL,
        FILEEXT TEXT NOT NULL,
        FILESAVED INT NOT NULL CHECK (FILESAVED in (0, 1, 10, 11)),
        FAVORITE TEXT NOT NULL,
        MENTIONS TEXT NOT NULL,
        FOLDER TEXT NOT NULL CHECK (FOLDER IN ('gallery', 'scraps')),
        USERUPDATE INT NOT NULL CHECK (USERUPDATE in (0, 1)),
        PRIMARY KEY (ID ASC));"""
    )

    db.execute(
        """CREATE TABLE IF NOT EXISTS JOURNALS
        (ID INT UNIQUE NOT NULL CHECK (ID > 0),
        AUTHOR TEXT NOT NULL CHECK (length(AUTHOR) > 0),
        TITLE TEXT NOT NULL,
        DATE DATE NOT NULL CHECK (DATE==strftime('%Y-%m-%d',DATE)),
        CONTENT TEXT NOT NULL,
        MENTIONS TEXT NOT NULL,
        USERUPDATE INT NOT NULL CHECK (USERUPDATE in (0, 1)),
        PRIMARY KEY (ID ASC));""",
    )

    db.execute(
        """CREATE TABLE IF NOT EXISTS SETTINGS
        (SETTING TEXT UNIQUE NOT NULL CHECK (length(SETTING) > 0),
        SVALUE TEXT NOT NULL CHECK (length(SVALUE) > 0),
        PRIMARY KEY (SETTING ASC));"""
    )

    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["HISTORY", "[]"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["COOKIES", "{}"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["FILESFOLDER", "FA.files"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["VERSION", "4.11.0"])

    db.commit()

    return db


def make_database(path: str, make_function: Callable[[Connection], Connection]):
    make_function(connect_database(path)).close()


def update_2_7_to_3(db: Connection, db_path: str, db_new_path: str):
    print("Updating 2.7.0 to 3.0.0")

    make_database(db_new_path, make_database_3)

    # Transferring entries
    print("Transferring entries")
    db.execute(f"ATTACH DATABASE '{db_new_path}' AS db_new")
    db.execute(
        """INSERT OR IGNORE INTO
        db_new.SUBMISSIONS(ID, AUTHOR, UDATE, TITLE, DESCRIPTION, TAGS, CATEGORY, SPECIES, GENDER, RATING, FILELINK)
        SELECT ID, AUTHOR, UDATE, TITLE, DESCRIPTION, TAGS, CATEGORY, SPECIES, GENDER, RATING, FILELINK
        FROM SUBMISSIONS"""
    )
    db.execute(
        """INSERT OR IGNORE INTO
        db_new.USERS(USERNAME, FOLDERS, GALLERY, SCRAPS, FAVORITES, EXTRAS)
        SELECT USER, FOLDERS, GALLERY, SCRAPS, FAVORITES, EXTRAS
        FROM USERS"""
    )

    # Update users folders
    print("Updating users folders")
    user: str
    folders: str
    for n, user, folders in db.execute("SELECT row_number() over (), USERNAME, FOLDERS FROM db_new.USERS"):
        print(n, end="\r", flush=True)
        folders_new: list[str] = []
        for folder in folders.split(","):
            folder_new = ""
            folder_disabled = folder.endswith("!")
            if (folder := folder.strip("!")) == "g":
                folder_new = "gallery"
            elif folder == "s":
                folder_new = "scraps"
            elif folder == "f":
                folder_new = "favorites"
            elif folder == "e":
                folder_new = "extras"
            elif folder == "E":
                folder_new = "Extras"
            folders_new.append(folder_new + ("!" if folder_disabled else ""))
        db.execute("update db_new.USERS set FOLDERS = ? where USERNAME = ?", [",".join(folders_new), user])
        db.commit() if n % 1000 == 0 else None
    print()

    # Update submissions FILEEXT and FILESAVED and move to new location
    print("Updating submissions entries and moving files to new location")
    sub_not_found: list[int] = []
    files_folder_old: str = join(dirname(db_path), "FA.files")
    files_folder_new: str = join(dirname(db_path), "FA.files_new")
    for n, id_, location in db.execute("SELECT row_number() over (), ID, LOCATION FROM SUBMISSIONS"):
        print(n, end="\r", flush=True)
        if isdir(folder_old := join(files_folder_old, location.strip("/"))):
            fileglob = glob(join(folder_old, "submission*"))
            fileext = ""
            if filename := fileglob[0] if fileglob else "":
                makedirs((folder_new := join(files_folder_new, tiered_path(id_))), exist_ok=True)
                copy(filename, join(folder_new, (filename := basename(filename))))
                fileext = filename.split(".")[-1] if "." in filename else ""
            else:
                sub_not_found.append(id_)
            db.execute("update db_new.SUBMISSIONS set FILEEXT = ?, FILESAVED = ? where ID = ?",
                       [fileext, bool(filename), id_])
            rmtree(folder_old)
        elif isdir(join("FA.files_new", tiered_path(id_))):
            # if this block is reached, original folder was removed and database entry updated
            continue
        else:
            sub_not_found.append(id_)
            db.execute("update db_new.SUBMISSIONS set FILEEXT = ?, FILESAVED = ? where ID = ?",
                       ["", False, id_])
        db.commit() if n % 10000 == 0 else None
    db.commit()
    print()
    if sub_not_found:
        print(f"{len(sub_not_found)} submissions not found in FA.files\n" +
              "Writing ID's to FA_update_2_7_to_3.txt")
        with open("FA_update_2_7_to_3.txt", "w") as f:
            for i, sub in enumerate(sorted(sub_not_found)):
                print(i, end="\r", flush=True)
                f.write(str(sub) + "\n")

    # Replace older files folder with new
    print("Replacing older files folder with new")
    if isdir(f := join(dirname(db_path), "FA.files")):
        if not sub_not_found:
            rmtree(f)
        else:
            print("Saving older FA.files to FA.files_old")
            move(f, f + "_old")
    if isdir(f := join(dirname(db_path), "FA.files_new")):
        move(f, join(dirname(db_path), "FA.files"))

    # Update counters for new database
    db.execute("update db_new.SETTINGS set SVALUE = ? where SETTING = 'SUBN'", [str(count(db, "SUBMISSIONS"))])
    db.execute("update db_new.SETTINGS set SVALUE = ? where SETTING = 'USRN'", [str(count(db, "USERS"))])


def update_3_0_to_3_1(db: Connection, db_path: str, db_new_path: str):
    print("Updating 3.0.0 to 3.1.0")

    make_database(db_new_path, make_database_3_1)

    # Transferring entries
    print("Transferring entries")
    db.execute(f"ATTACH DATABASE '{db_new_path}' AS db_new")
    db.execute(
        """INSERT OR IGNORE INTO db_new.SUBMISSIONS
        SELECT * FROM SUBMISSIONS"""
    )
    db.execute(
        """INSERT OR IGNORE INTO db_new.USERS
        SELECT * FROM USERS"""
    )
    db.execute(
        """INSERT OR REPLACE INTO db_new.SETTINGS
        SELECT * FROM SETTINGS WHERE SETTING != "VERSION";"""
    )

    # Update users folders
    print("Updating users folders")
    db.execute("UPDATE db_new.USERS SET FOLDERS = replace(FOLDERS, 'extras', 'mentions')")
    db.execute("UPDATE db_new.USERS SET FOLDERS = replace(FOLDERS, 'Extras', 'mentions_all')")


def update_3_1_to_3_2(db: Connection, db_path: str, db_new_path: str):
    print("Updating 3.1.0 to 3.2.0")

    make_database(db_new_path, make_database_3_2)

    # Transferring entries
    print("Transferring entries")
    db.execute(f"ATTACH DATABASE '{db_new_path}' AS db_new")
    db.execute(
        """INSERT OR IGNORE INTO db_new.SUBMISSIONS
        SELECT * FROM SUBMISSIONS"""
    )
    db.execute(
        """INSERT OR IGNORE INTO db_new.USERS(USERNAME, FOLDERS, GALLERY, SCRAPS, FAVORITES, MENTIONS)
        SELECT USERNAME, FOLDERS, GALLERY, SCRAPS, FAVORITES, MENTIONS FROM USERS"""
    )
    db.execute(
        """INSERT OR REPLACE INTO db_new.SETTINGS
        SELECT * FROM SETTINGS WHERE SETTING != "VERSION";"""
    )
    db.execute("UPDATE db_new.USERS SET JOURNALS = ''")


def update_3_2_to_3_3(db: Connection, db_path: str, db_new_path: str):
    print("Updating 3.2.0 to 3.3.0")

    make_database(db_new_path, make_database_3_3)

    # Transferring entries
    print("Transferring entries")
    db.execute(f"ATTACH DATABASE '{db_new_path}' AS db_new")
    db.execute(
        f"""INSERT OR IGNORE INTO db_new.USERS
        SELECT * FROM USERS"""
    )
    db.execute(
        f"""INSERT OR IGNORE INTO db_new.SUBMISSIONS
        SELECT * FROM SUBMISSIONS"""
    )
    db.execute(
        f"""INSERT OR IGNORE INTO db_new.JOURNALS
        SELECT * FROM JOURNALS"""
    )
    db.execute(
        f"""INSERT OR REPLACE INTO db_new.SETTINGS
        SELECT * FROM SETTINGS WHERE SETTING != "LASTSTART" AND SETTING != "LASTUPDATE" AND SETTING != "VERSION";"""
    )

    # Add update to history
    print("Adding history")
    last_update: str = db.execute("SELECT SVALUE FROM SETTINGS WHERE SETTING = 'LASTUPDATE'").fetchone()
    if last_update and last_update[0] != "0":
        db.execute("UPDATE db_new.SETTINGS SET SVALUE = ? WHERE SETTING = 'HISTORY'",
                   json_dumps([[float(last_update[0]), "update"]]))


def update_3_4_to_3_5(db: Connection, db_path: str, db_new_path: str):
    print("Updating 3.4.0 to 3.5.0")

    make_database(db_new_path, make_database_3_3)

    # Transferring entries
    print("Transferring entries")
    db.execute(f"ATTACH DATABASE '{db_new_path}' AS db_new")
    db.execute(
        f"""INSERT OR IGNORE INTO db_new.USERS
        SELECT * FROM USERS"""
    )
    db.execute(
        f"""INSERT OR IGNORE INTO db_new.SUBMISSIONS
        SELECT * FROM SUBMISSIONS"""
    )
    db.execute(
        f"""INSERT OR IGNORE INTO db_new.JOURNALS
        SELECT * FROM JOURNALS"""
    )
    db.execute(
        f"""INSERT OR REPLACE INTO db_new.SETTINGS
        SELECT * FROM SETTINGS WHERE SETTING != "HISTORY" AND SETTING != "VERSION";"""
    )

    # Update history
    print("Updating history")
    history: list[list[str]] = json_loads(
        db.execute("SELECT SVALUE FROM SETTINGS WHERE SETTING = 'HISTORY'").fetchone()[0]
    )
    history_new: list[tuple[float, str]] = list(map(lambda th: (float(th[0]), th[1]), history))
    db.execute("UPDATE db_new.SETTINGS SET SVALUE = ? WHERE SETTING = 'HISTORY'", [json_dumps(history_new)])
    db.execute("UPDATE db_new.SETTINGS SET SVALUE = ? WHERE SETTING = 'VERSION'", ["3.5.0"])


def update_3_8_to_4(db: Connection, db_path: str, db_new_path: str):
    print("Updating 3.8.0 to 4.0.0")

    make_database(db_new_path, make_database_4)

    # Transferring entries
    print("Transferring entries")
    db.execute(f"ATTACH DATABASE '{db_new_path}' AS db_new")
    db.execute(
        """INSERT OR IGNORE INTO db_new.USERS
        SELECT * FROM USERS"""
    )
    db.execute(
        """INSERT OR IGNORE INTO db_new.SUBMISSIONS
        SELECT * FROM SUBMISSIONS"""
    )
    db.execute(
        """INSERT OR IGNORE INTO db_new.JOURNALS
        SELECT * FROM JOURNALS"""
    )
    db.execute(
        """INSERT OR REPLACE INTO db_new.SETTINGS
        SELECT * FROM SETTINGS WHERE SETTING != 'VERSION';"""
    )


def update_4_2_to_4_3(db: Connection, db_path: str, db_new_path: str):
    print("Updating 4.2.0 to 4.3.0")

    make_database(db_new_path, make_database_4_3)

    # Transferring entries
    print("Transferring entries")
    db.execute(f"ATTACH DATABASE '{db_new_path}' AS db_new")
    db.execute(
        f"""INSERT OR IGNORE INTO db_new.USERS
        SELECT * FROM USERS"""
    )
    db.execute(
        f"""INSERT OR IGNORE INTO db_new.SUBMISSIONS
        SELECT * FROM SUBMISSIONS"""
    )
    db.execute(
        f"""INSERT OR IGNORE INTO db_new.JOURNALS
        SELECT * FROM JOURNALS"""
    )
    db.execute(
        f"""INSERT OR REPLACE INTO db_new.SETTINGS
        SELECT * FROM SETTINGS WHERE SETTING NOT IN ('VERSION', 'JRNN', 'SUBN', 'USRN');"""
    )


def update_4_3_to_4_4(db: Connection, db_path: str, db_new_path: str):
    print("Updating 4.3.0 to 4.4.0")

    make_database(db_new_path, make_database_4_4)

    # Transferring entries
    print("Transferring entries")
    db.execute(f"ATTACH DATABASE '{db_new_path}' AS db_new")
    db.execute(
        """INSERT OR IGNORE INTO db_new.USERS
        SELECT * FROM USERS"""
    )
    db.execute(
        """INSERT OR IGNORE INTO db_new.SUBMISSIONS
        SELECT *,'' FROM SUBMISSIONS"""
    )
    db.execute(
        """INSERT OR IGNORE INTO db_new.JOURNALS
        SELECT * FROM JOURNALS"""
    )
    db.execute(
        """INSERT OR REPLACE INTO db_new.SETTINGS
        SELECT * FROM SETTINGS WHERE SETTING NOT IN ('VERSION');"""
    )

    print("Updating favorites from users entries")
    for n, u, fs in db.execute("select row_number() over (), USERNAME, FAVORITES from db_new.USERS where FAVORITES != ''"):
        print(n, end="\r", flush=True)
        for f in map(int, filter(bool, fs.split(","))):
            f_us: Optional[tuple] = db.execute(f"select FAVORITE from db_new.SUBMISSIONS where ID = {f}").fetchone()
            if not f_us:
                continue
            f_us_new: str = ",".join({u, *filter(bool, f_us[0].split(","))})
            if f_us_new == f_us:
                continue
            db.execute(f"update db_new.SUBMISSIONS set FAVORITE = ? where ID = {f}", [f_us_new])


def update_4_4_to_4_5(db: Connection, db_path: str, db_new_path: str):
    print("Updating 4.4.0 to 4.5.0")

    make_database(db_new_path, make_database_4_5)

    # Transferring entries
    print("Transferring entries")
    db.execute(f"ATTACH DATABASE '{db_new_path}' AS db_new")
    db.execute(
        """INSERT OR IGNORE INTO db_new.USERS
        SELECT USERNAME,FOLDERS FROM USERS"""
    )
    db.execute(
        """INSERT OR IGNORE INTO db_new.SUBMISSIONS
        SELECT *,'','gallery' FROM SUBMISSIONS"""
    )
    db.execute(
        """INSERT OR IGNORE INTO db_new.JOURNALS
        SELECT * FROM JOURNALS"""
    )
    db.execute(
        """INSERT OR REPLACE INTO db_new.SETTINGS
        SELECT * FROM SETTINGS WHERE SETTING NOT IN ('VERSION');"""
    )

    print("Parsing submissions mentions")
    mentions_exp: Pattern = re_compile(r'href *= *"(?:(?:https?://)?(?:www.)?furaffinity.net)?/user/([^/"#]+)[^"]*"')
    for n, i, d in db.execute("select row_number() over (), ID, DESCRIPTION from db_new.SUBMISSIONS").fetchall():
        print(n, end="\r", flush=True)
        mentions: list[str] = sorted(set(filter(bool, map(clean_username, findall(mentions_exp, d)))))
        if mentions:
            db.execute("update db_new.SUBMISSIONS set MENTIONS = ? where ID = ?", (",".join(mentions), i))

    print("Updating folders from users entries")
    missing_mentions: list[tuple[str, int]] = []
    double_folders: list[tuple[str, int]] = []
    for n, user, g, s, m in db.execute("select row_number() over (), USERNAME,GALLERY,SCRAPS,MENTIONS from USERS"):
        print(n, end="\r", flush=True)
        g_set: set[int] = set(map(int, filter(bool, g.split(","))))
        s_set: set[int] = set(map(int, filter(bool, s.split(","))))
        double_folders.extend((user, i) for i in g_set.intersection(s_set))
        for i in g_set:
            db.execute("update db_new.SUBMISSIONS set FOLDER = ? where ID = ?", ("gallery", i))
        for i in s_set:
            db.execute("update db_new.SUBMISSIONS set FOLDER = ? where ID = ?", ("scraps", i))
        for i in map(int, filter(bool, m.split(","))):
            ms: Optional[tuple] = db.execute("select MENTIONS from db_new.SUBMISSIONS where ID = ?",
                                             (i,)).fetchone()
            if not ms or user not in ms[0].split(","):
                missing_mentions.append((user, i))

    if missing_mentions:
        print("Missing submissions:", len(missing_mentions), "FA_v4_5_missing_mentions.txt")
        missing_mentions.sort(key=lambda m_: (m_[0], m_[1]))
        with open(join(dirname(db_path), "FA_v4_5_missing_mentions.txt"), "w") as f:
            f.write("\n".join(f"{u} {s}" for u, s in missing_mentions))
    if double_folders:
        print("Double folders:", len(double_folders), "FA_v4_5_double_folders.txt")
        double_folders.sort(key=lambda m_: (m_[0], m_[1]))
        with open(join(dirname(db_path), "FA_v4_5_double_folders.txt"), "w") as f:
            f.write("\n".join(f"{u} {s}" for u, s in double_folders))


def update_4_5_to_4_6(db: Connection, db_path: str, db_new_path: str):
    print("Updating 4.5.0 to 4.6.0")

    make_database(db_new_path, make_database_4_6)

    # Transferring entries
    print("Transferring entries")
    db.execute(f"ATTACH DATABASE '{db_new_path}' AS db_new")
    db.execute(
        """INSERT OR IGNORE INTO db_new.USERS
        SELECT USERNAME,FOLDERS FROM USERS"""
    )
    db.execute(
        """INSERT OR IGNORE INTO db_new.SUBMISSIONS
        SELECT *,
            (SELECT 1 FROM USERS
            WHERE USERNAME = lower(replace(AUTHOR, '_', '')) AND instr(FOLDERS, FOLDER)
            ) IS NOT NULL
        FROM SUBMISSIONS"""
    )
    db.execute(
        """INSERT OR IGNORE INTO db_new.JOURNALS
        SELECT *,
            (SELECT 1 FROM USERS
            WHERE USERNAME = lower(replace(AUTHOR, '_', '')) AND instr(lower(FOLDERS), 'journals')
            ) IS NOT NULL
        FROM JOURNALS"""
    )
    db.execute(
        """INSERT OR REPLACE INTO db_new.SETTINGS
        SELECT * FROM SETTINGS WHERE SETTING NOT IN ('VERSION');"""
    )


def update_4_6_to_4_7(db: Connection, db_path: str, db_new_path: str):
    print("Updating 4.6.0 to 4.7.0")

    make_database(db_new_path, make_database_4_7)

    # Transferring entries
    print("Transferring entries")
    db.execute(f"ATTACH DATABASE '{db_new_path}' AS db_new")
    db.execute(
        """INSERT OR IGNORE INTO db_new.USERS
        SELECT * FROM USERS"""
    )
    db.execute(
        """INSERT OR IGNORE INTO db_new.SUBMISSIONS
        SELECT * FROM SUBMISSIONS"""
    )
    db.execute(
        """INSERT OR IGNORE INTO db_new.JOURNALS
        SELECT ID, AUTHOR, TITLE, DATE, CONTENT, '', USERUPDATE FROM JOURNALS"""
    )
    db.execute(
        """INSERT OR REPLACE INTO db_new.SETTINGS
        SELECT * FROM SETTINGS WHERE SETTING NOT IN ('VERSION');"""
    )

    print("Parsing journals mentions")
    mentions_exp: Pattern = re_compile(r'<a[^>]*href="(?:(?:https?://)?(?:www.)?furaffinity.net)?/user/([^/">]+)"')
    for n, i, c in db.execute("select row_number() over (), ID, CONTENT from db_new.JOURNALS").fetchall():
        print(n, end="\r", flush=True)
        mentions: list[str] = sorted(set(filter(bool, map(clean_username, findall(mentions_exp, c)))))
        if mentions:
            db.execute("update db_new.JOURNALS set MENTIONS = ? where ID = ?", (",".join(mentions), i))


def update_4_7_to_4_8(db: Connection, db_path: str, db_new_path: str):
    print("Updating 4.7.0 to 4.8.0")

    make_database(db_new_path, make_database_4_8)

    # Transferring entries
    print("Transferring entries")
    db.execute(f"ATTACH DATABASE '{db_new_path}' AS db_new")
    db.execute(
        """INSERT OR IGNORE INTO db_new.USERS
        SELECT * FROM USERS"""
    )
    db.execute(
        """INSERT OR IGNORE INTO db_new.SUBMISSIONS
        SELECT * FROM SUBMISSIONS"""
    )
    db.execute(
        """INSERT OR IGNORE INTO db_new.JOURNALS
        SELECT * FROM JOURNALS"""
    )
    db.execute(
        """INSERT OR REPLACE INTO db_new.SETTINGS
        SELECT * FROM SETTINGS WHERE SETTING NOT IN ('VERSION')"""
    )

    print("Updating users entries")
    for n, u, fs in db.execute("select row_number() over (), USERNAME, FOLDERS from USERS"):
        print(n, end="\r", flush=True)
        fs = "".join(sorted((f"|{f}|" for f in fs.split(",") if f), key=str.lower))
        db.execute("update db_new.USERS set FOLDERS = ? where USERNAME = ?", (fs, u))
    print("Updating submissions entries")
    for n, i, ts, fs, ms in db.execute("select row_number() over (), ID, TAGS, FAVORITE, MENTIONS from SUBMISSIONS"):
        print(n, end="\r", flush=True)
        if not ts and not fs and not ms:
            continue
        ts = "".join(sorted((f"|{t}|" for t in ts.split(",") if t), key=str.lower))
        fs = "".join(sorted((f"|{f}|" for f in fs.split(",") if f), key=str.lower))
        ms = "".join(sorted((f"|{m}|" for m in ms.split(",") if m), key=str.lower))
        db.execute("update db_new.SUBMISSIONS set TAGS = ?, FAVORITE = ?, MENTIONS = ? where ID = ?",
                   (ts, fs, ms, i))
    for i, ms in db.execute("select ID, MENTIONS from JOURNALS where MENTIONS != ''"):
        if not ms:
            continue
        ms = "".join(sorted((f"|{m}|" for m in ms.split(",") if m), key=str.lower))
        db.execute("update db_new.JOURNALS set MENTIONS = ? where ID = ?", (ms, i))


def update_4_8_to_4_9(db: Connection, db_path: str, db_new_path: str):
    print("Updating 4.8.0 to 4.9.0")

    make_database(db_new_path, make_database_4_9)

    # Transferring entries
    print("Transferring entries")
    db.execute(f"ATTACH DATABASE '{db_new_path}' AS db_new")
    db.execute(
        """INSERT OR IGNORE INTO db_new.USERS
        SELECT * FROM USERS"""
    )
    db.execute(
        """INSERT OR IGNORE INTO db_new.SUBMISSIONS
        SELECT ID, AUTHOR, TITLE, DATE, DESCRIPTION, TAGS, CATEGORY, 
        SPECIES, GENDER, RATING, 'image', FILEURL, FILEEXT, FILESAVED, 
        FAVORITE, MENTIONS, FOLDER, USERUPDATE FROM SUBMISSIONS"""
    )
    db.execute(
        """INSERT OR IGNORE INTO db_new.JOURNALS
        SELECT * FROM JOURNALS"""
    )
    db.execute(
        """INSERT OR REPLACE INTO db_new.SETTINGS
        SELECT * FROM SETTINGS WHERE SETTING NOT IN ('VERSION')"""
    )

    print("Updating submissions types")
    unknown_extensions: list[tuple[int, str]] = []
    blank_extensions: list[tuple[int, str]] = []
    files_folder: str = join(
        dirname(db_path),
        db.execute("select SVALUE from SETTINGS where SETTING = 'FILESFOLDER'").fetchone()[0]
    )
    for n, i, e, s in db.execute("select row_number() over (), ID, FILEEXT, FILESAVED from SUBMISSIONS"):
        print(n, end="\r", flush=True)
        if (e := e.lower()) in ("jpg", "jpeg", "png", "gif", "tif", "tiff", "bmp"):
            pass
        elif e in ("mp3", "wav", "mid", "midi"):
            db.execute("update db_new.SUBMISSIONS set TYPE = ? where ID = ?", ("music", i))
        elif e in ("txt", "doc", "docx", "odt", "rtf", "pdf"):
            db.execute("update db_new.SUBMISSIONS set TYPE = ? where ID = ?", ("text", i))
        elif e == "swf":
            db.execute("update db_new.SUBMISSIONS set TYPE = ? where ID = ?", ("flash", i))
        elif e:
            print(f"Unknown extension: {i} '{e}'")
            unknown_extensions.append((i, e))
        elif not e and s:
            print(f"Blank extension: {i} {(p := tiered_path(i))}")
            blank_extensions.append((i, p))
            if isfile(sp := join(files_folder, p, "submission.")):
                copy(sp, sp.rstrip("."))
                remove(sp)

    if unknown_extensions:
        print("Unknown extensions:", len(unknown_extensions), "FA_v4_9_unknown_extensions.txt")
        unknown_extensions.sort(key=lambda e_: e_[0])
        with open(join(dirname(db_path), "FA_v4_9_unknown_extensions.txt"), "w") as f:
            f.write("\n".join(f"{i} {e}" for i, e in unknown_extensions))
    if blank_extensions:
        print("Blank extensions:", len(blank_extensions), "FA_v4_9_blank_extensions.txt")
        print("  Blank extensions files renamed to 'submission'")
        blank_extensions.sort(key=lambda e_: e_[0])
        with open(join(dirname(db_path), "FA_v4_9_blank_extensions.txt"), "w") as f:
            f.write("\n".join(f"{i} {e}" for i, e in blank_extensions))


def update_4_10_4_11(db: Connection, db_path: str, db_new_path: str):
    print("Updating 4.10.0 to 4.11.0")

    make_database(db_new_path, make_database_4_11)

    # Transferring entries
    print("Transferring entries")
    db.execute(f"ATTACH DATABASE '{db_new_path}' AS db_new")
    db.execute("INSERT OR IGNORE INTO db_new.USERS SELECT * FROM USERS")
    db.execute("INSERT OR IGNORE INTO db_new.SUBMISSIONS SELECT * FROM SUBMISSIONS")
    db.execute("INSERT OR IGNORE INTO db_new.JOURNALS SELECT * FROM JOURNALS")
    db.execute("INSERT OR REPLACE INTO db_new.SETTINGS SELECT * FROM SETTINGS WHERE SETTING NOT IN ('VERSION')")

    print("Checking submission thumbnails")
    files_folder: str = join(
        dirname(db_path),
        db.execute("select SVALUE from SETTINGS where SETTING = 'FILESFOLDER'").fetchone()[0]
    )
    for n, i, f in db.execute("SELECT row_number() OVER (ORDER BY ID), ID, FILESAVED FROM SUBMISSIONS ORDER BY ID"):
        print(n, end="\r", flush=True)
        f *= 10
        f += isfile(join(files_folder, tiered_path(i), "thumbnail.jpg"))
        db.execute("UPDATE db_new.SUBMISSIONS SET FILESAVED = ? WHERE ID = ?", (f, i))


def update_wrapper(db: Connection, function: Callable[[Connection, str, str], None], prefix_old: str) -> Connection:
    db_path: str = dp if (dp := database_path(db)) else "FA.db"
    db_new_path: str = join(dirname(db_path), "new_" + basename(db_path))
    try:
        function(db, db_path, db_new_path)
        db.commit()
        db.close()
        db = None
        move(db_path, join(dirname(db_path), prefix_old + basename(db_path)))
        move(db_new_path, db_path)
        return connect_database(db_path)
    except KeyboardInterrupt:
        print("Database update interrupted")
        raise
    except (BaseException, Exception) as err:
        print("Database update error")
        print("  " + repr(err))
        raise err
    finally:
        if db is not None:
            db.commit()
            db.close()


def update_version(db: Connection, version: str, target_version: str) -> Connection:
    print(f"Updating {version} to {target_version}")
    db.execute("UPDATE SETTINGS SET SVALUE = ? WHERE SETTING = 'VERSION'", [target_version])
    db.commit()
    return db


def update_database(db: Connection, version: str) -> Connection:
    if not (db_version := get_version(db)):
        raise DatabaseError("Cannot read version from database.")
    elif (v := compare_versions(db_version, version)) == 0:
        return db
    elif v > 0:
        raise DatabaseError("Database version is newer than program.")
    elif compare_versions(db_version, "2.7.0") < 0:
        raise DatabaseError("Update does not support versions lower than 2.11.2")
    elif compare_versions(db_version, "3.0.0") < 0:
        db = update_wrapper(db, update_2_7_to_3, "v2_7_")  # 2.7.x to 3.0.0
    elif compare_versions(db_version, "3.1.0") < 0:
        db = update_wrapper(db, update_3_0_to_3_1, "v3_0_")  # 3.0.x to 3.1.0
    elif compare_versions(db_version, "3.2.0") < 0:
        db = update_wrapper(db, update_3_1_to_3_2, "v3_1_")  # 3.1.x to 3.2.0
    elif compare_versions(db_version, "3.3.0") < 0:
        db = update_wrapper(db, update_3_2_to_3_3, "v3_2_")  # 3.2.x to 3.3.0
    elif compare_versions(db_version, "3.4.0") < 0:
        db = update_version(db, db_version, "3.4.0")  # 3.3.x to 3.4.0
    elif compare_versions(db_version, "3.5.0") < 0:
        db = update_wrapper(db, update_3_4_to_3_5, "v3_4_")  # 3.4.x to 3.5.0
    elif compare_versions(db_version, "3.8.0") < 0:
        db = update_version(db, db_version, "3.8.0")  # 3.5.0-3.7.x to 3.8.0
    elif compare_versions(db_version, "4.0.0") < 0:
        db = update_wrapper(db, update_3_8_to_4, "v3_8_")  # 3.8.x to 4.0.0
    elif compare_versions(db_version, "4.3.0") < 0:
        db = update_wrapper(db, update_4_2_to_4_3, "v4_2_")  # 4.0.0-4.2.x to 4.3.0
    elif compare_versions(db_version, "4.4.0") < 0:
        db = update_wrapper(db, update_4_3_to_4_4, "v4_3_")  # 4.3.x to 4.4.0
    elif compare_versions(db_version, "4.5.0") < 0:
        db = update_wrapper(db, update_4_4_to_4_5, "v4_4_")  # 4.4.x to 4.5.0
    elif compare_versions(db_version, "4.6.0") < 0:
        db = update_wrapper(db, update_4_5_to_4_6, "v4_5_")  # 4.5.x to 4.6.0
    elif compare_versions(db_version, "4.7.0") < 0:
        db = update_wrapper(db, update_4_6_to_4_7, "v4_6_")  # 4.6.x to 4.7.0
    elif compare_versions(db_version, "4.8.0") < 0:
        db = update_wrapper(db, update_4_7_to_4_8, "v4_7_")  # 4.7.x to 4.8.0
    elif compare_versions(db_version, "4.9.0") < 0:
        db = update_wrapper(db, update_4_8_to_4_9, "v4_8_")  # 4.8.x to 4.9.0
    elif compare_versions(db_version, "4.10.0") < 0:
        db = update_version(db, db_version, "4.10.0")  # 4.9.x to 4.10.0
    elif compare_versions(db_version, "4.11.0") < 0:
        db = update_wrapper(db, update_4_10_4_11, "v4_10_")  # 4.10.x to 4.11.0
    elif compare_versions(db_version, version) < 0:
        return update_version(db, db_version, version)  # Update to latest patch

    return update_database(db, version)
