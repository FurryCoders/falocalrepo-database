from json import dumps
from sqlite3 import Connection

from .__version__ import __version__

journals_table: str = "JOURNALS"
settings_table: str = "SETTINGS"
submissions_table: str = "SUBMISSIONS"
users_table: str = "USERS"

list_columns: dict[str, list[str]] = {
    journals_table: ["MENTIONS"],
    settings_table: [],
    submissions_table: ["TAGS", "FAVORITE", "MENTIONS"],
    users_table: ["FOLDERS"],
}


def make_journals_table(db: Connection):
    db.execute(
        f"""CREATE TABLE IF NOT EXISTS {journals_table}
        (ID INT UNIQUE NOT NULL CHECK (ID > 0),
        AUTHOR TEXT NOT NULL CHECK (length(AUTHOR) > 0),
        TITLE TEXT NOT NULL,
        DATE DATE NOT NULL CHECK (DATE==strftime('%Y-%m-%d',DATE)),
        CONTENT TEXT NOT NULL,
        MENTIONS TEXT NOT NULL,
        USERUPDATE INT NOT NULL CHECK (USERUPDATE in (0, 1)),
        PRIMARY KEY (ID ASC));""",
    )

    db.commit()


def make_settings_table(db: Connection):
    db.execute(
        f"""CREATE TABLE IF NOT EXISTS {settings_table}
        (SETTING TEXT UNIQUE NOT NULL CHECK (length(SETTING) > 0),
        SVALUE TEXT NOT NULL CHECK (length(SVALUE) > 0),
        PRIMARY KEY (SETTING ASC));"""
    )

    db.execute(f"INSERT OR IGNORE INTO {settings_table} VALUES ('HISTORY', ?);", (dumps([]),))
    db.execute(f"INSERT OR IGNORE INTO {settings_table} VALUES ('COOKIES', ?);", (dumps({}),))
    db.execute(f"INSERT OR IGNORE INTO {settings_table} VALUES ('FILESFOLDER', ?);", ("FA.files",))
    db.execute(f"INSERT OR IGNORE INTO {settings_table} VALUES ('VERSION', ?);", (__version__,))

    db.commit()


def make_submissions_table(db: Connection):
    db.execute(
        f"""CREATE TABLE IF NOT EXISTS {submissions_table}
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

    db.commit()


def make_users_table(db: Connection):
    db.execute(
        f"""CREATE TABLE IF NOT EXISTS {users_table}
        (USERNAME TEXT UNIQUE NOT NULL CHECK (length(USERNAME) > 0),
        FOLDERS TEXT NOT NULL,
        PRIMARY KEY (USERNAME ASC));"""
    )

    db.commit()
