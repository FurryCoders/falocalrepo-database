from typing import Optional

from .__version__ import __version__
from .database import Connection
from .database import insert
from .database import select
from .database import update

settings_table: str = "SETTINGS"


def write_setting(db: Connection, key: str, value: str):
    update(db, settings_table, ["SVALUE"], [value], "SETTING", key)
    db.commit()


def read_setting(db: Connection, key: str) -> Optional[str]:
    setting = select(db, settings_table, ["SVALUE"], "SETTING", key).fetchone()

    return None if not setting else setting[0]


def make_settings_table(db: Connection):
    db.execute(
        f"""CREATE TABLE IF NOT EXISTS {settings_table}
        (SETTING TEXT UNIQUE,
        SVALUE TEXT,
        PRIMARY KEY (SETTING ASC));"""
    )

    # Add settings
    insert(db, settings_table, ["SETTING", "SVALUE"], ["USRN", "0"], False)
    insert(db, settings_table, ["SETTING", "SVALUE"], ["SUBN", "0"], False)
    insert(db, settings_table, ["SETTING", "SVALUE"], ["JRNN", "0"], False)
    insert(db, settings_table, ["SETTING", "SVALUE"], ["HISTORY", "[]"], False)
    insert(db, settings_table, ["SETTING", "SVALUE"], ["COOKIES", "{}"], False)
    insert(db, settings_table, ["SETTING", "SVALUE"], ["FILESFOLDER", "FA.files"], False)
    insert(db, settings_table, ["SETTING", "SVALUE"], ["VERSION", __version__], False)
