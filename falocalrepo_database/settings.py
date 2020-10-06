from json import dumps as json_dumps
from json import loads as json_loads
from typing import List
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
    setting = select(db, settings_table, ["SVALUE"], ["SETTING"], [key]).fetchone()

    return None if not setting else setting[0]


def read_history(db: Connection) -> List[List[str]]:
    return sorted(json_loads(read_setting(db, "HISTORY")), key=lambda h: h[0])


def add_history(db: Connection, time: float, command: str):
    history: List[List[str]] = [[str(time), command], *read_history(db)]
    write_setting(db, "HISTORY", json_dumps(sorted(history, key=lambda h: h[0])))


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
