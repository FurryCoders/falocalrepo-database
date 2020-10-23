from sqlite3 import Connection

from .__version__ import __version__

settings_table: str = "SETTINGS"


def make_settings_table(db: Connection):
    db.execute(
        f"""CREATE TABLE IF NOT EXISTS {settings_table}
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
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["VERSION", __version__])
