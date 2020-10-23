from typing import Dict
from typing import List

from .database import Connection

"""
Entries guide - USERS
v3.0        v3.1      v3.2
0 USERNAME  USERNAME  USERNAME
1 FOLDERS   FOLDERS   FOLDERS
2 GALLERY   GALLERY   GALLERY
3 SCRAPS    SCRAPS    SCRAPS
4 FAVORITES FAVORITES FAVORITES
5 EXTRAS    MENTIONS  MENTIONS
6                     JOURNALS
"""

users_table: str = "USERS"
users_fields: List[str] = [
    "USERNAME", "FOLDERS",
    "GALLERY", "SCRAPS",
    "FAVORITES", "MENTIONS",
    "JOURNALS"
]
users_indexes: Dict[str, int] = {f: i for i, f in enumerate(users_fields)}


def make_users_table(db: Connection):
    db.execute(
        f"""CREATE TABLE IF NOT EXISTS {users_table}
        (USERNAME TEXT UNIQUE NOT NULL,
        FOLDERS TEXT NOT NULL,
        GALLERY TEXT,
        SCRAPS TEXT,
        FAVORITES TEXT,
        MENTIONS TEXT,
        JOURNALS TEXT,
        PRIMARY KEY (USERNAME ASC));"""
    )


def users_table_errors(db: Connection):
    errors: List[tuple] = []
    errors.extend(db.execute("SELECT * FROM USERS WHERE USERNAME = ''").fetchall())
    errors.extend(db.execute(
        """SELECT * FROM USERS WHERE FOLDERS = '' AND
        (GALLERY != '' OR FAVORITES != '' OR MENTIONS != '' OR JOURNALS != '')"""
    ).fetchall())
    errors.extend(db.execute(f"SELECT * FROM USERS WHERE {' OR '.join(f'{f} = null' for f in users_fields)}"))

    return sorted(set(errors), key=lambda s: s[0])
