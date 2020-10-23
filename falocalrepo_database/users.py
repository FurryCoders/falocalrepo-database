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
        (USERNAME TEXT UNIQUE NOT NULL CHECK (length(USERNAME) > 0),
        FOLDERS TEXT NOT NULL,
        GALLERY TEXT NOT NULL,
        SCRAPS TEXT NOT NULL,
        FAVORITES TEXT NOT NULL,
        MENTIONS TEXT NOT NULL,
        JOURNALS TEXT NOT NULL,
        PRIMARY KEY (USERNAME ASC));"""
    )
