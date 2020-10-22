from typing import Dict
from typing import List
from typing import Union

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
        (GALLERY != '' OR SCRAPS != '' OR FAVORITES != '' OR MENTIONS != '' OR JOURNALS != ''"""
    ).fetchall())
    errors.extend(db.execute(f"SELECT * FROM USERS WHERE {' OR '.join(f'{f} = null' for f in users_fields)}"))

    return sorted(set(errors), key=lambda s: s[0])


def search_users(db: Connection, username: List[str] = None, folders: List[str] = None, gallery: List[str] = None,
                 scraps: List[str] = None, favorites: List[str] = None, mentions: List[str] = None,
                 limit: List[Union[str, int]] = None, offset: List[Union[str, int]] = None, order: List[str] = None,
                 ) -> List[tuple]:
    username = [] if username is None else username
    folders = [] if folders is None else folders
    gallery = [] if gallery is None else gallery
    scraps = [] if scraps is None else scraps
    favorites = [] if favorites is None else favorites
    mentions = [] if mentions is None else mentions

    assert any((username, folders, gallery, scraps, favorites, mentions)), "at least one parameter needed"

    wheres: List[str] = [
        " OR ".join(['replace(lower(USERNAME), "_", "") like ?'] * len(username)),
        " OR ".join(["lower(FOLDERS) like ?"] * len(folders)),
        " OR ".join(["GALLERY like ?"] * len(gallery)),
        " OR ".join(["SCRAPS like ?"] * len(scraps)),
        " OR ".join(["FAVORITES like ?"] * len(favorites)),
        " OR ".join(["MENTIONS like ?"] * len(mentions))
    ]

    wheres_str: str = " AND ".join(map(lambda p: "(" + p + ")", filter(len, wheres)))
    order_str: str = f"ORDER BY {','.join(order)}" if order else ""
    limit_str: str = f"LIMIT {int(limit[0])}" if limit is not None else ""
    offset_str: str = f"OFFSET {int(offset[0])}" if offset is not None else ""

    return db.execute(
        f"""SELECT * FROM {users_table} WHERE {wheres_str} {order_str} {limit_str} {offset_str}""",
        username + folders + gallery + scraps + favorites + mentions
    ).fetchall()
