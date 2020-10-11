from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

from .database import Connection
from .database import Cursor
from .database import delete
from .database import insert
from .database import select
from .database import update

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


def exist_user(db: Connection, user: str) -> bool:
    return bool(select(db, users_table, ["USERNAME"], ["USERNAME"], [user]).fetchone())


def exist_user_field_value(db: Connection, user: str, field: str, value: str) -> bool:
    value_: Optional[tuple] = select(db, users_table, [field], ["USERNAME"], [user]).fetchone()
    if value_ is None:
        return False
    elif value in value_[0].split(","):
        return True

    return False


def new_user(db: Connection, user: str):
    insert(db, users_table, users_fields, [user] + [""] * (len(users_fields) - 1), False)
    db.commit()


def remove_user(db: Connection, user: str):
    delete(db, users_table, "USERNAME", user)
    db.commit()


def edit_user_field_replace(db: Connection, user: str, fields: List[str], new_values: List[str]):
    update(db, users_table, fields, new_values, "USERNAME", user)
    db.commit()


def edit_user_field_add(db: Connection, user: str, field: str, values: List[Union[str, int]]):
    old_values_raw: Optional[Tuple[str]] = select(db, users_table, [field], ["USERNAME"], [user]).fetchone()
    if old_values_raw is None:
        return

    old_values: List[str] = old_values_raw[0].split(",") if old_values_raw[0] else []
    values = list(set(old_values + list(map(str, values))))

    edit_user_field_replace(db, user, [field], [",".join(values)])


def edit_user_field_remove(db: Connection, user: str, field: str, values: List[Union[str, int]]):
    old_values_raw: Optional[Tuple[str]] = select(db, users_table, [field], ["USERNAME"], [user]).fetchone()
    if old_values_raw is None:
        return

    old_values: List[str] = old_values_raw[0].split(",") if old_values_raw[0] else []
    values = list(map(str, values))
    values = [v for v in old_values if v not in values]

    if values == old_values:
        return

    edit_user_field_replace(db, user, [field], [",".join(values)])


def edit_user_remove_submission(db: Connection, user: str, sub: int):
    sub_format: str = str(sub).zfill(10)
    edit_user_field_remove(db, user, "GALLERY", [sub_format])
    edit_user_field_remove(db, user, "SCRAPS", [sub_format])
    edit_user_field_remove(db, user, "FAVORITES", [sub_format])
    edit_user_field_remove(db, user, "MENTIONS", [sub_format])


def edit_user_remove_journal(db: Connection, user: str, jrn: int):
    edit_user_field_remove(db, user, "JOURNALS", [str(jrn).zfill(10)])


def find_user_from_fields(db: Connection, fields: List[str], values: List[str], and_: bool = False) -> Cursor:
    return select(db, users_table, ["*"], fields, values, True, and_, ["USERNAME"])


def find_user_from_galleries(db: Connection, submission_id: int) -> Cursor:
    return find_user_from_fields(db, ["GALLERY", "SCRAPS"], [f"%{int(submission_id):010}%"] * 2)


def find_user_from_submission(db: Connection, submission_id: int) -> Cursor:
    return find_user_from_fields(
        db,
        ["GALLERY", "SCRAPS", "FAVORITES", "MENTIONS"],
        [f"%{int(submission_id):010}%"] * 4
    )


def find_user_from_journal(db: Connection, journal_id: int) -> Cursor:
    return find_user_from_fields(db, ["JOURNALS"], [f"%{int(journal_id):010}%"])


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

    assert any((username, folders, gallery, scraps, favorites, mentions))

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
