from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from .database import Connection
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
    return bool(select(db, users_table, ["USERNAME"], "USERNAME", user).fetchone())


def exist_user_field_value(db: Connection, user: str, field: str, value: str) -> bool:
    value_: Optional[tuple] = select(db, users_table, [field], "USERNAME", user).fetchone()
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


def edit_user_field_add(db: Connection, user: str, field: str, values: List[str]):
    old_values_raw: Optional[Tuple[str]] = select(db, users_table, [field], "USERNAME", user).fetchone()
    if old_values_raw is None:
        return

    values = old_values_raw[0].split(",") + values
    values.sort()

    edit_user_field_replace(db, user, [field], [",".join(values)])


def edit_user_field_remove(db: Connection, user: str, field: str, values: List[str]):
    old_values_raw: Optional[Tuple[str]] = select(db, users_table, [field], "USERNAME", user).fetchone()
    if old_values_raw is None:
        return

    values = [v for v in old_values_raw[0].split(",") if v not in values]

    edit_user_field_replace(db, user, [field], [",".join(values)])
