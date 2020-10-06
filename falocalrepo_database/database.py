from os.path import join as join_path
from sqlite3 import Connection
from sqlite3 import Cursor
from sqlite3 import connect
from typing import List
from typing import Union


def tiered_path(id_: Union[int, str], depth: int = 5, width: int = 2) -> str:
    assert isinstance(id_, int) or (isinstance(id_, str) and id_.isdigit())
    assert isinstance(depth, int) and depth > 0
    assert isinstance(width, int) and width > 0

    id_str: str = str(int(id_)).zfill(depth * width)
    return join_path(*[id_str[n:n + width] for n in range(0, depth * width, width)])


def connect_database(path: str) -> Connection:
    return connect(path)


def insert(db: Connection, table: str, keys: List[str], values: List[Union[int, str]], replace: bool = True):
    db.execute(
        f"""INSERT OR {"REPLACE" if replace else "IGNORE"} INTO {table}
        ({",".join(keys)})
        VALUES ({",".join(["?"] * len(values))})""",
        values
    )


def select(db: Connection, table: str, fields: List[str], keys: List[str], key_values: List[Union[int, str]],
           like: bool = False, and_: bool = True, order: List[str] = None
           ) -> Cursor:
    op: str = "like" if like else "="
    logic: str = "AND" if and_ else "OR"
    where: List[str] = [f"{key} {op} ?" for key in keys]
    order = [] if order is None else order
    return db.execute(
        f'''SELECT {",".join(fields)} FROM {table}
        WHERE {f" {logic} ".join(where)}
        {"ORDER BY " + ','.join(order) if order else ""}''',
        key_values
    )


def select_all(db: Connection, table: str, fields: List[str], order: List[str] = None) -> Cursor:
    order = [] if order is None else order
    return db.execute(f'''SELECT {",".join(fields)} FROM {table} {"ORDER BY " + ','.join(order) if order else ""}''')


def update(db: Connection, table: str, fields: List[str], values: List[Union[int, str]], key: str, key_value: str):
    assert len(fields) == len(values) and len(fields) > 0

    update_values: List[str] = [f"{u} = ?" for u in fields]

    db.execute(
        f"""UPDATE {table}
        SET {",".join(update_values)}
        WHERE {key} = ?""",
        (*values, key_value,)
    )


def delete(db: Connection, table: str, key: str, key_value: Union[int, str]):
    db.execute(f"""DELETE FROM {table} where {key} = ?""", (key_value,))


def count(db: Connection, table: str) -> int:
    return db.execute(f"SELECT COUNT(*) FROM {table}").fetchall()[0][0]


def vacuum(db: Connection):
    db.execute("VACUUM")


def check_errors(db: Connection, table: str) -> List[tuple]:
    if (table := table.upper()) in ("SUBMISSIONS", "JOURNALS"):
        errors: List[tuple] = []
        errors.extend(select(db, table, ["*"], "ID", 0).fetchall())
        errors.extend(select(db, table, ["*"], "AUTHOR", "").fetchall())
        errors.extend(select(db, table, ["*"], "TITLE", "").fetchall())
        errors.extend(select(db, table, ["*"], "UDATE", "").fetchall())

        if table == "SUBMISSIONS":
            errors.extend(select(db, table, ["*"], "FILELINK", "").fetchall())
            errors.extend(select(db, table, ["*"], "FILESAVED", "").fetchall())

        return sorted(set(errors), key=lambda s: s[0])
    else:
        return []


def make_tables(db: Connection):
    from .journals import make_journals_table
    from .settings import make_settings_table
    from .submissions import make_submissions_table
    from .users import make_users_table

    make_journals_table(db)
    make_settings_table(db)
    make_submissions_table(db)
    make_users_table(db)

    db.commit()
