from sqlite3 import Connection
from typing import Dict
from typing import List

"""
Entries guide - JOURNALS
v3.2
0 ID
1 AUTHOR
2 TITLE
3 UDATE
4 CONTENT
"""

journals_table: str = "JOURNALS"
journals_fields: List[str] = [
    "ID", "AUTHOR", "TITLE",
    "UDATE", "CONTENT"
]
journals_indexes: Dict[str, int] = {f: i for i, f in enumerate(journals_fields)}


def make_journals_table(db: Connection):
    db.execute(
        f"""CREATE TABLE IF NOT EXISTS {journals_table}
        (ID INT UNIQUE NOT NULL,
        AUTHOR TEXT NOT NULL,
        TITLE TEXT,
        UDATE DATE NOT NULL,
        CONTENT TEXT,
        PRIMARY KEY (ID ASC));"""
    )


def journals_table_errors(db: Connection):
    errors: List[tuple] = []
    errors.extend(db.execute("SELECT * FROM JOURNALS WHERE ID = 0").fetchall())
    errors.extend(db.execute("SELECT * FROM JOURNALS WHERE AUTHOR = ''").fetchall())
    errors.extend(db.execute("SELECT * FROM JOURNALS WHERE UDATE = ''").fetchall())
    errors.extend(db.execute(f"SELECT * FROM JOURNALS WHERE {' OR '.join(f'{f} = null' for f in journals_fields)}"))

    return sorted(set(errors), key=lambda s: s[0])
