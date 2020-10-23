from sqlite3 import Connection
from typing import Dict
from typing import List

"""
Entries guide - JOURNALS
v3.2        v4.0
0 ID        ID
1 AUTHOR    AUTHOR
2 TITLE     TITLE
3 UDATE     DATE
4 CONTENT   CONTENT
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
        (ID INT UNIQUE NOT NULL CHECK (ID > 0),
        AUTHOR TEXT NOT NULL CHECK (length(AUTHOR) > 0),
        TITLE TEXT NOT NULL,
        DATE DATE NOT NULL CHECK (DATE==strftime('%Y-%m-%d',DATE)),
        CONTENT TEXT NOT NULL,
        PRIMARY KEY (ID ASC));"""
    )


def journals_table_errors(db: Connection):
    errors: List[tuple] = []
    errors.extend(db.execute("SELECT * FROM JOURNALS WHERE ID = 0").fetchall())
    errors.extend(db.execute(f"SELECT * FROM JOURNALS WHERE {' OR '.join(f'{f} = null' for f in journals_fields)}"))

    return sorted(set(errors), key=lambda s: s[0])
