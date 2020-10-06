from typing import Dict
from typing import List
from typing import Union

from .database import Connection
from .database import delete
from .database import insert
from .database import select

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


def exist_journal(db: Connection, journal_id: int) -> bool:
    return bool(select(db, journals_table, ["ID"], ["ID"], [journal_id]).fetchone())


def save_journal(db: Connection, journal: Dict[str, Union[str, int]]):
    insert(db, journals_table, journals_fields,
           [journal["id"], journal["author"], journal["title"], journal["date"], journal["content"]])
    db.commit()


def remove_journal(db: Connection, journal_id: int):
    delete(db, journals_table, "ID", journal_id)
    db.commit()


def search_journals(db: Connection,
                    limit: List[Union[str, int]] = None, offset: List[Union[str, int]] = None,
                    order: List[str] = None, author: List[str] = None, title: List[str] = None,
                    date: List[str] = None, content: List[str] = None
                    ) -> List[tuple]:
    order = [] if order is None else order
    author = [] if author is None else list(map(str.lower, author))
    title = [] if title is None else list(map(str.lower, title))
    date = [] if date is None else list(map(str.lower, date))
    content = [] if content is None else list(map(str.lower, content))

    assert any((author, title, date, content))

    wheres: List[str] = [
        " OR ".join(["UDATE like ?"] * len(date)),
        " OR ".join(['replace(lower(AUTHOR), "_", "") like ?'] * len(author)),
        " OR ".join(["lower(TITLE) like ?"] * len(title)),
        " OR ".join(["lower(content) like ?"] * len(content))
    ]

    wheres_str = " AND ".join(map(lambda p: "(" + p + ")", filter(len, wheres)))
    order_str: str = f"ORDER BY {','.join(order)}" if order else ""
    limit_str: str = f"LIMIT {int(limit[0])}" if limit is not None else ""
    offset_str: str = f"OFFSET {int(offset[0])}" if offset is not None else ""

    return db.execute(
        f"""SELECT * FROM {journals_table} WHERE {wheres_str} {order_str} {limit_str} {offset_str}""",
        date + author + title + content
    ).fetchall()
