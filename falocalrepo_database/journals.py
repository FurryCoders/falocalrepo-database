from sqlite3 import Connection

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
