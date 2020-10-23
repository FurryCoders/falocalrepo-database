from sqlite3 import Connection
from typing import Dict
from typing import List

"""
Entries guide - SUBMISSIONS
v3.2            v4.0
0  ID           ID
1  AUTHOR       AUTHOR
2  TITLE        TITLE
3  UDATE        DATE
4  DESCRIPTION  DESCRIPTION
5  TAGS         TAGS
6  CATEGORY     CATEGORY
7  SPECIES      SPECIES
8  GENDER       GENDER
9  RATING       RATING
10 FILELINK     FILELINK
11 FILEEXT      FILEEXT
12 FILESAVED    FILESAVED
13 LOCATION     LOCATION
14 SERVER       SERVER
"""

submissions_table: str = "SUBMISSIONS"
submissions_fields: List[str] = [
    "ID", "AUTHOR", "TITLE",
    "DATE", "DESCRIPTION", "TAGS",
    "CATEGORY", "SPECIES", "GENDER",
    "RATING", "FILELINK", "FILEEXT",
    "FILESAVED"
]
submissions_indexes: Dict[str, int] = {f: i for i, f in enumerate(submissions_fields)}


def make_submissions_table(db: Connection):
    db.execute(
        f"""CREATE TABLE IF NOT EXISTS {submissions_table}
        (ID INT UNIQUE NOT NULL,
        AUTHOR TEXT NOT NULL,
        TITLE TEXT,
        DATE DATE NOT NULL,
        DESCRIPTION TEXT,
        TAGS TEXT,
        CATEGORY TEXT,
        SPECIES TEXT,
        GENDER TEXT,
        RATING TEXT,
        FILELINK TEXT,
        FILEEXT TEXT,
        FILESAVED INT,
        PRIMARY KEY (ID ASC));"""
    )


def submissions_table_errors(db: Connection) -> List[tuple]:
    errors: List[tuple] = []
    errors.extend(db.execute("SELECT * FROM SUBMISSIONS WHERE ID = 0").fetchall())
    errors.extend(db.execute("SELECT * FROM SUBMISSIONS WHERE AUTHOR = ''").fetchall())
    errors.extend(db.execute("SELECT * FROM SUBMISSIONS WHERE UDATE = ''").fetchall())
    errors.extend(db.execute("SELECT * FROM SUBMISSIONS WHERE FILELINK = ''").fetchall())
    errors.extend(db.execute("SELECT * FROM SUBMISSIONS WHERE FILESAVED NOT IN (0, 1) ").fetchall())
    errors.extend(db.execute(
        f"SELECT * FROM SUBMISSIONS WHERE {' OR '.join(f'{f} = null' for f in submissions_fields)}").fetchall())

    return sorted(set(errors), key=lambda s: s[0])
