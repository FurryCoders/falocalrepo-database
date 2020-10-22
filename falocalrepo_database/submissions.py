from sqlite3 import Connection
from typing import Dict
from typing import List
from typing import Union

"""
Entries guide - SUBMISSIONS
v3.2
0  ID
1  AUTHOR
2  TITLE
3  UDATE
4  DESCRIPTION
5  TAGS
6  CATEGORY
7  SPECIES
8  GENDER
9  RATING
10 FILELINK
11 FILEEXT
12 FILESAVED
13 LOCATION
14 SERVER
"""

submissions_table: str = "SUBMISSIONS"
submissions_fields: List[str] = [
    "ID", "AUTHOR", "TITLE",
    "UDATE", "DESCRIPTION", "TAGS",
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
        UDATE DATE NOT NULL,
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


def search_submissions(db: Connection,
                       author: List[str] = None, title: List[str] = None,
                       date: List[str] = None, description: List[str] = None, tags: List[str] = None,
                       category: List[str] = None, species: List[str] = None, gender: List[str] = None,
                       rating: List[str] = None, limit: List[Union[str, int]] = None,
                       offset: List[Union[str, int]] = None, order: List[str] = None
                       ) -> List[tuple]:
    author = [] if author is None else list(map(str.lower, author))
    title = [] if title is None else list(map(str.lower, title))
    date = [] if date is None else list(map(str.lower, date))
    description = [] if description is None else list(map(str.lower, description))
    tags = [] if tags is None else list(map(str.lower, tags))
    category = [] if category is None else list(map(str.lower, category))
    species = [] if species is None else list(map(str.lower, species))
    gender = [] if gender is None else list(map(str.lower, gender))
    rating = [] if rating is None else list(map(str.lower, rating))

    assert any(
        (author, title, date, description, tags, category, species, gender, rating)), "at least one parameter needed"

    wheres: List[str] = [
        " OR ".join(["UDATE like ?"] * len(date)),
        " OR ".join(["lower(RATING) like ?"] * len(rating)),
        " OR ".join(["lower(GENDER) like ?"] * len(gender)),
        " OR ".join(["lower(SPECIES) like ?"] * len(species)),
        " OR ".join(["lower(CATEGORY) like ?"] * len(category)),
        " OR ".join(['replace(lower(AUTHOR), "_", "") like ?'] * len(author)),
        " OR ".join(["lower(TITLE) like ?"] * len(title)),
        " OR ".join(["lower(TAGS) like ?"] * len(tags)),
        " OR ".join(["lower(DESCRIPTION) like ?"] * len(description))
    ]

    wheres_str: str = " AND ".join(map(lambda p: "(" + p + ")", filter(len, wheres)))
    order_str: str = f"ORDER BY {','.join(order)}" if order else ""
    limit_str: str = f"LIMIT {int(limit[0])}" if limit is not None else ""
    offset_str: str = f"OFFSET {int(offset[0])}" if offset is not None else ""

    return db.execute(
        f"""SELECT * FROM {submissions_table} WHERE {wheres_str} {order_str} {limit_str} {offset_str}""",
        date + rating + gender + species + category + author + title + tags + description
    ).fetchall()
