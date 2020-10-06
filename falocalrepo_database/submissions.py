from os import makedirs
from os.path import join as join_path
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from filetype import guess_extension

from .database import Connection
from .database import delete
from .database import insert
from .database import select
from .database import tiered_path
from .settings import read_setting

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


def exist_submission(db: Connection, submission_id: int) -> bool:
    return bool(select(db, submissions_table, ["ID"], ["ID"], [submission_id]).fetchone())


def remove_submission(db: Connection, submission_id: int):
    delete(db, submissions_table, "ID", submission_id)
    db.commit()


def save_submission(db: Connection, submission: Dict[str, Union[str, int]], file: Optional[bytes]):
    file_ext: str = ""
    file_url_name: str = submission["file_url"].split("/")[-1]
    file_url_ext: str = file_url_name.split(".")[-1] if "." in file_url_name else ""

    if file:
        if (file_ext_tmp := guess_extension(file)) is None:
            file_ext = file_url_ext
        elif (file_ext := str(file_ext_tmp)) == "zip" and file_url_ext:
            file_ext = file_url_ext

        sub_folder: str = join_path(read_setting(db, "FILESFOLDER"), tiered_path(submission["id"]))
        sub_file: str = "submission" + (f".{file_ext}" if file_ext else "")

        makedirs(sub_folder, exist_ok=True)

        with open(join_path(sub_folder, sub_file), "wb") as f:
            f.write(file)

    insert(db, submissions_table,
           list(submissions_indexes.keys()),
           [submission["id"], submission["author"], submission["title"],
            submission["date"], submission["description"], ",".join(sorted(submission["tags"], key=str.lower)),
            submission["category"], submission["species"], submission["gender"],
            submission["rating"], submission["file_url"], file_ext,
            file is not None],
           replace=True)

    db.commit()


def search_submissions(db: Connection,
                       limit: List[Union[str, int]] = None, offset: List[Union[str, int]] = None,
                       order: List[str] = None, author: List[str] = None, title: List[str] = None,
                       date: List[str] = None, description: List[str] = None, tags: List[str] = None,
                       category: List[str] = None, species: List[str] = None, gender: List[str] = None,
                       rating: List[str] = None
                       ) -> List[tuple]:
    order = [] if order is None else order
    author = [] if author is None else list(map(str.lower, author))
    title = [] if title is None else list(map(str.lower, title))
    date = [] if date is None else list(map(str.lower, date))
    description = [] if description is None else list(map(str.lower, description))
    tags = [] if tags is None else list(map(str.lower, tags))
    category = [] if category is None else list(map(str.lower, category))
    species = [] if species is None else list(map(str.lower, species))
    gender = [] if gender is None else list(map(str.lower, gender))
    rating = [] if rating is None else list(map(str.lower, rating))

    assert any((author, title, date, description, tags, category, species, gender, rating))

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
