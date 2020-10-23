from sqlite3 import Connection

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


def make_submissions_table(db: Connection):
    db.execute(
        f"""CREATE TABLE IF NOT EXISTS {submissions_table}
        (ID INT UNIQUE NOT NULL CHECK (ID > 0),
        AUTHOR TEXT NOT NULL CHECK (length(AUTHOR) > 0),
        TITLE TEXT NOT NULL,
        DATE DATE NOT NULL CHECK (DATE==strftime('%Y-%m-%d',DATE)),
        DESCRIPTION TEXT NOT NULL,
        TAGS TEXT NOT NULL,
        CATEGORY TEXT NOT NULL,
        SPECIES TEXT NOT NULL,
        GENDER TEXT NOT NULL,
        RATING TEXT NOT NULL,
        FILELINK TEXT NOT NULL,
        FILEEXT TEXT NOT NULL,
        FILESAVED INT NOT NULL CHECK (FILESAVED in (0, 1)),
        PRIMARY KEY (ID ASC));"""
    )
