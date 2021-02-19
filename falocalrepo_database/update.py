from glob import glob
from json import dumps as json_dumps
from json import loads as json_loads
from os import makedirs
from os.path import basename
from os.path import dirname
from os.path import isdir
from os.path import join as path_join
from re import Pattern
from re import compile as re_compile
from re import findall
from re import sub
from shutil import copy
from shutil import move
from shutil import rmtree
from sqlite3 import Connection
from sqlite3 import DatabaseError
from sqlite3 import OperationalError
from sqlite3 import connect as connect_database
from typing import Collection
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union


def get_version(db: Connection) -> str:
    try:
        # Database version 3.0.0 and above
        return db.execute(f"SELECT SVALUE FROM SETTINGS WHERE SETTING = 'VERSION'").fetchone()[0]
    except OperationalError:
        # Database version 2.7.0
        return db.execute(f"SELECT VALUE FROM SETTINGS WHERE FIELD = 'VERSION'").fetchone()[0]


def compare_versions(a: str, b: str) -> int:
    a_split = list(map(int, a.split("-", 1)[0].split(".")))
    b_split = list(map(int, b.split("-", 1)[0].split(".")))
    a_split.extend([0] * (3 - len(a_split)))
    b_split.extend([0] * (3 - len(b_split)))

    for a_, b_ in zip(a_split, b_split):
        if a_ > b_:
            return 1
        elif a_ < b_:
            return -1
    return 0


def insert(db: Connection, table: str, keys: Collection[str], values: Collection[Union[int, str]],
           replace: bool = True):
    db.execute(
        f"""INSERT OR {"REPLACE" if replace else "IGNORE"} INTO {table}
        ({",".join(keys)})
        VALUES ({",".join(["?"] * len(values))})""",
        values
    )


def update(db: Connection, table: str, fields: Collection[str], values: Collection[Union[int, str]], key: str,
           key_value: str):
    assert len(fields) == len(values) and len(fields) > 0

    update_values: List[str] = [f"{u} = ?" for u in fields]

    db.execute(
        f"""UPDATE {table}
        SET {",".join(update_values)}
        WHERE {key} = ?""",
        (*values, key_value,)
    )


def count(db: Connection, table: str) -> int:
    return db.execute(f"SELECT COUNT(*) FROM {table}").fetchall()[0][0]


def database_path(db: Connection) -> Optional[str]:
    name: str
    filename: Optional[str]
    for _, name, filename in db.execute("PRAGMA database_list"):
        if name == "main" and filename is not None:
            return filename

    return None


def clean_username(user: str) -> str:
    return sub(r"[^a-z0-9.~-]", "", user.lower())


def make_database_3(db: Connection):
    # Create submissions table
    db.execute(
        """CREATE TABLE IF NOT EXISTS SUBMISSIONS
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

    # Create users table
    db.execute(
        """CREATE TABLE IF NOT EXISTS USERS
        (USERNAME TEXT UNIQUE NOT NULL,
        FOLDERS TEXT NOT NULL,
        GALLERY TEXT,
        SCRAPS TEXT,
        FAVORITES TEXT,
        EXTRAS TEXT,
        PRIMARY KEY (USERNAME ASC));"""
    )

    # Create settings table
    db.execute(
        """CREATE TABLE IF NOT EXISTS SETTINGS
        (SETTING TEXT UNIQUE,
        SVALUE TEXT,
        PRIMARY KEY (SETTING ASC));"""
    )

    db.commit()

    # Add settings
    insert(db, "SETTINGS", ["SETTING", "SVALUE"], ["USRN", "0"], False)
    insert(db, "SETTINGS", ["SETTING", "SVALUE"], ["SUBN", "0"], False)
    insert(db, "SETTINGS", ["SETTING", "SVALUE"], ["LASTUPDATE", "0"], False)
    insert(db, "SETTINGS", ["SETTING", "SVALUE"], ["LASTSTART", "0"], False)
    insert(db, "SETTINGS", ["SETTING", "SVALUE"], ["COOKIES", "{}"], False)
    insert(db, "SETTINGS", ["SETTING", "SVALUE"], ["FILESFOLDER", "FA.files"], False)
    insert(db, "SETTINGS", ["SETTING", "SVALUE"], ["VERSION", "3.0.0"], False)

    db.commit()


def make_database_3_1(db: Connection):
    # Create submissions table
    db.execute(
        """CREATE TABLE IF NOT EXISTS SUBMISSIONS
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

    # Create users table
    db.execute(
        """CREATE TABLE IF NOT EXISTS USERS
        (USERNAME TEXT UNIQUE NOT NULL,
        FOLDERS TEXT NOT NULL,
        GALLERY TEXT,
        SCRAPS TEXT,
        FAVORITES TEXT,
        MENTIONS TEXT,
        PRIMARY KEY (USERNAME ASC));"""
    )

    # Create settings table
    db.execute(
        """CREATE TABLE IF NOT EXISTS SETTINGS
        (SETTING TEXT UNIQUE,
        SVALUE TEXT,
        PRIMARY KEY (SETTING ASC));"""
    )

    db.commit()

    # Add settings
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["USRN", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["SUBN", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["JRNN", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["LASTUPDATE", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["LASTSTART", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["COOKIES", "{}"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["FILESFOLDER", "FA.files"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["VERSION", "3.1.0"])

    db.commit()


def make_database_3_2(db: Connection):
    db.execute(
        f"""CREATE TABLE IF NOT EXISTS USERS
        (USERNAME TEXT UNIQUE NOT NULL,
        FOLDERS TEXT NOT NULL,
        GALLERY TEXT,
        SCRAPS TEXT,
        FAVORITES TEXT,
        MENTIONS TEXT,
        JOURNALS TEXT,
        PRIMARY KEY (USERNAME ASC));"""
    )

    db.execute(
        f"""CREATE TABLE IF NOT EXISTS SUBMISSIONS
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

    db.execute(
        f"""CREATE TABLE IF NOT EXISTS JOURNALS
        (ID INT UNIQUE NOT NULL,
        AUTHOR TEXT NOT NULL,
        TITLE TEXT,
        UDATE DATE NOT NULL,
        CONTENT TEXT,
        PRIMARY KEY (ID ASC));"""
    )

    db.execute(
        f"""CREATE TABLE IF NOT EXISTS SETTINGS
        (SETTING TEXT UNIQUE,
        SVALUE TEXT,
        PRIMARY KEY (SETTING ASC));"""
    )

    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["USRN", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["SUBN", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["JRNN", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["LASTUPDATE", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["LASTSTART", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["COOKIES", "{}"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["FILESFOLDER", "FA.files"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["VERSION", "3.2.0"])

    db.commit()


def make_database_3_3(db: Connection):
    db.execute(
        f"""CREATE TABLE IF NOT EXISTS USERS
        (USERNAME TEXT UNIQUE NOT NULL,
        FOLDERS TEXT NOT NULL,
        GALLERY TEXT,
        SCRAPS TEXT,
        FAVORITES TEXT,
        MENTIONS TEXT,
        JOURNALS TEXT,
        PRIMARY KEY (USERNAME ASC));"""
    )

    db.execute(
        f"""CREATE TABLE IF NOT EXISTS SUBMISSIONS
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

    db.execute(
        f"""CREATE TABLE IF NOT EXISTS JOURNALS
        (ID INT UNIQUE NOT NULL,
        AUTHOR TEXT NOT NULL,
        TITLE TEXT,
        UDATE DATE NOT NULL,
        CONTENT TEXT,
        PRIMARY KEY (ID ASC));"""
    )

    db.execute(
        f"""CREATE TABLE IF NOT EXISTS SETTINGS
        (SETTING TEXT UNIQUE,
        SVALUE TEXT,
        PRIMARY KEY (SETTING ASC));"""
    )

    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["USRN", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["SUBN", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["JRNN", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["HISTORY", "[]"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["COOKIES", "{}"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["FILESFOLDER", "FA.files"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["VERSION", "3.3.0"])

    db.commit()


def make_database_4(db: Connection):
    db.execute(
        f"""CREATE TABLE IF NOT EXISTS USERS
        (USERNAME TEXT UNIQUE NOT NULL CHECK (length(USERNAME) > 0),
        FOLDERS TEXT NOT NULL,
        GALLERY TEXT NOT NULL,
        SCRAPS TEXT NOT NULL,
        FAVORITES TEXT NOT NULL,
        MENTIONS TEXT NOT NULL,
        JOURNALS TEXT NOT NULL,
        PRIMARY KEY (USERNAME ASC));"""
    )

    db.execute(
        f"""CREATE TABLE IF NOT EXISTS SUBMISSIONS
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

    db.execute(
        f"""CREATE TABLE IF NOT EXISTS JOURNALS
        (ID INT UNIQUE NOT NULL CHECK (ID > 0),
        AUTHOR TEXT NOT NULL CHECK (length(AUTHOR) > 0),
        TITLE TEXT NOT NULL,
        DATE DATE NOT NULL CHECK (DATE==strftime('%Y-%m-%d',DATE)),
        CONTENT TEXT NOT NULL,
        PRIMARY KEY (ID ASC));"""
    )

    db.execute(
        f"""CREATE TABLE IF NOT EXISTS SETTINGS
        (SETTING TEXT UNIQUE NOT NULL CHECK (length(SETTING) > 0),
        SVALUE TEXT NOT NULL CHECK (length(SVALUE) > 0),
        PRIMARY KEY (SETTING ASC));"""
    )

    # Add settings
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["USRN", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["SUBN", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["JRNN", "0"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["HISTORY", "[]"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["COOKIES", "{}"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["FILESFOLDER", "FA.files"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["VERSION", "4.0.0"])

    db.commit()


def make_database_4_3(db: Connection):
    db.execute(
        f"""CREATE TABLE IF NOT EXISTS USERS
        (USERNAME TEXT UNIQUE NOT NULL CHECK (length(USERNAME) > 0),
        FOLDERS TEXT NOT NULL,
        GALLERY TEXT NOT NULL,
        SCRAPS TEXT NOT NULL,
        FAVORITES TEXT NOT NULL,
        MENTIONS TEXT NOT NULL,
        JOURNALS TEXT NOT NULL,
        PRIMARY KEY (USERNAME ASC));"""
    )

    db.execute(
        f"""CREATE TABLE IF NOT EXISTS SUBMISSIONS
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

    db.execute(
        f"""CREATE TABLE IF NOT EXISTS JOURNALS
        (ID INT UNIQUE NOT NULL CHECK (ID > 0),
        AUTHOR TEXT NOT NULL CHECK (length(AUTHOR) > 0),
        TITLE TEXT NOT NULL,
        DATE DATE NOT NULL CHECK (DATE==strftime('%Y-%m-%d',DATE)),
        CONTENT TEXT NOT NULL,
        PRIMARY KEY (ID ASC));"""
    )

    db.execute(
        f"""CREATE TABLE IF NOT EXISTS SETTINGS
        (SETTING TEXT UNIQUE NOT NULL CHECK (length(SETTING) > 0),
        SVALUE TEXT NOT NULL CHECK (length(SVALUE) > 0),
        PRIMARY KEY (SETTING ASC));"""
    )

    # Add settings
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["HISTORY", "[]"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["COOKIES", "{}"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["FILESFOLDER", "FA.files"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["VERSION", "4.3.0"])

    db.commit()


def make_database_4_4(db: Connection):
    db.execute(
        f"""CREATE TABLE IF NOT EXISTS USERS
        (USERNAME TEXT UNIQUE NOT NULL CHECK (length(USERNAME) > 0),
        FOLDERS TEXT NOT NULL,
        GALLERY TEXT NOT NULL,
        SCRAPS TEXT NOT NULL,
        FAVORITES TEXT NOT NULL,
        MENTIONS TEXT NOT NULL,
        JOURNALS TEXT NOT NULL,
        PRIMARY KEY (USERNAME ASC));"""
    )

    db.execute(
        f"""CREATE TABLE IF NOT EXISTS SUBMISSIONS
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
        FAVORITE TEXT NOT NULL,
        PRIMARY KEY (ID ASC));"""
    )

    db.execute(
        f"""CREATE TABLE IF NOT EXISTS JOURNALS
        (ID INT UNIQUE NOT NULL CHECK (ID > 0),
        AUTHOR TEXT NOT NULL CHECK (length(AUTHOR) > 0),
        TITLE TEXT NOT NULL,
        DATE DATE NOT NULL CHECK (DATE==strftime('%Y-%m-%d',DATE)),
        CONTENT TEXT NOT NULL,
        PRIMARY KEY (ID ASC));"""
    )

    db.execute(
        f"""CREATE TABLE IF NOT EXISTS SETTINGS
        (SETTING TEXT UNIQUE NOT NULL CHECK (length(SETTING) > 0),
        SVALUE TEXT NOT NULL CHECK (length(SVALUE) > 0),
        PRIMARY KEY (SETTING ASC));"""
    )

    # Add settings
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["HISTORY", "[]"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["COOKIES", "{}"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["FILESFOLDER", "FA.files"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["VERSION", "4.4.0"])

    db.commit()


def make_database_4_5(db: Connection):
    db.execute(
        f"""CREATE TABLE IF NOT EXISTS USERS
        (USERNAME TEXT UNIQUE NOT NULL CHECK (length(USERNAME) > 0),
        FOLDERS TEXT NOT NULL,
        PRIMARY KEY (USERNAME ASC));"""
    )

    db.execute(
        """CREATE TABLE IF NOT EXISTS SUBMISSIONS
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
        FAVORITE TEXT NOT NULL,
        MENTIONS TEXT NOT NULL,
        FOLDER TEXT NOT NULL CHECK (FOLDER = 'gallery' OR FOLDER = 'scraps'),
        PRIMARY KEY (ID ASC));"""
    )

    db.execute(
        """CREATE TABLE IF NOT EXISTS JOURNALS
        (ID INT UNIQUE NOT NULL CHECK (ID > 0),
        AUTHOR TEXT NOT NULL CHECK (length(AUTHOR) > 0),
        TITLE TEXT NOT NULL,
        DATE DATE NOT NULL CHECK (DATE==strftime('%Y-%m-%d',DATE)),
        CONTENT TEXT NOT NULL,
        PRIMARY KEY (ID ASC));""",
    )

    db.execute(
        """CREATE TABLE IF NOT EXISTS SETTINGS
        (SETTING TEXT UNIQUE NOT NULL CHECK (length(SETTING) > 0),
        SVALUE TEXT NOT NULL CHECK (length(SVALUE) > 0),
        PRIMARY KEY (SETTING ASC));"""
    )

    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["HISTORY", "[]"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["COOKIES", "{}"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["FILESFOLDER", "FA.files"])
    db.execute(f"INSERT OR IGNORE INTO SETTINGS (SETTING, SVALUE) VALUES (?, ?)", ["VERSION", "4.5.0"])

    db.commit()


def update_2_7_to_3(db: Connection) -> Connection:
    from .database import tiered_path

    print("Updating 2.7.0 to 3.0.0")
    db_new: Optional[Connection] = None

    db_path: str = dp if (dp := database_path(db)) else "FA.db"
    db_new_path: str = path_join(dirname(db_path), "new_" + basename(db_path))

    try:
        db_new = connect_database(db_new_path)
        make_database_3(db_new)

        # Transfer common submissions and users data
        print("Transfer common submissions and users data")
        db.execute(f"ATTACH DATABASE '{db_new_path}' AS db_new")
        db.execute(
            """INSERT OR IGNORE INTO
            db_new.SUBMISSIONS(ID, AUTHOR, UDATE, TITLE, DESCRIPTION, TAGS, CATEGORY, SPECIES, GENDER, RATING, FILELINK)
            SELECT ID, AUTHOR, UDATE, TITLE, DESCRIPTION, TAGS, CATEGORY, SPECIES, GENDER, RATING, FILELINK
            FROM SUBMISSIONS"""
        )
        db.execute(
            """INSERT OR IGNORE INTO
            db_new.USERS(USERNAME, FOLDERS, GALLERY, SCRAPS, FAVORITES, EXTRAS)
            SELECT USER, FOLDERS, GALLERY, SCRAPS, FAVORITES, EXTRAS
            FROM USERS"""
        )

        db.commit()
        db_new.commit()

        # Update users folders
        print("Update users folders")
        user: str
        folders: str
        user_n: int = 0
        for user, folders in db_new.execute("SELECT USERNAME, FOLDERS FROM USERS"):
            user_n += 1
            print(user_n, end="\r", flush=True)
            folders_new: List[str] = []
            for folder in folders.split(","):
                folder_new = ""
                folder_disabled = folder.endswith("!")
                if (folder := folder.strip("!")) == "g":
                    folder_new = "gallery"
                elif folder == "s":
                    folder_new = "scraps"
                elif folder == "f":
                    folder_new = "favorites"
                elif folder == "e":
                    folder_new = "extras"
                elif folder == "E":
                    folder_new = "Extras"
                folders_new.append(folder_new + ("!" if folder_disabled else ""))
            update(db_new, "USERS", ["FOLDERS"], [",".join(folders_new)], "USERNAME", user)
            db_new.commit() if user_n % 1000 == 0 else None
        db_new.commit()
        print()

        # Update submissions FILEEXT and FILESAVED and move to new location
        print("Update submissions FILEEXT and FILESAVED and move to new location")
        sub_n: int = 0
        sub_not_found: List[int] = []
        files_folder_old: str = path_join(dirname(db_path), "FA.files")
        files_folder_new: str = path_join(dirname(db_path), "FA.files_new")
        for id_, location in db.execute("SELECT ID, LOCATION FROM SUBMISSIONS"):
            sub_n += 1
            print(sub_n, end="\r", flush=True)
            if isdir(folder_old := path_join(files_folder_old, location.strip("/"))):
                fileglob = glob(path_join(folder_old, "submission*"))
                fileext = ""
                if filename := fileglob[0] if fileglob else "":
                    makedirs((folder_new := path_join(files_folder_new, tiered_path(id_))), exist_ok=True)
                    copy(filename, path_join(folder_new, (filename := basename(filename))))
                    fileext = filename.split(".")[-1] if "." in filename else ""
                else:
                    sub_not_found.append(id_)
                update(db_new, "SUBMISSIONS", ["FILEEXT", "FILESAVED"], [fileext, bool(filename)], "ID", id_)
                rmtree(folder_old)
            elif isdir(path_join("FA.files_new", tiered_path(id_))):
                # if this block is reached, original folder was removed and database entry updated
                continue
            else:
                sub_not_found.append(id_)
                update(db_new, "SUBMISSIONS", ["FILEEXT", "FILESAVED"], ["", False], "ID", id_)
            db_new.commit() if sub_n % 10000 == 0 else None
        db_new.commit()
        print()
        if sub_not_found:
            print(f"{len(sub_not_found)} submissions not found in FA.files\n" +
                  "Writing ID's to FA_update_2_7_to_3.txt")
            with open("FA_update_2_7_to_3.txt", "w") as f:
                for i, sub in enumerate(sorted(sub_not_found)):
                    print(i, end="\r", flush=True)
                    f.write(str(sub) + "\n")

        # Replace older files folder with new
        print("Replace older files folder with new")
        if isdir(f := path_join(dirname(db_path), "FA.files")):
            if not sub_not_found:
                rmtree(f)
            else:
                print("Saving older FA.files to FA.files_old")
                move(f, f + "_old")
        if isdir(f := path_join(dirname(db_path), "FA.files_new")):
            move(f, path_join(dirname(db_path), "FA.files"))

        # Update counters for new database
        update(db, "SETTINGS", ["SVALUE"], [str(count(db_new, "SUBMISSIONS"))], "SETTING", "SUBN")
        update(db, "SETTINGS", ["SVALUE"], [str(count(db_new, "USERS"))], "SETTING", "USRN")

        # Close databases and replace old database
        print("Close databases and replace old database")
        db.commit()
        db.close()
        db_new.commit()
        db_new.close()
        move(db_path, path_join(dirname(db_path), "v2_7_" + basename(db_path)))
        move(db_new_path, db_path)
    except (BaseException, Exception) as err:
        print("Database update interrupted!")
        db.commit()
        db.close()
        if db_new is not None:
            db_new.commit()
            db_new.close()
        raise err

    return connect_database(db_path)


def update_3_to_3_1(db: Connection) -> Connection:
    print("Updating 3.0.0 to 3.1.0")
    db_new: Optional[Connection] = None

    db_path: str = dp if (dp := database_path(db)) else "FA.db"
    db_new_path: str = path_join(dirname(db_path), "new_" + basename(db_path))

    try:
        db_new = connect_database(db_new_path)
        make_database_3_1(db_new)

        # Transfer common submissions and users data
        print("Transfer common submissions and users data")
        db.execute(f"ATTACH DATABASE '{db_new_path}' AS db_new")
        db.execute(
            """INSERT OR IGNORE INTO db_new.SUBMISSIONS
            SELECT * FROM SUBMISSIONS"""
        )
        db.execute(
            """INSERT OR IGNORE INTO db_new.USERS
            SELECT * FROM USERS"""
        )
        db.execute(
            """INSERT OR REPLACE INTO db_new.SETTINGS
            SELECT * FROM SETTINGS WHERE SETTING != "VERSION";"""
        )

        db.commit()
        db_new.commit()

        # Update users folders
        print("Update user folders")
        db_new.execute("UPDATE USERS SET FOLDERS = replace(FOLDERS, 'extras', 'mentions')")
        db_new.execute("UPDATE USERS SET FOLDERS = replace(FOLDERS, 'Extras', 'mentions_all')")

        # Close databases and replace old database
        print("Close databases and replace old database")
        db.commit()
        db.close()
        db_new.commit()
        db_new.close()
        move(db_path, path_join(dirname(db_path), "v3_" + basename(db_path)))
        move(db_new_path, db_path)
    except (BaseException, Exception) as err:
        print("Database update interrupted!")
        db.commit()
        db.close()
        if db_new is not None:
            db_new.commit()
            db_new.close()
        raise err

    return connect_database(db_path)


def update_3_1_to_3_2(db: Connection) -> Connection:
    print("Updating 3.1.0 to 3.2.0")
    db_new: Optional[Connection] = None

    db_path: str = dp if (dp := database_path(db)) else "FA.db"
    db_new_path: str = path_join(dirname(db_path), "new_" + basename(db_path))

    try:
        db_new = connect_database(db_new_path)
        make_database_3_2(db_new)

        # Transfer common submissions and users data
        print("Transfer common submissions and users data")
        db.execute(f"ATTACH DATABASE '{db_new_path}' AS db_new")
        db.execute(
            """INSERT OR IGNORE INTO db_new.SUBMISSIONS
            SELECT * FROM SUBMISSIONS"""
        )
        db.execute(
            """INSERT OR IGNORE INTO db_new.USERS(USERNAME, FOLDERS, GALLERY, SCRAPS, FAVORITES, MENTIONS)
        SELECT USERNAME, FOLDERS, GALLERY, SCRAPS, FAVORITES, MENTIONS FROM USERS"""
        )
        db.execute(
            """INSERT OR REPLACE INTO db_new.SETTINGS
            SELECT * FROM SETTINGS WHERE SETTING != "VERSION";"""
        )
        db.execute("UPDATE db_new.USERS SET JOURNALS = ''")

        db.commit()
        db_new.commit()

        # Close databases and replace old database
        print("Close databases and replace old database")
        db.commit()
        db.close()
        db_new.commit()
        db_new.close()
        move(db_path, path_join(dirname(db_path), "v3_1_" + basename(db_path)))
        move(db_new_path, db_path)
    except (BaseException, Exception) as err:
        print("Database update interrupted!")
        db.commit()
        db.close()
        if db_new is not None:
            db_new.commit()
            db_new.close()
        raise err

    return connect_database(db_path)


def update_3_2_to_3_3(db: Connection) -> Connection:
    print("Updating 3.2.0 to 3.3.0")
    db_new: Optional[Connection] = None

    db_path: str = dp if (dp := database_path(db)) else "FA.db"
    db_new_path: str = path_join(dirname(db_path), "new_" + basename(db_path))

    try:
        db_new = connect_database(db_new_path)
        make_database_3_3(db_new)

        # Transfer common submissions and users data
        print("Transfer common submissions and users data")
        db.execute(f"ATTACH DATABASE '{db_new_path}' AS db_new")
        db.execute(
            f"""INSERT OR IGNORE INTO db_new.USERS
            SELECT * FROM USERS"""
        )
        db.execute(
            f"""INSERT OR IGNORE INTO db_new.SUBMISSIONS
            SELECT * FROM SUBMISSIONS"""
        )
        db.execute(
            f"""INSERT OR IGNORE INTO db_new.JOURNALS
            SELECT * FROM JOURNALS"""
        )
        db.execute(
            f"""INSERT OR REPLACE INTO db_new.SETTINGS
            SELECT * FROM SETTINGS WHERE SETTING != "LASTSTART" AND SETTING != "LASTUPDATE" AND SETTING != "VERSION";"""
        )

        # Add update to history
        last_update: str = db.execute("SELECT SVALUE FROM SETTINGS WHERE SETTING = 'LASTUPDATE'").fetchone()
        if last_update and last_update[0] != "0":
            # Commit and close databases to unlock
            db.commit()
            db.close()
            db = None
            db_new.execute("UPDATE SETTINGS SET SVALUE = ? WHERE SETTING = 'HISTORY'",
                           json_dumps([[float(last_update[0]), "update"]]))

        # Close databases and replace old database
        print("Close databases and replace old database")
        db_new.commit()
        db_new.close()
        move(db_path, path_join(dirname(db_path), "v3_2_" + basename(db_path)))
        move(db_new_path, db_path)
    except (BaseException, Exception) as err:
        print("Database update interrupted!")
        if db is not None:
            db.commit()
            db.close()
        if db_new is not None:
            db_new.commit()
            db_new.close()
        raise err

    return connect_database("FA.db")


def update_3_4_to_3_5(db: Connection) -> Connection:
    print("Updating 3.4.0 to 3.5.0")
    db_new: Optional[Connection] = None

    db_path: str = dp if (dp := database_path(db)) else "FA.db"
    db_new_path: str = path_join(dirname(db_path), "new_" + basename(db_path))

    try:
        db_new = connect_database(db_new_path)
        make_database_3_3(db_new)

        # Transfer common submissions and users data
        print("Transfer common submissions and users data")
        db.execute(f"ATTACH DATABASE '{db_new_path}' AS db_new")
        db.execute(
            f"""INSERT OR IGNORE INTO db_new.USERS
            SELECT * FROM USERS"""
        )
        db.execute(
            f"""INSERT OR IGNORE INTO db_new.SUBMISSIONS
            SELECT * FROM SUBMISSIONS"""
        )
        db.execute(
            f"""INSERT OR IGNORE INTO db_new.JOURNALS
            SELECT * FROM JOURNALS"""
        )
        db.execute(
            f"""INSERT OR REPLACE INTO db_new.SETTINGS
            SELECT * FROM SETTINGS WHERE SETTING != "HISTORY" AND SETTING != "VERSION";"""
        )

        # Update history
        history: List[List[str]] = json_loads(
            db.execute("SELECT SVALUE FROM SETTINGS WHERE SETTING = 'HISTORY'").fetchone()[0]
        )
        history_new: List[Tuple[float, str]] = list(map(lambda th: (float(th[0]), th[1]), history))
        db.commit()
        db.close()
        db = None
        db_new.execute("UPDATE SETTINGS SET SVALUE = ? WHERE SETTING = 'HISTORY'", json_dumps(history_new))

        # Close databases and replace old database
        print("Close databases and replace old database")
        db_new.commit()
        db_new.close()
        move(db_path, path_join(dirname(db_path), "v3_4_" + basename(db_path)))
        move(db_new_path, db_path)
    except (BaseException, Exception) as err:
        print("Database update interrupted!")
        if db is not None:
            db.commit()
            db.close()
        if db_new is not None:
            db_new.commit()
            db_new.close()
        raise err

    return connect_database(db_path)


def update_3_8_to_4(db: Connection) -> Connection:
    print("Updating 3.8.0 to 4.0.0")
    db_new: Optional[Connection] = None

    db_path: str = dp if (dp := database_path(db)) else "FA.db"
    db_new_path: str = path_join(dirname(db_path), "new_" + basename(db_path))

    try:
        db_new = connect_database(db_new_path)
        make_database_4(db_new)

        # Transfer common submissions and users data
        print("Transfer common submissions and users data")
        db.execute(f"ATTACH DATABASE '{db_new_path}' AS db_new")
        db.execute(
            f"""INSERT OR IGNORE INTO db_new.USERS
            SELECT * FROM USERS"""
        )
        db.execute(
            f"""INSERT OR IGNORE INTO db_new.SUBMISSIONS
            SELECT * FROM SUBMISSIONS"""
        )
        db.execute(
            f"""INSERT OR IGNORE INTO db_new.JOURNALS
            SELECT * FROM JOURNALS"""
        )
        db.execute(
            f"""INSERT OR REPLACE INTO db_new.SETTINGS
            SELECT * FROM SETTINGS WHERE SETTING != 'VERSION';"""
        )

        # Close databases and replace old database
        print("Close databases and replace old database")
        db.commit()
        db.close()
        db_new.commit()
        db_new.close()
        move(db_path, path_join(dirname(db_path), "v3_8_" + basename(db_path)))
        move(db_new_path, db_path)
    except (BaseException, Exception) as err:
        print("Database update interrupted!")
        if db is not None:
            db.commit()
            db.close()
        if db_new is not None:
            db_new.commit()
            db_new.close()
        raise err

    return connect_database(db_path)


def update_4_2_to_4_3(db: Connection) -> Connection:
    print("Updating 4.2.0 to 4.3.0")
    db_new: Optional[Connection] = None

    db_path: str = dp if (dp := database_path(db)) else "FA.db"
    db_new_path: str = path_join(dirname(db_path), "new_" + basename(db_path))

    try:
        db_new = connect_database(db_new_path)
        make_database_4_3(db_new)

        # Transfer common submissions and users data
        print("Transfer common submissions and users data")
        db.execute(f"ATTACH DATABASE '{db_new_path}' AS db_new")
        db.execute(
            f"""INSERT OR IGNORE INTO db_new.USERS
            SELECT * FROM USERS"""
        )
        db.execute(
            f"""INSERT OR IGNORE INTO db_new.SUBMISSIONS
            SELECT * FROM SUBMISSIONS"""
        )
        db.execute(
            f"""INSERT OR IGNORE INTO db_new.JOURNALS
            SELECT * FROM JOURNALS"""
        )
        db.execute(
            f"""INSERT OR REPLACE INTO db_new.SETTINGS
            SELECT * FROM SETTINGS WHERE SETTING NOT IN ('VERSION', 'JRNN', 'SUBN', 'USRN');"""
        )

        # Close databases and replace old database
        print("Close databases and replace old database")
        db.commit()
        db.close()
        db_new.commit()
        db_new.close()
        move(db_path, path_join(dirname(db_path), "v4_2" + basename(db_path)))
        move(db_new_path, db_path)
    except (BaseException, Exception) as err:
        print("Database update interrupted!")
        if db is not None:
            db.commit()
            db.close()
        if db_new is not None:
            db_new.commit()
            db_new.close()
        raise err

    return connect_database(db_path)


def update_4_3_to_4_4(db: Connection) -> Connection:
    print("Updating 4.3.0 to 4.4.0")
    db_new: Optional[Connection] = None

    db_path: str = dp if (dp := database_path(db)) else "FA.db"
    db_new_path: str = path_join(dirname(db_path), "new_" + basename(db_path))

    try:
        db_new = connect_database(db_new_path)
        make_database_4_4(db_new)

        # Transfer common submissions and users data
        print("Transfer common submissions and users data")
        db.execute(f"ATTACH DATABASE '{db_new_path}' AS db_new")
        db.execute(
            """INSERT OR IGNORE INTO db_new.USERS
            SELECT * FROM USERS"""
        )
        db.execute(
            """INSERT OR IGNORE INTO db_new.SUBMISSIONS
            SELECT *,'' FROM SUBMISSIONS"""
        )
        db.execute(
            """INSERT OR IGNORE INTO db_new.JOURNALS
            SELECT * FROM JOURNALS"""
        )
        db.execute(
            """INSERT OR REPLACE INTO db_new.SETTINGS
            SELECT * FROM SETTINGS WHERE SETTING NOT IN ('VERSION');"""
        )
        db.commit()
        db.close()
        db = None

        for u, fs in db_new.execute("select USERNAME, FAVORITES from USERS where FAVORITES != ''"):
            for f in map(int, filter(bool, fs.split(","))):
                f_us: Optional[tuple] = db_new.execute(f"select FAVORITE from SUBMISSIONS where ID = {f}").fetchone()
                if not f_us:
                    continue
                f_us_new: str = ",".join({u, *filter(bool, f_us[0].split(","))})
                if f_us_new == f_us:
                    continue
                db_new.execute(f"update SUBMISSIONS set FAVORITE = ? where ID = {f}", [f_us_new])
            db_new.commit()

        # Close databases and replace old database
        print("Close databases and replace old database")
        db_new.commit()
        db_new.close()
        move(db_path, path_join(dirname(db_path), "v4_3" + basename(db_path)))
        move(db_new_path, db_path)
    except (BaseException, Exception) as err:
        print("Database update interrupted!")
        if db is not None:
            db.commit()
            db.close()
        if db_new is not None:
            db_new.commit()
            db_new.close()
        raise err

    return connect_database(db_path)


def update_4_4_to_4_5(db: Connection) -> Connection:
    print("Updating 4.4.0 to 4.5.0")
    db_new: Optional[Connection] = None

    db_path: str = dp if (dp := database_path(db)) else "FA.db"
    db_new_path: str = path_join(dirname(db_path), "new_" + basename(db_path))

    try:
        db_new = connect_database(db_new_path)
        make_database_4_5(db_new)

        # Transfer common submissions and users data
        print("Transfer common submissions and users data")
        db.execute(f"ATTACH DATABASE '{db_new_path}' AS db_new")
        db.execute(
            """INSERT OR IGNORE INTO db_new.USERS
            SELECT USERNAME,FOLDERS FROM USERS"""
        )
        db.execute(
            """INSERT OR IGNORE INTO db_new.SUBMISSIONS
            SELECT *,'','gallery' FROM SUBMISSIONS"""
        )
        db.execute(
            """INSERT OR IGNORE INTO db_new.JOURNALS
            SELECT * FROM JOURNALS"""
        )
        db.execute(
            """INSERT OR REPLACE INTO db_new.SETTINGS
            SELECT * FROM SETTINGS WHERE SETTING NOT IN ('VERSION');"""
        )
        db.commit()

        users_old: List[Tuple[str, ...]] = db.execute("select USERNAME,GALLERY,SCRAPS,MENTIONS from USERS").fetchall()
        missing_mentions: List[Tuple[str, int]] = []

        db.close()
        db = None

        mentions_exp: Pattern = re_compile(r'<a[^>]*href="(?:(?:https?://)?(?:www.)?furaffinity.net)?/user/([^/">]+)"')
        for i, d in db_new.execute("select ID, DESCRIPTION from SUBMISSIONS").fetchall():
            mentions: list[str] = sorted(set(filter(bool, map(clean_username, findall(mentions_exp, d)))))
            if mentions:
                db_new.execute("update SUBMISSIONS set MENTIONS = ? where ID = ?", (",".join(mentions), i))
        db_new.commit()

        for user, g, s, m in users_old:
            for i in map(int, filter(bool, g.split(","))):
                db_new.execute("update SUBMISSIONS set FOLDER = ? where ID = ?", ("gallery", i))
            for i in map(int, filter(bool, s.split(","))):
                db_new.execute("update SUBMISSIONS set FOLDER = ? where ID = ?", ("scraps", i))
            db_new.commit()
            for i in map(int, filter(bool, m.split(","))):
                ms: Optional[tuple] = db_new.execute("select MENTIONS from SUBMISSIONS where ID = ?", (i,)).fetchone()
                if not ms or user.lower() not in map(str.lower, ms[0].split(",")):
                    print(f"Missing mention: {user} {i}")
                    missing_mentions.append((user, i))

        if missing_mentions:
            print(f"Missing submissions: {len(missing_mentions)}")
            missing_mentions.sort(key=lambda m_: (m_[0], m_[1]))
            with open(path_join(dirname(db_path), "FA_v4_5_missing_mentions.txt"), "w") as f:
                f.write("\n".join(f"{u} {s}" for u, s in missing_mentions))

        db_new.commit()
        db_new.close()
        move(db_path, path_join(dirname(db_path), "v4_4" + basename(db_path)))
        move(db_new_path, db_path)
    except (BaseException, Exception) as err:
        print("Database update interrupted!")
        if db is not None:
            db.commit()
            db.close()
        if db_new is not None:
            db_new.commit()
            db_new.close()
        raise err

    return connect_database(db_path)


def update_version(db: Connection, version: str, target_version: str) -> Connection:
    print(f"Updating {version} to {target_version}")
    db.execute("UPDATE SETTINGS SET SVALUE = ? WHERE SETTING = 'VERSION'", [target_version])
    db.commit()
    return db


def update_database(db: Connection, version: str) -> Connection:
    if not (db_version := get_version(db)):
        raise DatabaseError("Cannot read version from database.")
    elif (v := compare_versions(db_version, version)) == 0:
        return db
    elif v > 0:
        raise DatabaseError("Database version is newer than program.")
    elif (v := compare_versions(db_version, "2.7.0")) < 0:
        raise DatabaseError("Update does not support versions lower than 2.11.2")
    elif v >= 0 > (v := compare_versions(db_version, "3.0.0")):
        return update_database(update_2_7_to_3(db), version)  # 2.7.x to 3.0.0
    elif v >= 0 > (v := compare_versions(db_version, "3.1.0")):
        return update_database(update_3_to_3_1(db), version)  # 3.0.x to 3.1.0
    elif v >= 0 > (v := compare_versions(db_version, "3.2.0")) < 0:
        return update_database(update_3_1_to_3_2(db), version)  # 3.1.x to 3.2.0
    elif v >= 0 > (v := compare_versions(db_version, "3.3.0")) < 0:
        return update_database(update_3_2_to_3_3(db), version)  # 3.2.x to 3.3.0
    elif v >= 0 > (v := compare_versions(db_version, "3.4.0")) < 0:
        return update_database(update_version(db, db_version, "3.4.0"), version)  # 3.3.x to 3.4.0
    elif v >= 0 > (v := compare_versions(db_version, "3.5.0")) < 0:
        return update_database(update_3_4_to_3_5(db), version)  # 3.4.x to 3.5.0
    elif v >= 0 > (v := compare_versions(db_version, "3.8.0")) < 0:
        return update_database(update_version(db, db_version, "3.8.0"), version)  # 3.5.0-3.7.x to 3.8.0
    elif v >= 0 > (v := compare_versions(db_version, "4.0.0")) < 0:
        return update_database(update_3_8_to_4(db), version)  # 3.8.x to 4.0.0
    elif v >= 0 > (v := compare_versions(db_version, "4.3.0")) < 0:
        return update_database(update_4_2_to_4_3(db), version)  # 4.0.0-4.2.x to 4.3.0
    elif v >= 0 > (v := compare_versions(db_version, "4.4.0")) < 0:
        return update_database(update_4_3_to_4_4(db), version)  # 4.3.x to 4.4.0
    elif v >= 0 > compare_versions(db_version, version) < 0:
        return update_version(db, db_version, version)  # Update to latest patch

    return db
