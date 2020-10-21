from os import listdir
from os import makedirs
from os.path import dirname
from os.path import isdir
from os.path import isfile
from os.path import join
from shutil import copy
from sqlite3 import DatabaseError
from typing import List
from typing import Optional
from typing import Tuple

from .__version__ import __version__
from .database import Connection
from .journals import journals_fields
from .journals import journals_table
from .settings import settings_table
from .submissions import submissions_fields
from .submissions import submissions_table
from .users import users_fields
from .users import users_indexes
from .users import users_table


def merge_folders(src: str, dest: str):
    if isdir(src):
        for item in listdir(src):
            merge_folders(join(src, item), join(dest, item))
    elif isfile(src) and not isfile(dest):
        makedirs(dirname(dest), exist_ok=True)
        copy(src, dest)


def merge_database(db_a: Connection, db_a_folder: str, db_b: Connection, db_b_folder: str):
    """
    B -> A\n
    B.files -> A.files

    :param db_a: Destination database
    :param db_a_folder: Destination database location
    :param db_b: Secondary database
    :param db_b_folder: Secondary database location
    :return: None
    """

    db_a_version: str = db_a.execute(f"SELECT SVALUE FROM {settings_table} WHERE SETTING = 'FILESFOLDER'").fetchone()[0]
    db_b_version: str = db_b.execute(f"SELECT SVALUE FROM {settings_table} WHERE SETTING = 'FILESFOLDER'").fetchone()[0]
    if db_b_version is None or db_b_version is None:
        raise DatabaseError(f"Cannot read database versions: DB A: {db_a_version}, DB B: {db_b_version}")
    elif db_a_version != db_b_version:
        raise DatabaseError(f"Database versions do not match: DB A: {db_a_version}, DB B: {db_b_version}")
    elif db_a_version != __version__ or db_b_version != __version__:
        raise DatabaseError(f"Database versions are not latest: {db_a_version} != {__version__}")

    db_a_files_folder: str = join(db_a_folder, db_a.execute(
        f"SELECT SVALUE FROM {settings_table} WHERE SETTING = 'FILESFOLDER'").fetchone()[0])
    db_b_files_folder: str = join(db_b_folder, db_b.execute(
        f"SELECT SVALUE FROM {settings_table} WHERE SETTING = 'FILESFOLDER'").fetchone()[0])

    merge_folders(db_b_files_folder, db_a_files_folder)

    submission: tuple
    for submission in db_b.execute(f"SELECT {','.join(submissions_fields)} FROM {submissions_table}"):
        db_a.execute(
            f"""INSERT OR IGNORE INTO {submissions_table}
            ({','.join(submissions_fields)}) VALUES ({','.join(['?'] * len(submissions_fields))})""",
            submission
        )
    db_a.commit()

    journal: tuple
    for journal in db_b.execute(f"SELECT {','.join(journals_fields)} FROM {journals_table}"):
        db_a.execute(
            f"""INSERT OR IGNORE INTO {journals_table}
            ({','.join(journals_fields)}) VALUES ({','.join(['?'] * len(journals_fields))})""",
            journal
        )
    db_a.commit()

    user_b: Tuple[str, ...]
    for user_b in db_b.execute(f"SELECT {','.join(users_fields)} FROM {users_table}"):
        user: str = user_b[users_indexes["USERNAME"]]
        user_a: Optional[Tuple[str, ...]] = db_b.execute(
            f"SELECT {','.join(users_fields)} FROM {users_table} WHERE USERNAME = ?", [user]).fetchone()
        user_new: List[str] = list(user_b)

        if user_a is not None:
            for i, field_a in enumerate(user_a):
                if (field_b := user_new[i]) != field_a:
                    user_new[i] = ",".join(filter(bool, set(field_a.split(",") + field_b.split(","))))
        db_a.execute(
            f"""INSERT OR REPLACE INTO {users_table}
            ({','.join(users_fields)}) VALUES ({','.join(['?'] * len(users_fields))})""",
            user_new
        )
    db_a.commit()
