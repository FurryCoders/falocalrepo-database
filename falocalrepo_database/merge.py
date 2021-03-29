from os import listdir
from os import makedirs
from os.path import dirname
from os.path import isdir
from os.path import isfile
from os.path import join
from shutil import copy
from sqlite3 import Connection
from sqlite3 import DatabaseError
from typing import Optional

from .__version__ import __version__
from .tables import journals_table
from .tables import settings_table
from .tables import submissions_table
from .tables import users_table
from .update import get_version


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

    db_a_version, db_b_version = get_version(db_a), get_version(db_b)
    if db_b_version is None or db_b_version is None:
        raise DatabaseError(f"Cannot read database versions: DB A: {db_a_version}, DB B: {db_b_version}")
    elif db_a_version != db_b_version:
        raise DatabaseError(f"Database versions do not match: DB A: {db_a_version}, DB B: {db_b_version}")
    elif db_a_version != __version__ or db_b_version != __version__:
        raise DatabaseError(
            f"Database versions are not latest: DB A: {db_a_version}, DB B: {db_b_version}, LATEST: {__version__}")

    db_a_files_folder: str = join(db_a_folder, db_a.execute(
        f"SELECT SVALUE FROM {settings_table} WHERE SETTING = 'FILESFOLDER'").fetchone()[0])
    db_b_files_folder: str = join(db_b_folder, db_b.execute(
        f"SELECT SVALUE FROM {settings_table} WHERE SETTING = 'FILESFOLDER'").fetchone()[0])

    merge_folders(db_b_files_folder, db_a_files_folder)

    submission: tuple
    for submission in db_b.execute(f"SELECT * FROM {submissions_table}"):
        db_a.execute(
            f"INSERT OR IGNORE INTO {submissions_table} VALUES ({','.join(['?'] * len(submission))})",
            submission
        )

    journal: tuple
    for journal in db_b.execute(f"SELECT * FROM {journals_table}"):
        db_a.execute(
            f"INSERT OR IGNORE INTO {journals_table} VALUES ({','.join(['?'] * len(journal))})",
            journal
        )

    user_b: tuple[str, str]
    for user_b in db_b.execute(f"SELECT USERNAME, FOLDERS FROM {users_table}"):
        user_new: list[str] = list(user_b)
        user_a: Optional[tuple[str, str]] = db_a.execute(
            f"SELECT USERNAME, FOLDERS FROM {users_table} WHERE USERNAME = ?", [user_b[0]]).fetchone()

        if user_a is not None:
            fs_a: list[str] = user_a[1].split("|")
            fs_b: list[str] = user_b[1].split("|")
            user_new[1] = "".join(f"|{f}|" for f in sorted(set(filter(bool, fs_a + fs_b)), key=str.lower))
        db_a.execute(
            f"INSERT OR REPLACE INTO {users_table} (USERNAME,FOLDERS) VALUES ({','.join(['?'] * len(user_new))})",
            user_new
        )
