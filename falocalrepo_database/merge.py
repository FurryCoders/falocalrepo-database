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
from .database import insert
from .database import select
from .database import select_all
from .journals import journals_fields
from .journals import journals_table
from .settings import read_setting
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

    db_a_version, db_b_version = read_setting(db_a, "VERSION"), read_setting(db_b, "VERSION")
    if db_b_version is None or db_b_version is None:
        raise DatabaseError(f"Cannot read database versions: DB A: {db_a_version}, DB B: {db_b_version}")
    elif db_a_version != db_b_version:
        raise DatabaseError(f"Database versions do not match: DB A: {db_a_version}, DB B: {db_b_version}")
    elif db_a_version != __version__ or db_b_version != __version__:
        raise DatabaseError(f"Database versions are not latest: {db_a_version} != {__version__}")

    db_a_files_folder: str = join(db_a_folder, read_setting(db_a, "FILESFOLDER"))
    db_b_files_folder: str = join(db_b_folder, read_setting(db_b, "FILESFOLDER"))

    merge_folders(db_b_files_folder, db_a_files_folder)

    submission: tuple
    for submission in select_all(db_b, submissions_table, submissions_fields):
        insert(db_a, submissions_table, submissions_fields, submission, False)
    db_a.commit()

    journal: tuple
    for journal in select_all(db_b, journals_table, journals_fields):
        insert(db_a, journals_table, journals_fields, journal, False)
    db_a.commit()

    user_b: Tuple[str, ...]
    for user_b in select_all(db_b, users_table, users_fields):
        user: str = user_b[users_indexes["USERNAME"]]
        user_a: Optional[Tuple[str, ...]] = select(db_b, users_table, users_fields, ["USERNAME"], [user]).fetchone()
        user_new: List[str] = list(user_b)

        if user_a is not None:
            for i, field_a in enumerate(user_a):
                if (field_b := user_new[i]) != field_a:
                    user_new[i] = ",".join(filter(bool, set(field_a.split(",") + field_b.split(","))))

        insert(db_a, users_table, users_fields, user_new, True)
    db_a.commit()
