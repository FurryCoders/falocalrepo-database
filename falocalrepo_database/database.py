from datetime import datetime
from json import dumps
from json import loads
from os import makedirs
from os.path import dirname
from os.path import join
from re import sub
from sqlite3 import Connection
from sqlite3 import Cursor
from sqlite3 import connect
from typing import Dict
from typing import Generator
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

from filetype import guess_extension as filetype_guess_extension

from .journals import journals_table
from .journals import make_journals_table
from .merge import merge_database
from .settings import make_settings_table
from .settings import settings_table
from .submissions import make_submissions_table
from .submissions import submissions_table
from .update import update_database
from .users import make_users_table
from .users import users_table

Key = Union[str, int, float]
Value = Union[str, int, float, None]


def guess_extension(file: bytes, default: str = "") -> str:
    if not file:
        return ""
    elif (file_type := filetype_guess_extension(file)) is None:
        return default
    elif (file_ext := str(file_type)) == "zip":
        return default if default != "zip" else file_ext
    else:
        return file_ext


def tiered_path(id_: Union[int, str], depth: int = 5, width: int = 2) -> str:
    assert isinstance(id_, int) or (isinstance(id_, str) and id_.isdigit()), "id not an integer"
    assert isinstance(depth, int) and depth > 0, "depth lower than 0"
    assert isinstance(width, int) and width > 0, "depth lower than 0"

    id_str: str = str(int(id_)).zfill(depth * width)
    return join(*[id_str[n:n + width] for n in range(0, depth * width, width)])


def clean_username(username: str) -> str:
    return str(sub(r"[^a-zA-Z0-9\-.~,]", "", username.lower().strip()))


class FADatabaseTable:
    def __init__(self, database: 'FADatabase', table: str):
        self.database: 'FADatabase' = database
        self.connection: Connection = database.connection
        self.table: str = table
        self.columns_info: List[Tuple[str, str]] = [
            (info[1], info[-1])
            for info in self.connection.execute(f"pragma table_info({self.table})")
        ]
        self.columns: List[str] = [name for name, _ in self.columns_info]
        self.column_id: str = [name for name, pk in self.columns_info if pk == 1][0]

    def __len__(self) -> int:
        return self.connection.execute(f"SELECT COUNT({self.column_id}) FROM {self.table}").fetchone()[0]

    def __getitem__(self, key: Union[Key, Dict[str, Union[List[Value], Value]]]) -> Optional[Dict[str, Value]]:
        key = key if isinstance(key, dict) else {self.column_id: key}
        entry: Optional[tuple] = self.select(key, self.columns).fetchone()
        return {k.upper(): v for k, v in zip(self.columns, entry)} if entry is not None else None

    def __setitem__(self, key: Key, values: Dict[str, Value]):
        values.update({self.column_id: key})
        self.insert(values)

    def __delitem__(self, key: Key):
        self.connection.execute(f"""DELETE FROM {self.table} WHERE {self.column_id} = ?""", (key,))

    def __contains__(self, key: Union[Key, Dict[str, Value]]) -> bool:
        return self[key] is not None

    def __iter__(self) -> Generator[Dict[str, Value], None, None]:
        entry: Tuple[Value]
        for entry in self.select():
            yield {k.upper(): v for k, v in zip(self.columns, entry)}

    def select(self, query: Dict[str, Union[List[Value], Value]] = None, columns: List[str] = None,
               query_and: bool = True, query_and_values: bool = False, like: bool = False,
               order: List[str] = None, limit: int = 0, offset: int = 0
               ) -> Cursor:
        query = {} if query is None else query
        query = {k: [v] if not isinstance(v, list) else v for k, v in query.items()}
        query = {k: vs for k, vs in query.items() if vs}
        order = [] if order is None else order
        columns = self.columns if columns is None else columns
        op: str = "like" if like else "="
        logic: str = "AND" if query_and else "OR"
        logic_values: str = "AND" if query_and_values else "OR"
        where_str: str = f" {logic} ".join(map(lambda q: f"({q})", [
            f" {logic_values} ".join([f"{k} {op} ?"] * len(vs))
            for k, vs in query.items()
        ]))
        order_str = ",".join(order)
        return self.connection.execute(
            f"""SELECT {','.join(columns)} FROM {self.table}
            {f' WHERE {where_str} ' if where_str else ''}
            {f' ORDER BY {order_str} ' if order_str else ''}
            {f' LIMIT {limit} ' if limit > 0 else ''}
            {f' OFFSET {offset} ' if limit > 0 and offset > 0 else ''}""",
            [v for values in query.values() for v in values]
        )

    def cursor_to_dict(self, cursor: Cursor, columns: List[str] = None) -> Generator[Dict[str, Value], None, None]:
        columns = self.columns if columns is None else columns
        return ({k.upper(): v for k, v in zip(columns, entry)} for entry in cursor)

    def insert(self, values: Dict[str, Value], replace: bool = True):
        self.connection.execute(
            f"""INSERT OR {'REPLACE' if replace else 'IGNORE'} INTO {self.table}
            ({','.join(col for col in values)}) VALUES ({','.join('?' for _ in values)})""",
            [v for v in values.values()]
        )

    def update(self, values: Dict[str, Value], key: Optional[Key] = None):
        update_columns: List[str] = [f"{col} = ?" for col in values]
        where_str: str = f"WHERE {self.column_id} = ?" if key else ""
        self.connection.execute(
            f"UPDATE {self.table} SET {','.join(update_columns)} {where_str}",
            [v for v in values.values()] + ([key] if key is not None else [])
        )

    def delete(self, key: Key):
        del self[key]


class FADatabaseJournals(FADatabaseTable):
    def save_journal(self, journal: Dict[str, Union[int, str]]):
        journal = {k.upper(): v for k, v in journal.items()}
        self[journal["ID"]] = journal


class FADatabaseSettings(FADatabaseTable):
    def __getitem__(self, setting: str) -> Optional[str]:
        return entry["SVALUE"] if (entry := super().__getitem__(setting)) is not None else None

    def __setitem__(self, setting: str, value: str):
        self.insert({"SETTING": setting, "SVALUE": value})

    def read_history(self) -> List[Tuple[float, str]]:
        return list(map(tuple, loads(self["HISTORY"])))

    def add_history(self, command: str, time: float = datetime.now().timestamp()):
        self["HISTORY"] = dumps(sorted([*self.read_history(), (time, command)], key=lambda h: h[0]))


class FADatabaseSubmissions(FADatabaseTable):
    def save_submission_file(self, submission_id: int, file_ext: str, file: bytes) -> str:
        if not file:
            return ""

        file_ext = guess_extension(file, file_ext)

        path = join(dirname(self.database.database_path), self.database.settings["FILESFOLDER"],
                    tiered_path(submission_id), f"submission.{file_ext}")
        makedirs(dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(file)

        return file_ext

    def save_submission(self, submission: Dict[str, Union[int, str]], file: Optional[bytes] = None):
        submission = {k.upper(): v for k, v in submission.items()}

        submission["FILEEXT"] = name.split(".")[-1] if "." in (name := submission["FILELINK"].split("/")[-1]) else ""
        submission["FILESAVED"] = bool(file)

        submission["FILEEXT"] = self.save_submission_file(submission["ID"], submission["FILEEXT"], file)

        self[submission["ID"]] = submission


class FADatabaseUsers(FADatabaseTable):
    def new_user(self, user: str):
        user = clean_username(user)
        if user not in self:
            self[user] = {f: "" for f in self.columns}

    def enable_user(self, user: str):
        user = clean_username(user)
        if (user_entry := self[user]) is None:
            return

        self.update({"FOLDERS": ",".join([f.strip("!") for f in filter(bool, user_entry["FOLDERS"].split(","))])}, user)

    def disable_user(self, user: str):
        user = clean_username(user)
        if (user_entry := self[user]) is None:
            return

        self.update({"FOLDERS": ",".join([
            f"!{f}" if f.lower() in ("gallery", "scraps", "favorites") else f
            for f in filter(bool, user_entry["FOLDERS"].split(","))
        ])}, user)

    def add_item(self, user: str, folder: str, item: str) -> bool:
        folder = folder.upper()
        user = clean_username(user)
        if (user_entry := self[user]) is None:
            return False

        if item not in (items := user_entry[folder].split(",")):
            user_entry[folder] = ",".join(sorted([item, *filter(bool, items)]))
            self.update(user_entry, user)
            return True

        return False

    def remove_item(self, user: str, folder: str, item: str) -> bool:
        folder = folder.upper()
        user = clean_username(user)
        if (user_entry := self[user]) is None:
            return False

        if item in (items := user_entry[folder].split(",")):
            user_entry[folder] = ",".join(filter(lambda i: i != item, items))
            self.update(user_entry, user)
            return True

        return False

    def add_user_folder(self, user: str, folder: str):
        folder = folder.lower()
        self.add_item(clean_username(user), "FOLDERS", folder)

    def remove_user_folder(self, user: str, folder: str):
        folder = folder.lower()
        self.remove_item(clean_username(user), "FOLDERS", folder)

    def find_from_submission(self, submission_id: int) -> Cursor:
        return self.select(
            query=dict(zip(("GALLERY", "SCRAPS", "FAVORITES", "MENTIONS"), [f"%{submission_id:010}%"] * 4)),
            query_and=False,
            like=True
        )

    def find_from_journal(self, journal_id: int) -> Cursor:
        return self.select({"JOURNALS": f"%{journal_id:010}%"}, like=True)

    def add_submission(self, user: str, folder: str, submission_id: int):
        folder = folder.upper()
        self.add_item(clean_username(user), folder, f"{submission_id:010}")

    def add_journal(self, user: str, journal_id: int):
        self.add_item(clean_username(user), "JOURNALS", f"{journal_id:010}")

    def remove_submission(self, user: str, submission_id: int):
        user = clean_username(user)
        submission_id_fmt: str = f"{submission_id:010}"
        self.remove_item(user, "GALLERY", submission_id_fmt)
        self.remove_item(user, "SCRAPS", submission_id_fmt)
        self.remove_item(user, "FAVORITES", submission_id_fmt)
        self.remove_item(user, "MENTIONS", submission_id_fmt)

    def remove_journal(self, user: str, journal_id: int):
        self.remove_item(clean_username(user), "JOURNALS", f"{journal_id:010}")


class FADatabase:
    def __init__(self, database_path: str):
        self.database_path: str = database_path
        self.connection: Connection = connect(database_path)

        make_journals_table(self.connection)
        make_settings_table(self.connection)
        make_submissions_table(self.connection)
        make_users_table(self.connection)

        self.connection = update_database(self.connection)

        self.journals: FADatabaseJournals = FADatabaseJournals(self, journals_table)
        self.settings: FADatabaseSettings = FADatabaseSettings(self, settings_table)
        self.submissions: FADatabaseSubmissions = FADatabaseSubmissions(self, submissions_table)
        self.users: FADatabaseUsers = FADatabaseUsers(self, users_table)
        self.committed_changes: int = self.total_changes

    def __getitem__(self, table: str):
        return FADatabaseTable(self, table)

    def __iter__(self) -> Generator[Tuple[str, FADatabaseTable], None, None]:
        for table in self.tables:
            yield table, FADatabaseTable(self, table)

    def __contains__(self, table: str) -> bool:
        return table in self.tables

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def tables(self) -> List[str]:
        return [
            name
            for [name] in self.connection.execute(
                """SELECT name FROM sqlite_master 
                WHERE type = 'table' 
                AND name NOT LIKE 'sqlite_%'
                ORDER BY 1;"""
            )
        ]

    @property
    def version(self) -> str:
        return self.settings["VERSION"]

    @property
    def total_changes(self) -> int:
        return int(self.connection.total_changes)

    @property
    def is_clean(self) -> bool:
        return self.total_changes == self.committed_changes

    def commit(self):
        self.connection.commit()
        self.committed_changes = self.total_changes

    def close(self):
        self.connection.close()

    def update(self, db_b: 'FADatabase'):
        merge_database(self.connection, dirname(self.database_path), db_b.connection, dirname(db_b.database_path))

    def vacuum(self):
        self.connection.execute("VACUUM")
