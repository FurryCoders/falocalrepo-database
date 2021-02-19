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

from .__version__ import __version__
from .merge import merge_database
from .tables import journals_table
from .tables import make_journals_table
from .tables import make_settings_table
from .tables import make_submissions_table
from .tables import make_users_table
from .tables import settings_table
from .tables import submissions_table
from .tables import users_table
from .update import update_database

Key = Union[str, int, float]
Value = Union[str, int, float, None]


def guess_extension(file: bytes, default: str = "") -> str:
    if not file:
        return ""
    elif (file_type := filetype_guess_extension(file)) is None:
        return default
    elif (file_ext := str(file_type)) in (exts := ("zip", "octet-stream")):
        return default if default not in exts else file_ext
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
        self.table: str = table
        self.columns_info_: List[Tuple[str, str]] = []
        self.columns_: List[str] = []
        self.column_id_: str = ""

    def __len__(self) -> int:
        return self.database.connection.execute(f"SELECT COUNT(*) FROM {self.table}").fetchone()[0]

    def __getitem__(self, key: Union[Key, Dict[str, Union[List[Value], Value]]]) -> Optional[Dict[str, Value]]:
        key = key if isinstance(key, dict) else {self.column_id: key}
        entry: Optional[tuple] = self.select(key, self.columns).fetchone()
        return {k.upper(): v for k, v in zip(self.columns, entry)} if entry is not None else None

    def __setitem__(self, key: Key, values: Dict[str, Value]):
        values.update({self.column_id: key})
        self.insert(values)

    def __delitem__(self, key: Key):
        self.database.connection.execute(f"""DELETE FROM {self.table} WHERE {self.column_id} = ?""", (key,))

    def __contains__(self, key: Union[Key, Dict[str, Value]]) -> bool:
        return self[key] is not None

    def __iter__(self) -> Generator[Dict[str, Value], None, None]:
        return ({k.upper(): v for k, v in zip(self.columns_, entry)} for entry in self.select())

    @property
    def columns_info(self) -> List[Tuple[str, str]]:
        self.columns_info_ = [
            info[1:]
            for info in self.database.connection.execute(f"pragma table_info({self.table})")
        ] if not self.columns_info_ else self.columns_info_
        return self.columns_info_

    @property
    def columns(self) -> List[str]:
        self.columns_ = [name for name, *_ in self.columns_info] if not self.columns_ else self.columns_
        return self.columns_

    @property
    def column_id(self) -> str:
        self.column_id_ = [
            name
            for name, *_, pk in self.columns_info if pk == 1
        ][0]
        return self.column_id_

    def add_to_list(self, key: Key, values: Dict[str, List[Value]]) -> bool:
        item: Optional[dict] = self[key]
        if not item:
            return False
        item = {k: item[k] for k in values.keys()}
        item_new: Dict[str, str] = {k: ",".join(sorted(set(item[k].split(",") + v))) for k, v in values.items()}
        self.update(item_new, key) if item_new != item else None
        return item_new != item

    def remove_from_list(self, key: Key, values: Dict[str, List[Value]]) -> bool:
        item: Optional[dict] = self[key]
        if not item:
            return False
        item = {k: item[k] for k in values.keys()}
        item_new: Dict[str, str] = {k: ",".join(sorted(set(item[k].split(",")) - set(v))) for k, v in values.items()}
        self.update(item_new, key) if item_new != item else None
        return item_new != item

    def reload(self):
        self.__init__(self.database, self.table)

    def cursor_to_dict(self, cursor: Cursor, columns: List[str] = None) -> Generator[Dict[str, Value], None, None]:
        columns = self.columns if columns is None else columns
        return ({k.upper(): v for k, v in zip(columns, entry)} for entry in cursor)

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
        return self.database.connection.execute(
            f"""SELECT {','.join(columns)} FROM {self.table}
            {f' WHERE {where_str} ' if where_str else ''}
            {f' ORDER BY {order_str} ' if order_str else ''}
            {f' LIMIT {limit} ' if limit > 0 else ''}
            {f' OFFSET {offset} ' if limit > 0 and offset > 0 else ''}""",
            [v for values in query.values() for v in values]
        )

    def select_sql(self, where: str = "", values: List[Value] = None, columns: List[str] = None,
                   order: List[str] = None, limit: int = 0, offset: int = 0) -> Cursor:
        columns = self.columns if columns is None else columns
        order = [] if order is None else order
        return self.database.connection.execute(
            f"""SELECT {','.join(columns)} FROM {self.table}
            {f' WHERE {where} ' if where else ''}
            {f' ORDER BY {",".join(order)}' if order else ''}
            {f' LIMIT {limit}' if limit > 0 else ''}
            {f' OFFSET {offset}' if limit > 0 and offset > 0 else ''}""",
            [] if values is None else values
        )

    def insert(self, values: Dict[str, Value], replace: bool = True):
        self.database.connection.execute(
            f"""INSERT OR {'REPLACE' if replace else 'IGNORE'} INTO {self.table}
            ({','.join(values.keys())}) VALUES ({','.join(['?'] * len(values))})""",
            [v for v in values.values()]
        )

    def update(self, values: Dict[str, Value], key: Optional[Key] = None):
        update_columns: List[str] = [f"{col} = ?" for col in values]
        where_str: str = f"WHERE {self.column_id} = ?" if key else ""
        self.database.connection.execute(
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
        submission = {k: submission.get(k, "") for k in {*submission.keys(), *self.columns}}

        submission["FILEEXT"] = name.split(".")[-1] if "." in (name := submission["FILELINK"].split("/")[-1]) else ""
        submission["FILESAVED"] = bool(file)

        submission["FILEEXT"] = self.save_submission_file(submission["ID"], submission["FILEEXT"], file)

        self[submission["ID"]] = submission

    def set_folder(self, submission_id: int, folder: str) -> bool:
        folder_old: str = self[submission_id]["FOLDER"]
        self.update({"FOLDER": folder}, submission_id) if folder_old != (folder := folder.lower().strip()) else None
        return folder_old != folder

    def add_favorite(self, submission_id: int, user: str) -> bool:
        user = clean_username(user)
        assert len(user) > 0, "User cannot be empty"
        return self.add_to_list(submission_id, {"FAVORITE": [user]})

    def remove_favorite(self, submission_id: int, user: str) -> bool:
        user = clean_username(user)
        assert len(user) > 0, "User cannot be empty"
        return self.remove_from_list(submission_id, {"FAVORITE": [user]})

    def add_mention(self, submission_id: int, user: str) -> bool:
        user = clean_username(user)
        assert len(user) > 0, "User cannot be empty"
        return self.add_to_list(submission_id, {"MENTIONS": [user]})

    def remove_mention(self, submission_id: int, user: str) -> bool:
        user = clean_username(user)
        assert len(user) > 0, "User cannot be empty"
        return self.remove_from_list(submission_id, {"MENTIONS": [user]})


class FADatabaseUsers(FADatabaseTable):
    def new_user(self, user: str):
        user = clean_username(user)
        if user not in self:
            self[user] = {f: "" for f in self.columns_}

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

    def add_user_folder(self, user: str, folder: str):
        folder = folder.lower()
        self.add_to_list(clean_username(user), {"FOLDERS": [folder]})

    def remove_user_folder(self, user: str, folder: str):
        folder = folder.lower()
        self.remove_from_list(clean_username(user), {"FOLDERS": [folder]})


class FADatabase:
    def __init__(self, database_path: str):
        self.database_path: str = database_path
        self.connection: Connection = connect(database_path)

        if journals_table not in (tables := self.tables):
            make_journals_table(self.connection)
        if settings_table not in tables:
            make_settings_table(self.connection)
        if submissions_table not in tables:
            make_submissions_table(self.connection)
        if users_table not in tables:
            make_users_table(self.connection)

        self.journals: FADatabaseJournals = FADatabaseJournals(self, journals_table)
        self.settings: FADatabaseSettings = FADatabaseSettings(self, settings_table)
        self.submissions: FADatabaseSubmissions = FADatabaseSubmissions(self, submissions_table)
        self.users: FADatabaseUsers = FADatabaseUsers(self, users_table)
        self.committed_changes: int = self.total_changes

    def __getitem__(self, table: str):
        return FADatabaseTable(self, table)

    def __iter__(self) -> Generator[Tuple[str, FADatabaseTable], None, None]:
        return ((table, FADatabaseTable(self, table)) for table in self.tables)

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

    def upgrade(self):
        self.connection = update_database(self.connection, __version__)

        self.journals.reload()
        self.settings.reload()
        self.submissions.reload()
        self.users.reload()
        self.committed_changes: int = self.total_changes

    def commit(self):
        self.connection.commit()
        self.committed_changes = self.total_changes

    def rollback(self):
        self.connection.execute("ROLLBACK")

    def reset(self):
        self.close()
        self.__init__(self.database_path)

    def close(self):
        self.connection.close()

    def update(self, db_b: 'FADatabase'):
        merge_database(self.connection, dirname(self.database_path), db_b.connection, dirname(db_b.database_path))

    def vacuum(self):
        self.connection.execute("VACUUM")
