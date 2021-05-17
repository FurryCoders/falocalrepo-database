from datetime import datetime
from functools import cached_property
from json import dumps
from json import loads
from os import PathLike
from os import makedirs
from pathlib import Path
from re import sub
from shutil import copy
from shutil import move
from sqlite3 import Connection
from sqlite3 import Cursor
from sqlite3 import ProgrammingError
from sqlite3 import connect
from typing import Generator
from typing import Iterable
from typing import Optional
from typing import Union

from chardet import detect as detect_encoding
from filetype import guess_extension as filetype_guess_extension
from psutil import AccessDenied
from psutil import NoSuchProcess
from psutil import Process
from psutil import process_iter

from .__version__ import __version__
from .exceptions import MultipleConnections
from .exceptions import VersionMismatch
from .selector import Selector
from .selector import selector_to_sql
from .tables import journals_table
from .tables import list_columns
from .tables import make_journals_table
from .tables import make_settings_table
from .tables import make_submissions_table
from .tables import make_users_table
from .tables import settings_table
from .tables import submissions_table
from .tables import users_table
from .types import Entry
from .types import Key
from .types import Value
from .update import update_database


def guess_extension(file: Optional[bytes], default: str = "") -> str:
    if not file:
        return default
    elif (file_type := filetype_guess_extension(file)) is None:
        return default if default else "txt" * (detect_encoding(file[:2048]).get("encoding", None) is not None)
    elif (ext := str(file_type)) in (exts := ("zip", "octet-stream")):
        return default if default not in exts else ext
    else:
        return ext


def tiered_path(id_: Union[int, str], depth: int = 5, width: int = 2) -> Path:
    assert isinstance(id_, int) or (isinstance(id_, str) and id_.isdigit()), "id not an integer"
    assert isinstance(depth, int) and depth > 0, "depth lower than 0"
    assert isinstance(width, int) and width > 0, "depth lower than 0"

    id_str: str = str(int(id_)).zfill(depth * width)
    return Path(*[id_str[n:n + width] for n in range(0, depth * width, width)])


def check_version(version_a: str, *, major: bool = True, minor: bool = True, patch: bool = True,
                  version_b: str = __version__
                  ) -> Optional[VersionMismatch]:
    if not version_a:
        return VersionMismatch(f"version missing")
    elif version_a == version_b:
        return None
    elif (v_a := version_a.split("."))[0] != (v_b := version_b.split("."))[0]:
        return VersionMismatch(f"major version is not latest: {v_a[0]} != {v_b[0]}") if major else None
    elif v_a[1] != v_b[1]:
        return VersionMismatch(f"minor version is not latest: {v_a[1]} != {v_b[1]}") if minor else None
    elif v_a[2] != v_b[2]:
        return VersionMismatch(f"patch version is not latest: {v_a[2]} != {v_b[2]}") if patch else None


def copy_folder(src: Path, dest: Path):
    if src.is_dir():
        for item in src.iterdir():
            copy_folder(item, dest / item.name)
    elif src.is_file and not dest.is_file():
        makedirs(dest.parent, exist_ok=True)
        copy(src, dest)


def clean_username(username: str) -> str:
    return str(sub(r"[^a-zA-Z0-9\-.~,]", "", username.lower().strip()))


def copy_cursors(db_dest: 'FADatabase', cursors: list['FADatabaseCursor'] = None, replace: bool = True):
    if not cursors:
        return
    assert (e := check_version(version_dest := db_dest.version)) is None, \
        f"Destination database is not up to date. {e.args[0]}"
    assert all(check_version(c.table.database.version, version_b=version_dest) is None for c in cursors), \
        "Cursors must point to a database with the same version as the destination database"
    assert all(set(c.columns) == set(c.table.columns) for c in cursors), "Cursors must contain all table columns"

    dest_files: Path = db_dest.files_folder
    cursor_files: Optional[Path] = None

    for cursor in cursors:
        table_dest: FADatabaseTable = db_dest[cursor.table.table]
        if cursor.table.table == submissions_table:
            cursor_files = cursor.table.database.files_folder
        for entry in cursor:
            if not replace and table_dest.table == users_table and (entry_b := table_dest[entry[table_dest.column_id]]):
                entry["FOLDERS"] = list(set(entry["FOLDERS"] + entry_b["FOLDERS"]))
            if table_dest.table == submissions_table:
                copy_folder(cursor_files / (p := tiered_path(entry["ID"])), dest_files / p)
            table_dest.insert(cursor.table.format_dict(entry), replace=True)


def format_list(obj: list[Value]) -> str:
    return "".join(f"|{e}|" for e in sorted(set(map(str, obj)), key=str.lower))


def unpack_list(obj: str) -> list[str]:
    return [e for e in obj.split("|") if e]


class FADatabaseTable:
    def __init__(self, database: 'FADatabase', table: str):
        self.database: 'FADatabase' = database
        self.table: str = table.upper()
        self.list_columns: list[str] = list(map(str.upper, list_columns.get(self.table, [])))

    def __len__(self) -> int:
        return self.database.connection.execute(f"SELECT COUNT(*) FROM {self.table}").fetchone()[0]

    def __getitem__(self, key: Union[Key, Entry]) -> Optional[Entry]:
        key = key if isinstance(key, dict) else {self.column_id: key}
        return self.select({"$eq": key}).fetchone()

    def __setitem__(self, key: Key, values: Entry):
        values = self.format_dict(values)
        values[self.column_id] = key
        self.insert(values)

    def __delitem__(self, key: Key):
        self.database.connection.execute(f"""DELETE FROM {self.table} WHERE {self.column_id} = ?""", (key,))

    def __contains__(self, key: Union[Key, Entry]) -> bool:
        return self[key] is not None

    def __iter__(self) -> Generator[Entry, None, None]:
        return self.select().__iter__()

    @cached_property
    def columns_info(self) -> list[tuple[str, ...]]:
        return [
            (info[1].upper(), *info[2:])
            for info in self.database.connection.execute(f"pragma table_info({self.table})")
        ]

    @cached_property
    def columns(self) -> list[str]:
        return [name for name, *_ in self.columns_info]

    @cached_property
    def column_id(self) -> str:
        return [
            name
            for name, *_, pk in self.columns_info if pk == 1
        ][0]

    def add_to_list(self, key: Key, values: dict[str, list[Value]]) -> bool:
        if not (values := {k.upper(): v for k, v in values.items() if v and k.upper() in self.list_columns}):
            return False
        elif not (item := self[key]):
            return False
        item = {k: item[k] for k in values.keys()}
        item_new = {k: sorted(filter(bool, set(item[k] + v)), key=str.lower) for k, v in values.items()}
        self.update({k: format_list(v) for k, v in item_new.items()}, key) if item_new != item else None
        return item_new != item

    def remove_from_list(self, key: Key, values: dict[str, list[Value]]) -> bool:
        if not (values := {k.upper(): v for k, v in values.items() if v and k.upper() in self.list_columns}):
            return False
        elif not (item := self[key]):
            return False
        item = {k: item[k] for k in values.keys()}
        item_new = {k: sorted(filter(bool, set(item[k]) - set(v)), key=str.lower) for k, v in values.items()}
        self.update({k: format_list(v) for k, v in item_new.items()}, key) if item_new != item else None
        return item_new != item

    def reload(self):
        self.__init__(self.database, self.table)
        for attr in filter(self.__dict__.__contains__, ("columns", "columns_info", "column_id")):
            del self.__dict__[attr]

    def format_dict(self, obj: Entry) -> dict[str, Value]:
        obj = {k.upper().replace("_", ""): v for k, v in obj.items()}
        obj = {k: format_list(v) if isinstance(v := obj.get(k, ""), list) else v for k in self.columns}
        return obj

    def select(self, query: Selector = None, columns: list[str] = None,
               order: list[str] = None, limit: int = 0, offset: int = 0
               ) -> 'FADatabaseCursor':
        sql, values = selector_to_sql(query) if query else ("", [])
        cursor: Cursor = self.database.execute(
            f"""SELECT {','.join(columns := columns or self.columns)} FROM {self.table}
                    {f' WHERE {sql}' if sql else ''}
                    {f' ORDER BY {",".join(order)}' if order else ''}
                    {f' LIMIT {limit}' if limit > 0 else ''}
                    {f' OFFSET {offset}' if limit > 0 and offset > 0 else ''}""",
            values)
        return FADatabaseCursor(cursor, columns, self)

    def select_sql(self, where: str = "", values: list[Value] = None, columns: list[str] = None,
                   order: list[str] = None, limit: int = 0, offset: int = 0) -> 'FADatabaseCursor':
        columns = columns or self.columns
        order = order or []
        cursor: Cursor = self.database.connection.execute(
            f"""SELECT {','.join(columns)} FROM {self.table}
            {f' WHERE {where} ' if where else ''}
            {f' ORDER BY {",".join(order)}' if order else ''}
            {f' LIMIT {limit}' if limit > 0 else ''}
            {f' OFFSET {offset}' if limit > 0 and offset > 0 else ''}""",
            [] if values is None else values
        )
        return FADatabaseCursor(cursor, columns, self)

    def insert(self, values: dict[str, Value], replace: bool = True):
        self.database.connection.execute(
            f"""INSERT OR {'REPLACE' if replace else 'IGNORE'} INTO {self.table}
            ({','.join(values.keys())}) VALUES ({','.join(['?'] * len(values))})""",
            [v for v in values.values()]
        )

    def update(self, values: dict[str, Value], key: Optional[Key] = None):
        update_columns: list[str] = [f"{col} = ?" for col in values]
        where_str: str = f"WHERE {self.column_id} = ?" if key else ""
        self.database.connection.execute(
            f"UPDATE {self.table} SET {','.join(update_columns)} {where_str}",
            [v for v in values.values()] + ([key] if key is not None else [])
        )

    def delete(self, key: Key):
        del self[key]


class FADatabaseJournals(FADatabaseTable):
    def save_journal(self, journal: dict[str, Union[int, str, list]]):
        journal = self.format_dict(journal)
        self[journal["ID"]] = journal

    def add_mention(self, journal_id: int, user: str) -> bool:
        user = clean_username(user)
        assert len(user) > 0, "User cannot be empty"
        return self.add_to_list(journal_id, {"MENTIONS": [user]})

    def remove_mention(self, journal_id: int, user: str) -> bool:
        user = clean_username(user)
        assert len(user) > 0, "User cannot be empty"
        return self.remove_from_list(journal_id, {"MENTIONS": [user]})


class FADatabaseSettings(FADatabaseTable):
    def __getitem__(self, setting: str) -> Optional[str]:
        return (super().__getitem__(setting) or {}).get("SVALUE", None)

    def __setitem__(self, setting: str, value: str):
        super().__setitem__(setting, {"SVALUE": value})

    def read_history(self) -> list[tuple[float, str]]:
        return list(map(tuple, loads(h) if (h := self["HISTORY"]) else []))

    def add_history(self, command: str, time: float = 0):
        if self["HISTORY"] is None:
            return
        time = datetime.now().timestamp() if time <= 0 else time
        self["HISTORY"] = dumps(sorted([*self.read_history(), (time, command)], key=lambda h: h[0]))


class FADatabaseSubmissions(FADatabaseTable):
    def get_submission_files(self, submission_id: int) -> tuple[Optional[Path], Optional[Path]]:
        if (entry := self[submission_id]) is None or (f := entry["FILESAVED"]) == 0:
            return None, None
        folder: Path = self.database.files_folder / tiered_path(submission_id)
        file: Path = folder / ("submission" + f".{(ext := entry['FILEEXT'])}" * bool(ext)) if f >= 10 else None
        thumb: Path = folder / "thumbnail.jpg" if f % 10 == 1 else None
        return file, thumb

    def save_submission_file(self, submission_id: int, file: Optional[bytes], name: str, ext: str,
                             guess_ext: bool = True) -> str:
        if file is None:
            return ""

        ext = guess_extension(file, ext) if guess_ext else ext
        folder: Path = self.database.files_folder / tiered_path(submission_id)
        makedirs(folder, exist_ok=True)
        open(folder / (name + f".{ext}" * bool(ext)), "wb").write(file)

        return ext

    def save_submission_thumbnail(self, submission_id: int, file: Optional[bytes]):
        self.save_submission_file(submission_id, file, "thumbnail", "jpg", False)

    def save_submission(self, submission: dict[str, Union[int, str, list]], file: Optional[bytes] = None,
                        thumbnail: Optional[bytes] = None):
        submission = self.format_dict(submission)

        submission["FILEEXT"] = name.split(".")[-1] if "." in (name := submission["FILEURL"].split("/")[-1]) else ""
        submission["FILEEXT"] = self.save_submission_file(submission["ID"], file, "submission", submission["FILEEXT"])
        submission["FILESAVED"] = (10 * bool(file)) + bool(thumbnail)
        self.save_submission_thumbnail(submission["ID"], thumbnail)

        self[submission["ID"]] = submission

    def set_folder(self, submission_id: int, folder: str) -> bool:
        folder_old: str = self[submission_id]["FOLDER"]
        self.update({"FOLDER": folder}, submission_id) if folder_old != (folder := folder.lower().strip()) else None
        return folder_old != folder

    def add_favorite(self, submission_id: int, user: str) -> bool:
        return self.add_to_list(submission_id, {"FAVORITE": [clean_username(user)]})

    def remove_favorite(self, submission_id: int, user: str) -> bool:
        return self.remove_from_list(submission_id, {"FAVORITE": [clean_username(user)]})

    def add_mention(self, submission_id: int, user: str) -> bool:
        return self.add_to_list(submission_id, {"MENTIONS": [clean_username(user)]})

    def remove_mention(self, submission_id: int, user: str) -> bool:
        return self.remove_from_list(submission_id, {"MENTIONS": [clean_username(user)]})


class FADatabaseUsers(FADatabaseTable):
    def new_user(self, user: str) -> str:
        if (user := clean_username(user)) not in self:
            self[user] = {}
        return user

    def activate_user(self, user: str):
        if (user_entry := self[(user := clean_username(user))]) is None:
            return
        self.update({"FOLDERS": format_list([f.strip("!") for f in user_entry["FOLDERS"]])}, user)

    def deactivate_user(self, user: str):
        if (user_entry := self[(user := clean_username(user))]) is None:
            return
        self.update({"FOLDERS": format_list([f"!{f.strip('!')}" for f in user_entry["FOLDERS"]])}, user)

    def add_user_folder(self, user: str, folder: str) -> bool:
        if not (user_entry := self[(user := clean_username(user))]):
            return False
        elif (folder := folder.lower()) in user_entry["FOLDERS"]:
            return False
        elif f"!{folder}" in user_entry["FOLDERS"]:
            self.remove_user_folder(user, f"!{folder}")
        return self.add_to_list(user, {"FOLDERS": [folder]})

    def remove_user_folder(self, user: str, folder: str) -> bool:
        return self.remove_from_list(clean_username(user), {"FOLDERS": [folder.lower()]})


class FADatabaseCursor:
    def __init__(self, cursor: Cursor, columns: list[str], table: FADatabaseTable):
        self.table: FADatabaseTable = table
        self.cursor: Cursor = cursor
        self.columns: list[str] = columns
        self.entries: Generator[Entry, None, None] = (
            {k: unpack_list(v) if k in self.table.list_columns else v for k, v in zip(columns, entry)}
            for entry in self.cursor
        )

    def __next__(self) -> Entry:
        return next(self.entries)

    def __iter__(self) -> Generator[Entry, None, None]:
        return self.entries

    def fetchone(self):
        return next(self.entries, None)

    def fetchall(self):
        return list(self.entries)


class FADatabase:
    def __init__(self, database_path: Union[str, PathLike], *, make: bool = True):
        self.database_path: Path = Path(database_path).resolve()
        self.connection: Connection = connect(self.database_path)

        self.journals: FADatabaseJournals = FADatabaseJournals(self, journals_table)
        self.settings: FADatabaseSettings = FADatabaseSettings(self, settings_table)
        self.submissions: FADatabaseSubmissions = FADatabaseSubmissions(self, submissions_table)
        self.users: FADatabaseUsers = FADatabaseUsers(self, users_table)
        self.committed_changes: int = self.total_changes

        if make:
            self.make_tables()

    def __getitem__(self, table: str):
        return FADatabaseTable(self, table)

    def __iter__(self) -> Generator[tuple[str, FADatabaseTable], None, None]:
        return ((table, FADatabaseTable(self, table)) for table in self.tables)

    def __contains__(self, table: str) -> bool:
        return table in self.tables

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def tables(self) -> list[str]:
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

    @property
    def files_folder(self) -> Path:
        return self.database_path.parent / self.settings["FILESFOLDER"]

    def is_open(self) -> bool:
        try:
            self.connection.execute("SELECT * FROM sqlite_master")
            return True
        except ProgrammingError:
            return False

    def make_tables(self):
        if journals_table not in (tables := self.tables):
            make_journals_table(self.connection)
            self.journals.reload()
        if settings_table not in tables:
            make_settings_table(self.connection)
            self.settings.reload()
        if submissions_table not in tables:
            make_submissions_table(self.connection)
            self.submissions.reload()
        if users_table not in tables:
            make_users_table(self.connection)
            self.users.reload()

    def move_files_folder(self, new_folder: Union[str, PathLike]) -> Path:
        new_folder = Path(new_folder)
        new_folder = new_folder if new_folder.is_absolute() else (self.database_path.parent / new_folder)
        move(self.files_folder, new_folder)
        self.settings["FILESFOLDER"] = str(new_folder.relative_to(self.database_path.parent))
        return new_folder

    def upgrade(self):
        self.connection = update_database(self.connection, __version__)

        self.journals.reload()
        self.settings.reload()
        self.submissions.reload()
        self.users.reload()
        self.committed_changes: int = self.total_changes

    def check_version(self, raise_for_error: bool = True, *, major: bool = True, minor: bool = True, patch: bool = True,
                      version: str = __version__
                      ) -> Optional[VersionMismatch]:
        err: Optional[VersionMismatch] = check_version(
            self.version, major=major, minor=minor, patch=patch, version_b=version)
        if err is not None and raise_for_error:
            raise err
        return err

    def check_connection(self, raise_for_error: bool = True) -> list[Process]:
        ps: list[Process] = []
        database_path: str = str(self.database_path)
        for process in process_iter():
            try:
                if process.is_running() and any(database_path == f.path for f in process.open_files()):
                    ps.append(process)
                if len(ps) > 1 and raise_for_error:
                    raise MultipleConnections(f"Multiple connections to database: {ps}")
            except (NoSuchProcess, AccessDenied):
                pass
        return ps

    def execute(self, sql: str, parameters: Iterable) -> Cursor:
        return self.connection.execute(sql, parameters)

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

    def merge(self, db_b: 'FADatabase', *cursors: FADatabaseCursor):
        copy_cursors(self, cursors or [db_b.users.select(), db_b.submissions.select(), db_b.journals.select()], False)

    def copy(self, db_b: 'FADatabase', *cursors: FADatabaseCursor):
        copy_cursors(db_b, cursors or [self.users.select(), self.submissions.select(), self.journals.select()])

    def vacuum(self):
        self.connection.execute("VACUUM")
