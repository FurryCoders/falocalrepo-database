from datetime import datetime
from os import PathLike
from pathlib import Path
from re import search
from shutil import copy
from sqlite3 import Connection
from sqlite3 import Cursor as SQLCursor
from sqlite3 import DatabaseError
from sqlite3 import ProgrammingError
from sqlite3 import connect
from typing import Any
from typing import Generator
from typing import Iterable
from typing import Type
from typing import TypeVar
from typing import overload

from psutil import Process

from .__version__ import __version__
from .column import Column
from .column import NoDefault
from .exceptions import VersionError
from .selector import EQ
from .selector import OR
from .selector import Selector
from .selector import selector_to_sql
from .tables import HistoryColumns
from .tables import JournalsColumns
from .tables import SettingsColumns
from .tables import SubmissionsColumns
from .tables import UsersColumns
from .tables import history_table
from .tables import journals_table
from .tables import settings_table
from .tables import submissions_table
from .tables import users_table
from .types import Value
from .update import update_database
from .util import clean_username
from .util import compare_version
from .util import find_connections
from .util import guess_extension
from .util import query_to_sql
from .util import tiered_path

T = TypeVar("T")


def _copy_folder(src: Path, dest: Path):
    if src.is_dir():
        for item in src.iterdir():
            _copy_folder(item, dest / item.name)
    elif src.is_file and not dest.is_file():
        dest.parent.mkdir(parents=True, exist_ok=True)
        copy(src, dest)


def copy_cursors(db_dest: 'Database', cursors: Iterable['Cursor'], replace: bool, exist_ok: bool):
    if not cursors:
        return
    elif not db_dest.is_formatted:
        raise DatabaseError("Destination database is not formatted.")
    elif any(c.table.database.path == db_dest.path for c in cursors):
        raise DatabaseError("Cursors must not point to the destination database.")
    elif err := compare_version(version_dest := db_dest.version):
        raise VersionError("Destination database is not up to date.", *err.args)
    elif any(compare_version(c.table.database.version, version_b=version_dest) is not None for c in cursors):
        raise VersionError("Cursors must point to a database with the same version as the destination database.")
    elif any(set(c_.name for c_ in c.columns) != set(c_.name for c_ in c.table.columns) for c in cursors):
        raise DatabaseError("Cursors must contain all columns.")

    for cursor in cursors:
        cursor_db: Database = cursor.table.database
        dest_table: Table
        if cursor.table.name.lower() == db_dest.users.name.lower():
            dest_table = db_dest.users
        elif cursor.table.name.lower() == db_dest.submissions.name.lower():
            dest_table = db_dest.submissions
        elif cursor.table.name.lower() == db_dest.journals.name.lower():
            dest_table = db_dest.journals
        elif cursor.table.name.lower() == db_dest.settings.name.lower():
            dest_table = db_dest.settings
        elif cursor.table.name.lower() == db_dest.history.name.lower():
            dest_table = db_dest.history
        else:
            raise DatabaseError(f"Unknown table {cursor.table.name}")
        for entry in cursor:
            if dest_table.name.lower() == db_dest.submissions.name.lower():
                f, t = cursor_db.submissions.get_submission_files(entry[cursor.table.key.name])
                db_dest.submissions.save_submission(entry, f.read_bytes() if f else None, t.read_bytes() if t else None,
                                                    replace=replace, exist_ok=exist_ok)
            else:
                dest_table.insert(dest_table.format_entry(entry), replace=replace, exists_ok=True)


class Cursor:
    def __init__(self, cursor: SQLCursor, columns: list[Column], table: 'Table'):
        self.cursor: SQLCursor = cursor
        self.columns: list[Column] = columns
        self.table: Table = table

    def __next__(self) -> dict[str, Value]:
        return next(self.entries)

    def __iter__(self) -> Generator[dict[str, Value], None, None]:
        return self.entries

    @property
    def entries(self) -> Generator[dict[str, Any], None, None]:
        return ({c.name: c.from_entry(v) for c, v in zip(self.columns, row, strict=True)} for row in self.cursor)

    @property
    def tuples(self) -> Generator[tuple, None, None]:
        return (tuple(c.from_entry(v) for c, v in zip(self.columns, row, strict=True)) for row in self.cursor)

    def fetchone(self):
        return next(self.entries, None)

    def fetchall(self):
        return list(self.entries)


class Table:
    def __init__(self, database: "Database", name: str, columns: Iterable[Column] = None):
        self.database: Database = database
        self.name: str = name
        self._columns: list[Column] = columns or []

    def __len__(self) -> int:
        return self.select(columns=[Column(f"count({self.key.name})", int)]).cursor.fetchone()[0]

    def __contains__(self, key: Value) -> bool:
        return bool(self[key])

    @overload
    def __getitem__(self, key: dict[str, Value] | tuple[Value] | list[Value]) -> list[dict[str, Value]]:
        ...

    @overload
    def __getitem__(self, key: Value) -> dict[str, Value] | None:
        ...

    def __getitem__(self, key: Value | dict[str, Value] | tuple[Value]
                    ) -> dict[str, Value] | list[dict[str, Value]] | None:
        if isinstance(key, dict):
            return self.select({EQ: self.format_entry(key, defaults=False)}).fetchall()
        elif isinstance(key, (tuple, list)):
            return self.select({OR: [{EQ: {self.key.name: self.key.to_entry(k)}} for k in key]}).fetchall()
        else:
            return self.select({EQ: {self.key.name: self.key.to_entry(key)}}).fetchone()

    def __setitem__(self, key: Value, entry: dict[str, Any]):
        self.insert(self.format_entry(entry | {self.key.name: key}), replace=True)

    def __delitem__(self, key: Value | dict[str, Value] | tuple[Value] | list[Value]) -> SQLCursor:
        if isinstance(key, dict):
            return self.delete({EQ: self.format_entry(key, defaults=False)})
        elif isinstance(key, (tuple, list)):
            return self.delete({OR: [{EQ: {self.key.name: self.key.to_entry(k)}} for k in key]})
        else:
            return self.delete({EQ: {self.key.name: self.key.to_entry(key)}})

    def __iter__(self) -> Generator[dict[str, Value], None, None]:
        return self.select().entries

    def _get_exists(self, key: Value):
        if not (entry := self[key]):
            raise KeyError(f"Entry {self.key.name} = {key!r} does not exist in {self.name} table.")
        return entry

    def _get_columns(self) -> list[Column]:
        return [Column(name, t, not_null=bool(not_null), key=pk)
                for _, name, t, not_null, _, pk in self.database.execute(f"pragma table_info({self.name})")]

    @property
    def columns(self) -> list[Column]:
        self._columns = self._columns or self._get_columns()
        return self._columns

    @property
    def key(self) -> Column | None:
        return next((k for k in self.keys), None)

    @property
    def keys(self) -> list[Column]:
        return [c for c in self.columns if c.key]

    def get_column(self, name: str) -> Column | None:
        name = name.lower()
        return next((c for c in self.columns if c.name.lower() == name.lower()), None)

    def create_statement(self, exists_ignore: bool = False) -> str:
        elements: list[str] = ["create table"]

        if exists_ignore:
            elements.append("if not exists")

        elements.append(self.name)

        if self.columns:
            elements.append("(" + ", ".join(c.create_statement() for c in self.columns) +
                            (f", primary key ({', '.join(c.name for c in keys)})" if (keys := self.keys) else "") +
                            ")")

        return " ".join(elements)

    def create(self, exists_ignore: bool = True):
        self.database.execute(self.create_statement(exists_ignore=exists_ignore))

    def format_entry(self, entry: dict[str, Any], *, defaults: bool = True) -> dict[str, Value]:
        columns_dict: dict[str, Any] = {}
        if defaults:
            columns_dict = {c.name.upper(): c.default for c in self.columns if c.default is not NoDefault}
        entry = columns_dict | {k.upper(): v for k, v in entry.items()}
        entry = {(c := self.get_column(k)).name: c.to_entry(v) for k, v in entry.items()}
        return entry

    def insert(self, entry: dict[str, Value], *, replace: bool = False, exists_ok: bool = False):
        self.database.execute(
            f"""INSERT {'OR REPLACE' if replace else 'OR IGNORE' if exists_ok else ''} INTO {self.name}
                    ({','.join(entry.keys())}) VALUES ({','.join(['?'] * len(entry))})""",
            [v for v in entry.values()]
        )

    def select(self, query: Selector = None, columns: list[str | Column] = None, order: list[str] = None,
               limit: int = 0,
               offset: int = 0) -> Cursor:
        sql, values = selector_to_sql(query) if query else ("", None)
        return self.select_sql(sql, values, columns, order, limit, offset)

    def select_query(self, query: str, columns: list[str | Column], default_field: str, likes: list[str] = None,
                     aliases: dict[str, str] = None, order: list[str] = None, limit: int = 0, offset: int = 0
                     ) -> Cursor:

        elements, values = query_to_sql(query, default_field, likes, aliases)
        return self.select_sql(" ".join(elements), values, columns, order, limit, offset)

    def select_sql(self, sql: str, values: list[Any] = None, columns: list[str | Column] = None,
                   order: list[str] = None, limit: int = 0, offset: int = 0) -> Cursor:
        columns_: list[Column] = [(self.get_column(c) or Column(c, Any)) if isinstance(c, str) else c
                                  for c in columns] if columns else self.columns

        cursor: SQLCursor = self.database.execute(
            f"""SELECT {','.join(c.name for c in columns_)} FROM {self.name}
                            {f' WHERE {sql}' if sql else ''}
                            {f' ORDER BY {",".join(order)}' if order else ''}
                            {f' LIMIT {limit}' if limit > 0 else ''}
                            {f' OFFSET {offset}' if limit > 0 and offset > 0 else ''}""",
            values)

        return Cursor(cursor, columns_, self)

    def update(self, query: Selector, new_entry: dict[str, Value]) -> SQLCursor:
        sql, values = selector_to_sql(query) if query else ("", [])
        update_columns: list[str] = [f"{col} = ?" for col in new_entry]
        return self.database.execute(f"UPDATE {self.name} SET {','.join(update_columns)} WHERE {sql}",
                                     [*new_entry.values(), *values])

    def delete(self, query: Selector) -> SQLCursor:
        sql, values = selector_to_sql(query) if query else ("", [])
        return self.database.execute(f"DELETE FROM {self.name} WHERE {sql}", values)

    def add_to_list(self, key: Value, column: str | Column, new_values: Iterable[Value]):
        entry: dict = self._get_exists(key)
        column = column.name if isinstance(column, Column) else self.get_column(column).name
        if changes := any(v not in entry[column] for v in new_values):
            entry[column] = list(entry[column]) + list(new_values)
            self[key] = entry
        return changes

    def remove_from_list(self, key: Value, column: str | Column, new_values: Iterable[Value]):
        entry: dict = self._get_exists(key)
        column = column.name if isinstance(column, Column) else self.get_column(column).name
        if changes := any(v in entry[column] for v in new_values):
            new_list: set = set(entry[column]) - set(new_values)
            entry[column] = sorted(new_list, key=list(entry[column]).index)
            self[key] = entry
        return changes


class UsersTable(Table):
    def save_user(self, user: dict[str, Any], *, replace: bool = False, exist_ok: bool = False):
        self.insert(self.format_entry(user), replace=replace, exists_ok=exist_ok)

    def deactivate(self, user: str) -> bool:
        entry: dict = self._get_exists(user := clean_username(user))
        folders: set[str] = entry[col := UsersColumns.FOLDERS.value.name]
        if all(f.startswith("!") for f in folders):
            return False
        entry[col] = {f"!{f.strip('!')}" for f in folders}
        self[user] = entry
        return True

    def activate(self, user: str) -> bool:
        entry: dict = self._get_exists(user := clean_username(user))
        folders: set[str] = entry[col := UsersColumns.FOLDERS.value.name]
        if not any(f.startswith("!") for f in folders):
            return False
        entry[col] = {f.strip('!') for f in folders}
        self[user] = entry
        return True

    def add_folder(self, user: str, folder: str) -> bool:
        return self.add_to_list(clean_username(user), UsersColumns.FOLDERS.value.name, [folder])

    def remove_folder(self, user: str, folder: str) -> bool:
        return self.remove_from_list(clean_username(user), UsersColumns.FOLDERS.value.name, [folder])

    def set_userpage(self, user: str, userpage: str) -> bool:
        entry: dict = self._get_exists(user := clean_username(user))
        if entry[UsersColumns.USERPAGE.value.name] == userpage:
            return False
        entry[UsersColumns.USERPAGE.value.name] = userpage
        self[user] = entry
        return True


class SubmissionsTable(Table):
    @property
    def files_folder(self) -> Path:
        return self.database.settings.files_folder

    def save_submission(self, submission: dict[str, Value | list[Value]], file: bytes = None, thumbnail: bytes = None,
                        *, replace: bool = False, exist_ok: bool = False):
        submission = self.format_entry(submission)

        submission[SubmissionsColumns.FILEEXT.value.name] = \
            self.save_submission_file(
                submission[SubmissionsColumns.ID.value.name], file, "submission",
                s[1] if (s := search(r"/[^/]+\.([^.]+)$", submission[SubmissionsColumns.FILEURL.value.name])) else "")
        self.save_submission_thumbnail(submission["ID"], thumbnail)
        submission[SubmissionsColumns.FILESAVED.value.name] = (0b10 * bool(file)) + (0b01 * bool(thumbnail))

        self.insert(submission, replace=replace, exists_ok=exist_ok)

    def save_submission_file(self, submission_id: int, file: bytes | None, name: str, ext: str,
                             guess_ext: bool = True) -> str:
        if file is None:
            return ""

        ext: str = guess_extension(file, ext) if guess_ext else ext
        folder: Path = self.files_folder / tiered_path(submission_id)
        folder.mkdir(parents=True, exist_ok=True)
        folder.joinpath(name + f".{ext}" * bool(ext)).write_bytes(file)

        return ext

    def save_submission_thumbnail(self, submission_id: int, file: bytes | None):
        self.save_submission_file(submission_id, file, "thumbnail", "jpg", False)

    def get_submission_files(self, submission_id: int) -> tuple[Path | None, Path | None]:
        if (entry := self[submission_id]) is None or (f := entry[SubmissionsColumns.FILESAVED.value.name]) == 0:
            return None, None
        folder: Path = self.files_folder / tiered_path(submission_id)
        file_ext: str = f".{(ext := entry[SubmissionsColumns.FILEEXT.value.name])}" * bool(ext)
        return folder / f"submission{file_ext}" if f & 0b10 else None, folder / "thumbnail.jpg" if f & 0b01 else None

    def set_folder(self, submission_id: int, folder: str) -> bool:
        if self._get_exists(submission_id)[SubmissionsColumns.FOLDER.value.name] != (folder := folder.lower().strip()):
            self.update({EQ: {self.key.name: submission_id}}, {SubmissionsColumns.FOLDER.value.name: folder})
            return True
        return False

    def set_user_update(self, submission_id: int, update: int) -> bool:
        if self._get_exists(submission_id)[SubmissionsColumns.USERUPDATE.value.name] != update:
            self.update({EQ: {self.key.name: submission_id}}, {SubmissionsColumns.USERUPDATE.value.name: update})
            return True
        return False

    def add_favorite(self, submission_id: int, user: str) -> bool:
        return self.add_to_list(submission_id, SubmissionsColumns.FAVORITE.value.name, [clean_username(user)])

    def remove_favorite(self, submission_id: int, user: str) -> bool:
        return self.remove_from_list(submission_id, SubmissionsColumns.FAVORITE.value.name, [clean_username(user)])

    def add_mention(self, submission_id: int, user: str) -> bool:
        return self.add_to_list(submission_id, SubmissionsColumns.MENTIONS.value.name, [clean_username(user)])

    def remove_mention(self, submission_id: int, user: str) -> bool:
        return self.remove_from_list(submission_id, SubmissionsColumns.MENTIONS.value.name, [clean_username(user)])


class JournalsTable(Table):
    def save_journal(self, journal: dict[str, Any], *, replace: bool = False, exist_ok: bool = False):
        self.insert(self.format_entry(journal), replace=replace, exists_ok=exist_ok)

    def set_user_update(self, journal_id: int, update: int) -> bool:
        if self._get_exists(journal_id)[JournalsColumns.USERUPDATE.value.name] != update:
            self.update({EQ: {self.key.name: journal_id}}, {JournalsColumns.USERUPDATE.value.name: update})
            return True
        return False

    def add_mention(self, journal_id: int, user: str) -> bool:
        return self.add_to_list(journal_id, JournalsColumns.MENTIONS.value.name, [clean_username(user)])

    def remove_mention(self, journal_id: int, user: str) -> bool:
        return self.remove_from_list(journal_id, JournalsColumns.MENTIONS.value.name, [clean_username(user)])


class SettingsTable(Table):
    version_setting: str = "VERSION"
    files_folder_setting: str = "FILESFOLDER"
    default_files_folder: str = "FA.files"

    def __getitem__(self, item: str) -> str | None:
        return (super().__getitem__(item) or {}).get(SettingsColumns.SVALUE.value.name, None)

    def __setitem__(self, key: str, value: str):
        self.insert(self.format_entry({self.key.name: key, SettingsColumns.SVALUE.value.name: value}), replace=True)

    @property
    def version(self):
        return self[self.version_setting]

    @property
    def files_folder(self) -> Path:
        folder: Path = Path(self[self.files_folder_setting])
        return folder if folder.is_absolute() else (self.database.path.parent / folder).resolve()

    @files_folder.setter
    def files_folder(self, value: str | Path):
        self[self.files_folder_setting] = str(value)

    def create(self, exists_ignore: bool = False):
        super().create(exists_ignore=exists_ignore)
        self.insert({SettingsColumns.SETTING.value.name: self.files_folder_setting,
                     SettingsColumns.SVALUE.value.name: self.default_files_folder}, exists_ok=True)
        self.insert({SettingsColumns.SETTING.value.name: self.version_setting,
                     SettingsColumns.SVALUE.value.name: __version__}, exists_ok=True)


class HistoryTable(Table):
    def __iter__(self) -> Generator[dict[str, Value], None, None]:
        return self.select(order=[self.key.name]).entries

    def add_event(self, event: str, time: datetime = None):
        self[time or datetime.now()] = {HistoryColumns.EVENT.value.name: event}


class Database:
    def __init__(self, path: str | PathLike | Path, *, init: bool = False, check_connections: bool = True,
                 check_version: bool = True, read_only: bool = False, autocommit: bool = False):
        self.path: Path = Path(path).resolve()
        self.read_only: bool = read_only

        if check_connections:
            self.check_connection()

        self.connection: Connection = connect(self.path.as_uri() + ("?mode=ro" if read_only else ""), uri=True)
        self.autocommit = autocommit

        self.users: UsersTable = UsersTable(self, users_table, UsersColumns.as_list())
        self.submissions: SubmissionsTable = SubmissionsTable(self, submissions_table, SubmissionsColumns.as_list())
        self.journals: JournalsTable = JournalsTable(self, journals_table, JournalsColumns.as_list())
        self.settings: SettingsTable = SettingsTable(self, settings_table, SettingsColumns.as_list())
        self.history: HistoryTable = HistoryTable(self, history_table, HistoryColumns.as_list())

        self.committed_changes: int = self.total_changes

        if self.is_formatted:
            if check_version:
                self.check_version()
        elif init:
            self.init()

    def __getitem__(self, name: str) -> Table:
        return Table(self, name.upper())

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        self.close()

    def __contains__(self, value: str | Table):
        value = value.name.lower() if isinstance(value, Table) else value.lower()
        return value in [t.name.lower() for t in self.tables]

    @property
    def autocommit(self):
        return self.connection.isolation_level is None

    @autocommit.setter
    def autocommit(self, value: bool):
        self.connection.isolation_level = None if value else ""

    @property
    def tables(self) -> list[Table]:
        return [
            Table(self, name)
            for [name] in self.connection.execute(
                """select name from sqlite_master
                where type = 'table'
                and name not like 'sqlite_%'
                order by 1;"""
            )
        ]

    @property
    def total_changes(self) -> int:
        return int(self.connection.total_changes)

    @property
    def is_clean(self) -> bool:
        return self.total_changes == self.committed_changes

    @property
    def is_formatted(self):
        return settings_table in self

    @property
    def is_open(self) -> bool:
        try:
            self.connection.execute("SELECT * FROM sqlite_master")
            return True
        except ProgrammingError:
            return False

    @property
    def version(self) -> str | None:
        return self.settings.version

    def init(self):
        self.users.create(exists_ignore=True)
        self.submissions.create(exists_ignore=True)
        self.journals.create(exists_ignore=True)
        self.settings.create(exists_ignore=True)
        self.history.create(exists_ignore=True)

    def check_connection(self: Type["Database"] | str | PathLike | Path, raise_for_error: bool = True, limit: int = 0
                         ) -> list[Process]:
        return find_connections(self.path if isinstance(self, Database) else Path(self), raise_for_error, limit)

    def check_version(self, raise_for_error: bool = True) -> VersionError | None:
        err: VersionError | None = compare_version(self.version)
        if raise_for_error and err:
            raise err
        return err

    def execute(self, sql: str, parameters: Iterable = None) -> SQLCursor:
        return self.connection.execute(sql, parameters or [])

    def commit(self):
        self.connection.commit()
        self.committed_changes = self.total_changes

    def rollback(self):
        self.execute("ROLLBACK")

    def reset(self, *, init: bool = False, check_connections: bool = True, check_version: bool = True,
              read_only: bool = None, autocommit: bool = None):
        self.close()
        self.connection = None
        self.__init__(self.path, init=init, check_connections=check_connections, check_version=check_version,
                      read_only=self.read_only if read_only is None else read_only,
                      autocommit=self.autocommit if autocommit is None else autocommit)

    def upgrade(self, *, check_connections: bool = True, read_only: bool = None, autocommit: bool = None):
        self.connection = update_database(self.connection, __version__)
        self.reset(check_connections=check_connections, check_version=False,
                   read_only=self.read_only if read_only is None else read_only,
                   autocommit=self.autocommit if autocommit is None else autocommit)

    def merge(self, db_b: 'Database', *cursors: Cursor, replace: bool = True, exist_ok: bool = True):
        copy_cursors(self, cursors or [db_b.users.select(), db_b.submissions.select(), db_b.journals.select()],
                     replace=replace, exist_ok=exist_ok)

    def copy(self, db_b: 'Database', *cursors: Cursor, replace: bool = True, exist_ok: bool = True):
        copy_cursors(db_b, cursors or [self.users.select(), self.submissions.select(), self.journals.select()],
                     replace=replace, exist_ok=exist_ok)

    def close(self):
        self.connection.close()
