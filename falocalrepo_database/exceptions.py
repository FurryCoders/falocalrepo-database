from sqlite3 import DatabaseError

__all__ = [
    "UnknownSelector",
    "VersionError",
    "MultipleConnections",
]


class UnknownSelector(KeyError):
    pass


class VersionError(DatabaseError):
    pass


class MultipleConnections(DatabaseError):
    pass
