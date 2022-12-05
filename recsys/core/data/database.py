#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /database.py                                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday November 22nd 2022 02:25:42 am                                              #
# Modified   : Saturday December 3rd 2022 11:14:55 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import logging
import sqlite3
from typing import Any
from abc import ABC, abstractmethod

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
#                                    CONNECTION                                                    #
# ------------------------------------------------------------------------------------------------ #


class Connection(ABC):
    """Abstract base class for DBMS connections."""

    def __init__(self, connector: Any, *args, **kwargs) -> None:
        self._connector = connector
        self._connection = None
        self.connect()

    @abstractmethod
    def connect(self) -> Any:
        """Connects to the underlying database"""

    def is_connected(self) -> bool:
        try:
            self._connection.cursor()
            return True
        except Exception:
            return False

    def close(self) -> None:
        """Closes the connection."""
        self._connection.close()

    def commit(self) -> None:  # pragma: no cover
        """Commits the connection"""
        self._connection.commit()

    def cursor(self) -> None:  # pragma: no cover
        """Returns a the connection cursor"""
        return self._connection.cursor()

    def rollback(self) -> None:  # pragma: no cover
        """Rolls back the database to the last commit."""
        self._connection.rollback()

    def start_transaction(self) -> None:  # pragma: no cover
        """Starts transaction on the underlying connection."""
        self._connection.start_transaction()


# ------------------------------------------------------------------------------------------------ #
class SQLiteConnection(Connection):
    """Connection to the underlying SQLite DBMS."""

    def __init__(self, connector: sqlite3.connect, location: str) -> None:
        self._location = location
        super().__init__(connector=connector)

    def connect(self) -> sqlite3.Connection:
        os.makedirs(os.path.dirname(self._location), exist_ok=True)
        self._connection = self._connector(self._location)


# ------------------------------------------------------------------------------------------------ #
#                                        DATABASE                                                  #
# ------------------------------------------------------------------------------------------------ #
class Database(ABC):
    def __init__(self, connection: Connection):  # pragma: no cover
        self._connection = connection

    def __enter__(self):  # pragma: no cover
        if not self._connection.is_connected():
            self._connection.connect()
        return self

    def __exit__(self, ext_type, exc_value, traceback):  # pragma: no cover
        if isinstance(exc_value, Exception):
            self._connection.rollback()
        else:
            self._connection.commit()
        self._connection.close()

    def __del__(self):  # pragma: no cover
        if self._connection.is_connected():
            self._connection.close()

    @abstractmethod
    def _get_last_insert_rowid(self) -> int:
        """Returns the last inserted id. Implemented differently in databases."""

    def query(self, sql: str, args: tuple = None):
        cursor = self._connection.cursor()
        cursor.execute(sql, args)
        return cursor

    def create_table(self, sql: str, args: tuple = None) -> None:
        cursor = self.query(sql, args)
        self._connection.commit()
        cursor.close()

    def drop_table(self, sql: str, args: tuple = None) -> None:
        cursor = self.query(sql, args)
        self._connection.commit()
        cursor.close()

    def insert(self, sql, args):
        cursor = self.query(sql, args)
        self._connection.commit()
        cursor.close()
        return self._get_last_insert_rowid()

    def select(self, sql: str, args: tuple = None) -> list:
        cursor = self.query(sql, args)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def update(self, sql: str, args: tuple = None) -> None:
        cursor = self.query(sql, args)
        self._connection.commit()
        cursor.close()

    def count(self, sql: str, args: tuple = None) -> int:
        cursor = self.query(sql, args)
        rows = cursor.fetchall()
        cursor.close()
        return rows[0][0]

    def delete(self, sql: str, args: tuple = None) -> None:
        cursor = self.query(sql, args)
        self._connection.commit()
        cursor.close()

    def exists(self, sql: str, args: tuple = None) -> bool:
        cursor = self.query(sql, args)
        rows = cursor.fetchall()
        cursor.close()
        return rows[0][0] > 0


# ------------------------------------------------------------------------------------------------ #
class SQLiteDatabase(Database):
    def __init__(self, connection: SQLiteConnection):
        self._connection = connection

    def _get_last_insert_rowid(self) -> int:
        cursor = self.query(sql="SELECT last_insert_rowid();", args=())
        id = cursor.fetchall()[0][0]
        cursor.close()
        return id
