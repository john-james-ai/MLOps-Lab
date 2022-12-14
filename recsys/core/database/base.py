#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/database/base.py                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday November 22nd 2022 02:25:42 am                                              #
# Modified   : Tuesday December 13th 2022 03:57:27 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from typing import Any
from abc import abstractmethod
from recsys.core.services.base import Service
# ------------------------------------------------------------------------------------------------ #
#                                    CONNECTION                                                    #
# ------------------------------------------------------------------------------------------------ #


class Connection(Service):
    """Abstract base class for DBMS connections."""

    def __init__(self, connector: Any, *args, **kwargs) -> None:
        super().__init__()
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
#                                        DATABASE                                                  #
# ------------------------------------------------------------------------------------------------ #
class Database(Service):
    def __init__(self, connection: Connection):
        super().__init__()
        self._connection = connection

    def __enter__(self):
        if not self._connection.is_connected():
            self._connection.connect()
        return self

    def __exit__(self, ext_type, exc_value, traceback):
        if isinstance(exc_value, Exception):
            self._connection.rollback()
        else:
            self._connection.commit()
        self._connection.close()

    def __del__(self):
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
        cursor.close()

    def drop_table(self, sql: str, args: tuple = None) -> None:
        cursor = self.query(sql, args)
        cursor.close()

    def insert(self, sql, args):
        cursor = self.query(sql, args)
        cursor.close()
        return self._get_last_insert_rowid()

    def select(self, sql: str, args: tuple = None) -> list:
        cursor = self.query(sql, args)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def update(self, sql: str, args: tuple = None) -> None:
        cursor = self.query(sql, args)
        cursor.close()

    def count(self, sql: str, args: tuple = None) -> int:
        cursor = self.query(sql, args)
        rows = cursor.fetchall()
        cursor.close()
        return rows[0][0]

    def delete(self, sql: str, args: tuple = None) -> None:
        cursor = self.query(sql, args)
        cursor.close()

    def exists(self, sql: str, args: tuple = None) -> bool:
        cursor = self.query(sql, args)
        rows = cursor.fetchall()
        cursor.close()
        return rows[0][0] > 0

    def save(self) -> None:
        self._connection.commit()
