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
# Modified   : Sunday November 27th 2022 02:39:06 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import logging
import sqlite3

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class Database:
    def __init__(self, db_location: str):
        self._db_location = db_location
        self._connection = None
        self._connection = self._connect()

    def __enter__(self):
        self._connection = self._connect()
        return self

    def __exit__(self, ext_type, exc_value, traceback):
        if isinstance(exc_value, Exception):  # pragma: no cover
            self._connection.rollback()
        else:
            self._connection.commit()
        self._connection.close()

    def __del__(self):
        if self._connection is not None:
            self._connection.close()

    @property
    def location(self) -> str:
        if "test" in self._db_location:
            return self._db_location
        else:
            return "The database location is none of your business."  # pragma: no cover

    def query(self, sql: str, args: tuple = None):
        cursor = self._connection.cursor()
        cursor.execute(sql, args)
        return cursor

    def create(self, sql: str, args: tuple = None) -> None:
        cursor = self.query(sql, args)
        self._connection.commit()
        cursor.close()

    def drop(self, sql: str, args: tuple = None) -> None:
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

    def _get_last_insert_rowid(self) -> int:
        cursor = self.query(sql="SELECT last_insert_rowid();", args=())
        id = cursor.fetchall()[0][0]
        cursor.close()
        return id

    def _connect(self) -> None:
        os.makedirs(os.path.dirname(self._db_location), exist_ok=True)
        return sqlite3.connect(self._db_location)
