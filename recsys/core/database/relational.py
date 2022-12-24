#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/database/relational.py                                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday November 22nd 2022 02:25:42 am                                              #
# Modified   : Saturday December 24th 2022 01:50:30 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from .connection import Connection
from recsys.core.services.base import Service


# ------------------------------------------------------------------------------------------------ #
#                                        DATABASE                                                  #
# ------------------------------------------------------------------------------------------------ #
class RDB(Service):
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

    def begin(self, sql, args):
        cursor = self.query(sql, args)
        cursor.close()

    def insert(self, sql, args):
        cursor = self.query(sql, args)
        id = cursor.lastrowid
        cursor.close()
        return id

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
