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
# Modified   : Tuesday November 22nd 2022 04:25:00 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import logging
from dotenv import load_dotenv
import sqlite3

from recsys.core import DB_LOCATIONS

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class Database:

    __DB_LOCATION = None

    def __init__(self):
        self._set_db_location()
        self._connection = None
        self._connection = sqlite3.connect(Database.__DB_LOCATION)
        self._cursor = self._connection.cursor()

    def __enter__(self):
        return self

    def __exit__(self, ext_type, exc_value, traceback):
        self._cursor.close()
        if isinstance(exc_value, Exception):
            self._connection.rollback()
        else:
            self._connection.commit()
        self._connection.close()

    def __del__(self):
        if self.connection is not None:
            self.connection.close()

    def query(self, sql: str, args: tuple = None):
        cursor = self.connection.cursor()
        cursor.execute(sql, args)
        return cursor

    def create_table(self, sql: str, args: tuple = None) -> None:
        _ = self.query(sql, args)

    def insert(self, sql, args):
        cursor = self.query(sql, args)
        id = self.query(sql="SELECT last_insert_rowid();", args=None)
        self.connection.commit()
        cursor.close()
        return id

    def select(self, sql: str, args: tuple = None) -> list:
        cursor = self.query(sql, args)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def remove(self, sql: str, args: tuple = None) -> list:
        _ = self.query(sql, args)

    def exists(self, sql: str, args: tuple = None) -> list:
        cursor = self.query(sql, args)
        rows = cursor.fetchall()
        cursor.close()
        return len(rows) > 0

    def _set_db_location(self) -> None:
        load_dotenv()
        ENV = os.getenv("ENV")
        try:
            Database.__DB_LOCATION = DB_LOCATIONS.get(ENV)
        except KeyError:
            msg = "The current environment, specified by the 'ENV' variable in the .env file, is not supported."
            logger.error(msg)
            raise ValueError(msg)
