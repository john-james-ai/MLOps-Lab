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
# Modified   : Tuesday January 3rd 2023 07:52:12 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Relational Databases Module."""
import pymysql

from .base import AbstractConnection, AbstractDatabase


# ------------------------------------------------------------------------------------------------ #
#                                    CONNECTION                                                    #
# ------------------------------------------------------------------------------------------------ #
class MySQLConnection(AbstractConnection):
    """MySQL Databae."""

    def __init__(self, connector: pymysql.connect, host: str, user: str, password: str, database: str = None) -> None:
        super().__init__()
        self._database = database
        self._connection = connector(host=host, user=user, password=password, database=database, autocommit=False)
        self._is_connected = True
        self._in_transaction = False
        self._logger.debug(f"{self.__class__.__name__} is connected.")

    @property
    def cursor(self) -> pymysql.connections.Connection.cursor:
        return self._connection.cursor()

    @property
    def database(self) -> str:
        """Returns the name of the database to which the connection has been made."""
        return self._database

    @property
    def is_connected(self) -> bool:
        """Returns the True if the connection is open, False otherwise."""
        return self._is_connected

    @property
    def in_transaction(self) -> bool:
        """Returns the True if a transaction has been started."""
        return self._in_transaction

    def begin(self) -> None:
        """Start a transaction on the connection."""
        self._in_transaction = True
        self._logger.debug(f"{self.__class__.__name__}  transaction started.")

    def close(self) -> None:
        """Closes the connection."""
        self._connection.close()
        self._is_connected = False
        self._in_transaction = False
        self._logger.debug(f"{self.__class__.__name__}  is closed.")

    def commit(self) -> None:
        """Commits the connection"""
        self._connection.commit()
        self._in_transaction = False
        self._logger.debug(f"{self.__class__.__name__}  is committed.")

    def rollback(self) -> None:
        """Rolls back the database to the last commit."""
        self._connection.rollback()
        self._in_transaction = False
        self._logger.debug(f"{self.__class__.__name__}  is rolled back.")


# ------------------------------------------------------------------------------------------------ #
#                                        DATABASE                                                  #
# ------------------------------------------------------------------------------------------------ #
class Database(AbstractDatabase):
    def __init__(self, connection: pymysql.connections.Connection):
        super().__init__()
        self._connection = connection

    @property
    def connection(self) -> pymysql.connections.Connection:
        return self._connection

    def query(self, sql: str, args: tuple = None) -> pymysql.connections.Connection.cursor:
        """Executes a query on the database and returns a cursor object."""
        cursor = self._connection.cursor
        cursor.execute(sql, args)
        return cursor

    def begin(self, sql: str = None, args: tuple = None) -> None:
        """Starts a transaction on the underlying database connection."""
        self._connection.begin()

    def create(self, sql: str, args: tuple = None) -> None:
        cursor = self.query(sql, args)
        cursor.close()
        if not self._connection.in_transaction:
            self.save()

    def insert(self, sql: str, args: tuple = None) -> int:
        """Inserts data into a table and returns the last row id."""
        cursor = self.query(sql, args)
        id = cursor.lastrowid
        cursor.close()
        if not self._connection.in_transaction:
            self.save()
        return id

    def selectone(self, sql: str, args: tuple = None) -> tuple:
        """Performs a select query returning a single instance or row."""
        row = None
        cursor = self.query(sql, args)
        row = cursor.fetchone()
        cursor.close()
        if not self._connection.in_transaction:
            self.save()
        return row

    def selectall(self, sql: str, args: tuple = None) -> list:
        """Performs a select query returning multiple instances or rows."""
        rows = []
        cursor = self.query(sql, args)
        rows = cursor.fetchall()
        cursor.close()
        if not self._connection.in_transaction:
            self.save()
        return rows

    def update(self, sql: str, args: tuple = None) -> None:
        """Performs an update on existing data in the database."""
        cursor = self.query(sql, args)
        rowcount = cursor.rowcount
        cursor.close()
        if not self._connection.in_transaction:
            self.save()
        return rowcount

    def count(self, sql: str, args: tuple = None) -> int:
        """Counts the rows returned from a query."""
        cursor = self.query(sql, args)
        rows = cursor.fetchall()
        cursor.close()
        if not self._connection.in_transaction:
            self.save()
        return len(rows)

    def delete(self, sql: str, args: tuple = None) -> None:
        """Deletes existing data."""
        cursor = self.query(sql, args)
        cursor.close()
        if not self._connection.in_transaction:
            self.save()

    def drop(self, sql: str, args: tuple = None) -> None:
        """Drop a database or table."""
        cursor = self.query(sql, args)
        cursor.close()
        if not self._connection.in_transaction:
            self.save()

    def exists(self, sql: str, args: tuple = None) -> bool:
        """Returns True if the data specified by the parameters exists. Returns False otherwise."""
        cursor = self.query(sql, args)
        result = cursor.fetchone()
        cursor.close()
        if not self._connection.in_transaction:
            self.save()
        return result[0] == 1

    def save(self) -> None:
        """Saves changes to the database."""
        self._connection.commit()
