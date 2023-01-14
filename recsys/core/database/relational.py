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
# Modified   : Friday January 13th 2023 09:11:25 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Relational Databases Module."""
import os
import pymysql
import dotenv
import mysql.connector
from mysql.connector import errorcode

from .base import Connection, AbstractDatabase


# ------------------------------------------------------------------------------------------------ #
#                                       MYSQL CONNECTION                                           #
# ------------------------------------------------------------------------------------------------ #
class MySQLConnection(Connection):
    """MySQL Database."""

    def __init__(
        self, connector: pymysql.connect, autocommit: bool = False, autoclose: bool = False
    ) -> None:
        super().__init__(connector=connector, autocommit=autocommit, autoclose=autoclose)
        self._database = "MySQL"

    def open(self) -> None:
        """Opens a database connection."""

        dotenv.load_dotenv()
        host = os.getenv("DBMS_HOST")
        user = os.getenv("DBMS_USER")
        password = os.getenv("DBMS_PASSWORD")

        try:
            self._connection = self._connector(
                host=host, user=user, password=password, autocommit=False
            )
            self._is_open = True
            self._logger.debug(f"{self.__class__.__name__} is connected.")
        except mysql.connector.Error as err:  # pragma: no cover
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                msg = "Invalid user name or password"
                self._logger.error(msg)
                raise mysql.connector.Error(msg)
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                msg = "Database does not exist"
                self._logger.error(msg)
                raise mysql.connector.Error(msg)
            else:
                self._logger.error(err)
                raise mysql.connector.Error()


# ------------------------------------------------------------------------------------------------ #
#                                      DATABASE CONNECTION                                         #
# ------------------------------------------------------------------------------------------------ #
class DatabaseConnection(Connection):
    """Connection to a Database."""

    def __init__(
        self,
        connector: pymysql.connect,
        database: str,
        autocommit: bool = False,
        autoclose: bool = False,
    ) -> None:
        super().__init__(connector=connector, autocommit=autocommit, autoclose=autoclose)
        self._database = database
        msg = f"Database connection on {self._database} is instantiated at {id(self._database)}."
        self._logger.debug(msg)

    def open(self) -> None:
        """Opens a database connection."""

        dotenv.load_dotenv()
        host = os.getenv("DATABASE_HOST")
        user = os.getenv("DATABASE_USER")
        password = os.getenv("DATABASE_PASSWORD")

        try:
            self._connection = self._connector(
                host=host,
                user=user,
                password=password,
                database=self._database,
                autocommit=self._autocommit,
            )
            self._is_open = True
            self._logger.debug(
                f"{self.__class__.__name__}.{self._database} is connected at {id(self._database)}."
            )
        except mysql.connector.Error as err:  # pragma: no cover
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                msg = "Invalid user name or password"
                self._logger.error(msg)
                raise mysql.connector.Error(msg)
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                msg = "Database does not exist"
                self._logger.error(msg)
                raise mysql.connector.Error(msg)
            else:
                self._logger.error(err)
                raise mysql.connector.Error()


# ------------------------------------------------------------------------------------------------ #
#                                        DATABASE                                                  #
# ------------------------------------------------------------------------------------------------ #
class Database(AbstractDatabase):
    def __init__(self, connection: Connection) -> None:
        super().__init__()
        self._connection = connection
        self._autocommit = connection.autocommit
        self._autoclose = connection.autoclose
        self._is_open = self._connection.is_open
        self._database = self._connection.database
        self._in_transaction = False

    @property
    def database(self) -> str:
        return self._database

    @property
    def in_transaction(self) -> bool:
        return self._in_transaction

    @property
    def is_open(self) -> bool:
        return self._is_open

    def connect(self) -> None:
        """Connects to the database."""
        self._connection.open()
        self._is_open = True

    def begin(self, sql: str = None, args: tuple = None) -> None:
        """Starts a transaction on the underlying database connection."""
        if not self._is_open:
            self.connect()
        self._connection.begin()
        self._in_transaction = True

    def close(self) -> None:
        """Closes the underlying database connection."""
        self._connection.close()
        self._is_open = False
        self._in_transaction = False

    def save(self) -> None:
        """Saves changes to the database."""
        self._connection.commit()
        self._in_transaction = False

    def rollback(self) -> None:
        """Rolls back the database to state as of last save or commit."""
        if self._in_transaction:
            self._connection.rollback()
            self._in_transaction = False

    def query(self, sql: str, args: tuple = None) -> Connection.cursor:
        """Executes a query on the database and returns a cursor object."""
        self._open_session()
        cursor = self._connection.cursor
        try:
            cursor.execute(sql, args)
        except mysql.connector.Error as err:  # pragma: no cover
            self._logger.error(err)
            self._logger.error("Error Code: ", err.errno)
            self._logger.error("SQLSTATE: ", err.sqlstate)
            self._logger.error("Message: ", err.msg)
            raise mysql.connector.Error()

        self._close_session()
        return cursor

    def create(self, sql: str, args: tuple = None) -> None:
        """Executes create DDL statements for databases and tables."""
        cursor = self.query(sql, args)
        cursor.close()

    def insert(self, sql: str, args: tuple = None) -> int:
        """Inserts data into a table and returns the last row id."""
        cursor = self.query(sql, args)
        id = cursor.lastrowid
        cursor.close()
        return id

    def select(self, sql: str, args: tuple = None) -> tuple:
        """Performs a select query returning a single instance or row."""
        row = None
        cursor = self.query(sql, args)
        row = cursor.fetchone()
        cursor.close()
        return row

    def select_all(self, sql: str, args: tuple = None) -> list:
        """Performs a select query returning multiple instances or rows."""
        rows = []
        cursor = self.query(sql, args)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def update(self, sql: str, args: tuple = None) -> None:
        """Performs an update on existing data in the database."""
        cursor = self.query(sql, args)
        rowcount = cursor.rowcount
        cursor.close()
        return rowcount

    def count(self, sql: str, args: tuple = None) -> int:
        """Counts the rows returned from a query."""
        cursor = self.query(sql, args)
        rows = cursor.fetchall()
        cursor.close()
        return len(rows)

    def delete(self, sql: str, args: tuple = None) -> None:
        """Deletes existing data."""
        cursor = self.query(sql, args)
        cursor.close()

    def drop(self, sql: str, args: tuple = None) -> None:
        """Drop a database or table."""
        cursor = self.query(sql, args)
        cursor.close()

    def exists(self, sql: str, args: tuple = None) -> bool:
        """Returns True if the data specified by the parameters exists. Returns False otherwise."""
        result = []
        cursor = self.query(sql, args)
        result = cursor.fetchone()
        cursor.close()
        self._logger.debug(result)
        if result == []:
            return False
        else:
            return result[0] == 1

    def _open_session(self) -> None:
        """Opens a database connection if not already open."""
        if not self._is_open:
            self.connect()

    def _close_session(self) -> None:
        """Saves and closes the connection, if not in transaction."""
        if not self._in_transaction and self._autocommit:
            self.save()
        if not self._in_transaction and self._autoclose:
            self.close()
