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
# Modified   : Wednesday January 4th 2023 12:42:37 pm                                              #
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

from .base import AbstractConnection, AbstractDatabase


# ------------------------------------------------------------------------------------------------ #
#                                    CONNECTION                                                    #
# ------------------------------------------------------------------------------------------------ #
class MySQLConnection(AbstractConnection):
    """MySQL Database."""

    def __init__(self, connector: pymysql.connect) -> None:
        super().__init__()
        self._connector = connector
        self._database = None
        self._in_transaction = False
        self.open()

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
        return self._connection.open

    @property
    def in_transaction(self) -> bool:
        """Returns the True if a transaction has been started."""
        return self._in_transaction

    def begin(self) -> None:
        """Start a transaction on the connection."""
        self._in_transaction = True
        self._connection.begin()
        self._logger.debug(f"{self.__class__.__name__}  transaction started.")

    def open(self) -> None:
        """Opens a database connection."""

        dotenv.load_dotenv()
        host = os.getenv("MYSQL_HOST")
        user = os.getenv("MYSQL_USER")
        password = os.getenv("MYSQL_PASSWORD")
        self._database = os.getenv("MYSQL_DATABASE")

        try:
            self._connection = self._connector(host=host, user=user, password=password, database=self._database, autocommit=False)
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
        try:
            cursor.execute(sql, args)
            return cursor
        except mysql.connector.Error as err:  # pragma: no cover
            self._logger.error(err)
            self._logger.error("Error Code: ", err.errno)
            self._logger.error("SQLSTATE: ", err.sqlstate)
            self._logger.error("Message: ", err.msg)
            raise mysql.connector.Error()

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

    def select(self, sql: str, args: tuple = None) -> tuple:
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
