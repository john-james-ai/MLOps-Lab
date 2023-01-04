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
# Created    : Tuesday January 3rd 2023 12:33:25 am                                                #
# Modified   : Tuesday January 3rd 2023 04:44:28 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Base Database and Connection Classes."""
from abc import abstractmethod
from typing import Union

from recsys.core.services.base import Service


# ------------------------------------------------------------------------------------------------ #
#                                        CONNECTION                                                #
# ------------------------------------------------------------------------------------------------ #
class AbstractConnection(Service):
    """Abstract base class for Database connections."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__()

    @property
    @abstractmethod
    def database(self) -> str:
        """Returns the name of the database to which the connection has been made."""

    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """Returns the True if the connection is open, False otherwise."""

    @property
    @abstractmethod
    def in_transaction(self) -> bool:
        """Returns the True if a transaction has been started."""

    @abstractmethod
    def begin(self) -> None:
        """Start a transaction on the connection."""

    @abstractmethod
    def close(self) -> None:
        """Closes the connection."""

    @abstractmethod
    def commit(self) -> None:
        """Commits the connection"""

    @abstractmethod
    def rollback(self) -> None:
        """Rolls back the database to the last commit."""


# ------------------------------------------------------------------------------------------------ #
#                                        DATABASE                                                  #
# ------------------------------------------------------------------------------------------------ #
class AbstractDatabase(Service):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def query(self, *args, **kwargs):
        """Executes a query on the database and returns a cursor object."""

    @abstractmethod
    def begin(self, *args, **kwargs) -> None:
        """Starts a transaction on the underlying database connection."""

    @abstractmethod
    def create(self, *args, **kwargs) -> None:
        """Create a database or table."""

    @abstractmethod
    def insert(self, *args, **kwargs) -> Union[int, None]:
        """Inserts data into a table and returns the last row id."""

    @abstractmethod
    def selectone(self, *args, **kwargs) -> list:
        """Performs a select query returning a single instance or row."""

    @abstractmethod
    def selectall(self, *args, **kwargs) -> list:
        """Performs a select query returning multiple instances or rows."""

    @abstractmethod
    def update(self, *args, **kwargs) -> None:
        """Performs an update on existing data in the database."""

    @abstractmethod
    def delete(self, *args, **kwargs) -> None:
        """Deletes existing data or database."""

    @abstractmethod
    def drop(self, *args, **kwargs) -> None:
        """Drop a database or table."""

    @abstractmethod
    def exists(self, *args, **kwargs) -> bool:
        """Returns True if the data specified by the parameters exists. Returns False otherwise."""

    @abstractmethod
    def save(self) -> None:
        """Saves changes to the database."""
