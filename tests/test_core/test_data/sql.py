#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /sql.py                                                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday December 3rd 2022 09:09:28 am                                              #
# Modified   : Saturday December 3rd 2022 11:13:45 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass

# ------------------------------------------------------------------------------------------------ #


@dataclass
class CreateTestTable:
    sql: str = """CREATE TABLE IF NOT EXISTS test_table ( id INTEGER PRIMARY KEY, name TEXT NOT NULL, description TEXT NOT NULL);"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DropTestTable:
    sql: str = """DROP TABLE IF EXISTS test_table;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TestTableExists:
    table: str = "test_table"
    sql: str = """SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.table,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class InsertTest:
    """All attributes of a Dataset are included; however, two are not used - namely id, and data."""

    name: str = "test_row"
    description: str = "A Test Row"

    sql: str = """INSERT INTO test_table (name,description) VALUES (?,?);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.name, self.description)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class SelectTest:
    id: int
    sql: str = """SELECT * FROM test_table WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class UpdateTest:
    id: int
    name: str = "test_updated_name"
    sql: str = """UPDATE test_table SET name = ? WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.name = f"{self.name}_{self.id}"
        self.args = (
            self.name,
            self.id,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class CountTest:
    sql: str = """SELECT COUNT(*) FROM test_table;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DeleteTest:
    id: int
    sql: str = """DELETE FROM test_table WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class ExistsTest:
    id: int
    sql: str = """SELECT COUNT(*) FROM test_table WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)
