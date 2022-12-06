#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/dal/sql/operator.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 06:37:18 am                                                #
# Modified   : Tuesday December 6th 2022 06:07:06 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass
from recsys.core.dal.base import SQL, DDL, DML
from recsys.core.dal.dto import DTO

# ================================================================================================ #
#                                        OPERATOR                                                   #
# ================================================================================================ #


# ------------------------------------------------------------------------------------------------ #
#                                          DDL                                                     #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateOperatorTable(SQL):
    name: str = "operator"
    sql: str = """CREATE TABLE IF NOT EXISTS operator (id INTEGER PRIMARY KEY, name TEXT NOT NULL, description TEXT, module TEXT NOT NULL, classname TEXT NOT NULL, filepath TEXT NOT NULL);"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DropOperatorTable(SQL):
    name: str = "operator"
    sql: str = """DROP TABLE IF EXISTS operator;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class OperatorTableExists(SQL):
    name: str = "operator"
    sql: str = """SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.name,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class OperatorDDL(DDL):
    create: SQL = CreateOperatorTable()
    drop: SQL = DropOperatorTable()
    exists: SQL = OperatorTableExists()


# ------------------------------------------------------------------------------------------------ #
#                                          DML                                                     #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class InsertOperator(SQL):
    """All attributes of a Operator are included; however, two are not used - namely id, and data."""

    dto: DTO

    sql: str = """INSERT INTO operator (name, description, module, classname, filepath) VALUES (?,?,?,?,?);"""


def __post_init__(self) -> None:
    self.args = (
        self.dto.name,
        self.dto.description,
        self.dto.module,
        self.dto.classname,
        self.dto.filepath,
    )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UpdateOperator(SQL):
    dto: DTO
    sql: str = """UPDATE operator SET name = ?, description = ?, module = ?, classname = ?, filepath = ?, WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.description,
            self.dto.module,
            self.dto.classname,
            self.dto.filepath,
            self.dto.id,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectOperator(SQL):
    id: int
    sql: str = """SELECT * FROM operator WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectAllOperators(SQL):
    sql: str = """SELECT * FROM operator;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class OperatorExists(SQL):
    id: int
    sql: str = """SELECT COUNT(*) FROM operator WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DeleteOperator(SQL):
    id: int
    sql: str = """DELETE FROM operator WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class OperatorDML(DML):
    insert: type(SQL) = InsertOperator
    update: type(SQL) = UpdateOperator
    select: type(SQL) = SelectOperator
    select_all: type(SQL) = SelectAllOperators
    exists: type(SQL) = OperatorExists
    delete: type(SQL) = DeleteOperator
