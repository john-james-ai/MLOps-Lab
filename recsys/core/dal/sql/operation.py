#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/dal/sql/operation.py                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 06:37:18 am                                                #
# Modified   : Saturday December 17th 2022 03:38:52 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass
from recsys.core.dal.sql.base import SQL, DDL, DML
from recsys.core.dal.dto import DTO

# ================================================================================================ #
#                                           TASK                                                   #
# ================================================================================================ #


# ------------------------------------------------------------------------------------------------ #
#                                            DDL                                                   #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateOperationTable(SQL):
    name: str = "operation"
    sql: str = """CREATE TABLE IF NOT EXISTS operation (id INTEGER PRIMARY KEY, name TEXT NOT NULL, description TEXT, workspace TEXT NOT NULL, stage TEXT NOT NULL, uri TEXT NOT NULL, task_id INTEGER, created timestamp, modified timestamp);"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DropOperationTable(SQL):
    name: str = "operation"
    sql: str = """DROP TABLE IF EXISTS operation;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class OperationTableExists(SQL):
    name: str = "operation"
    sql: str = """SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.name,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class OperationDDL(DDL):
    create: SQL = CreateOperationTable()
    drop: SQL = DropOperationTable()
    exists: SQL = OperationTableExists()


# ------------------------------------------------------------------------------------------------ #
#                                          DML                                                     #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class InsertOperation(SQL):
    dto: DTO
    sql: str = """INSERT INTO operation (name, description, workspace, stage, uri, task_id, created, modified) VALUES (?,?,?,?,?,?,?,?);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.description,
            self.dto.workspace,
            self.dto.stage,
            self.dto.uri,
            self.dto.task_id,
            self.dto.created,
            self.dto.modified,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UpdateOperation(SQL):
    dto: DTO
    sql: str = """UPDATE operation SET name = ?, description = ?, workspace = ?, stage = ?, uri = ?, task_id = ?, created = ?, modified = ? WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.description,
            self.dto.workspace,
            self.dto.stage,
            self.dto.uri,
            self.dto.task_id,
            self.dto.created,
            self.dto.modified,
            self.dto.id,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectOperation(SQL):
    id: int
    sql: str = """SELECT * FROM operation WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)

# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectOperationByName(SQL):
    name: str
    sql: str = """SELECT * FROM operation WHERE name = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.name,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectAllOperations(SQL):
    sql: str = """SELECT * FROM operation;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #

@dataclass
class OperationExists(SQL):
    id: int
    sql: str = """SELECT COUNT(*) FROM operation WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DeleteOperation(SQL):
    id: int
    sql: str = """DELETE FROM operation WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class OperationDML(DML):
    insert: type(SQL) = InsertOperation
    update: type(SQL) = UpdateOperation
    select: type(SQL) = SelectOperation
    select_by_name: type(SQL) = SelectOperationByName
    select_all: type(SQL) = SelectAllOperations
    exists: type(SQL) = OperationExists
    delete: type(SQL) = DeleteOperation
