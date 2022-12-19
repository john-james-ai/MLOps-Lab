#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/dal/sql/dataset_collection.py                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 06:37:18 am                                                #
# Modified   : Monday December 19th 2022 08:00:42 am                                               #
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
class CreateDatasetCollectionTable(SQL):
    name: str = "operation"
    sql: str = """CREATE TABLE IF NOT EXISTS operation (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, description TEXT, datasource TEXT NOT NULL, mode TEXT NOT NULL, stage TEXT NOT NULL, task_id INTEGER, created timestamp, modified timestamp);"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DropDatasetCollectionTable(SQL):
    name: str = "operation"
    sql: str = """DROP TABLE IF EXISTS operation;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class DatasetCollectionTableExists(SQL):
    name: str = "operation"
    sql: str = """SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.name,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetCollectionDDL(DDL):
    create: SQL = CreateDatasetCollectionTable()
    drop: SQL = DropDatasetCollectionTable()
    exists: SQL = DatasetCollectionTableExists()


# ------------------------------------------------------------------------------------------------ #
#                                          DML                                                     #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class InsertDatasetCollection(SQL):
    dto: DTO
    sql: str = """INSERT INTO operation (name, description, datasource, mode, stage, task_id, created, modified) VALUES (?,?,?,?,?,?,?,?);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.description,
            self.dto.datasource,
            self.dto.mode,
            self.dto.stage,
            self.dto.task_id,
            self.dto.created,
            self.dto.modified,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UpdateDatasetCollection(SQL):
    dto: DTO
    sql: str = """UPDATE operation SET name = ?, description = ?, datasource = ?, mode = ?, stage = ?, task_id = ?, created = ?, modified = ? WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.description,
            self.dto.datasource,
            self.dto.mode,
            self.dto.stage,
            self.dto.task_id,
            self.dto.created,
            self.dto.modified,
            self.dto.id,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectDatasetCollection(SQL):
    id: int
    sql: str = """SELECT * FROM operation WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)

# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectDatasetCollectionByName(SQL):
    name: str
    sql: str = """SELECT * FROM operation WHERE name = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.name,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectAllDatasetCollections(SQL):
    sql: str = """SELECT * FROM operation;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #

@dataclass
class DatasetCollectionExists(SQL):
    id: int
    sql: str = """SELECT COUNT(*) FROM operation WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DeleteDatasetCollection(SQL):
    id: int
    sql: str = """DELETE FROM operation WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetCollectionDML(DML):
    insert: type(SQL) = InsertDatasetCollection
    update: type(SQL) = UpdateDatasetCollection
    select: type(SQL) = SelectDatasetCollection
    select_by_name: type(SQL) = SelectDatasetCollectionByName
    select_all: type(SQL) = SelectAllDatasetCollections
    exists: type(SQL) = DatasetCollectionExists
    delete: type(SQL) = DeleteDatasetCollection
