#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/dal/sql/dataset.py                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 06:37:18 am                                                #
# Modified   : Sunday December 11th 2022 03:17:34 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass
from recsys.core.dal.base import SQL, DDL, DML
from recsys.core.dal.dto import DTO

# ================================================================================================ #
#                                        DATASET                                                   #
# ================================================================================================ #


# ------------------------------------------------------------------------------------------------ #
#                                          DDL                                                     #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateDatasetTable(SQL):
    name: str = "dataset"
    sql: str = """CREATE TABLE IF NOT EXISTS dataset (id INTEGER PRIMARY KEY, name TEXT NOT NULL, description TEXT, datasource TEXT NOT NULL, workspace TEXT NOT NULL, stage TEXT NOT NULL, filepath TEXT, size INTEGER, nrows INTEGER, ncols INTEGER, nulls INTEGER, pct_nulls REAL, task_id INTEGER, created timestamp, modified timestamp);"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DropDatasetTable(SQL):
    name: str = "dataset"
    sql: str = """DROP TABLE IF EXISTS dataset;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class DatasetTableExists(SQL):
    name: str = "dataset"
    sql: str = """SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.name,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetDDL(DDL):
    create: SQL = CreateDatasetTable()
    drop: SQL = DropDatasetTable()
    exists: SQL = DatasetTableExists()


# ------------------------------------------------------------------------------------------------ #
#                                          DML                                                     #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class InsertDataset(SQL):
    dto: DTO
    sql: str = """INSERT INTO dataset (name, description, datasource, workspace, stage, filepath, size, nrows, ncols, nulls, pct_nulls,  task_id, created, modified) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.description,
            self.dto.datasource,
            self.dto.workspace,
            self.dto.stage,
            self.dto.filepath,
            self.dto.size,
            self.dto.nrows,
            self.dto.ncols,
            self.dto.nulls,
            self.dto.pct_nulls,
            self.dto.task_id,
            self.dto.created,
            self.dto.modified,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UpdateDataset(SQL):
    dto: DTO
    sql: str = """UPDATE dataset SET name = ?, description = ?, datasource = ?, workspace = ?, stage = ?, filepath = ?, size = ?, nrows = ?, ncols = ?, nulls = ?, pct_nulls = ?, task_id = ?, created = ?, modified = ? WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.description,
            self.dto.datasource,
            self.dto.workspace,
            self.dto.stage,
            self.dto.filepath,
            self.dto.size,
            self.dto.nrows,
            self.dto.ncols,
            self.dto.nulls,
            self.dto.pct_nulls,
            self.dto.task_id,
            self.dto.created,
            self.dto.modified,
            self.dto.id,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectDataset(SQL):
    id: int
    sql: str = """SELECT * FROM dataset WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectAllDatasets(SQL):
    sql: str = """SELECT * FROM dataset;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class DatasetExists(SQL):
    id: int
    sql: str = """SELECT COUNT(*) FROM dataset WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DeleteDataset(SQL):
    id: int
    sql: str = """DELETE FROM dataset WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetDML(DML):
    insert: type(SQL) = InsertDataset
    update: type(SQL) = UpdateDataset
    select: type(SQL) = SelectDataset
    select_all: type(SQL) = SelectAllDatasets
    exists: type(SQL) = DatasetExists
    delete: type(SQL) = DeleteDataset
