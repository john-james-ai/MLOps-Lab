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
# Modified   : Tuesday December 6th 2022 06:05:03 am                                               #
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
    sql: str = """CREATE TABLE IF NOT EXISTS dataset (id INTEGER PRIMARY KEY, name TEXT NOT NULL, description TEXT, source TEXT NOT NULL, workspace TEXT NOT NULL, stage TEXT NOT NULL, version INTEGER NOT NULL, cost INTEGER NOT NULL, nrows INTEGER NOT NULL, ncols INTEGER NOT NULL, null_counts INTEGER NOT NULL, memory_size_mb REAL NOT NULL, filename TEXT NOT NULL, filepath TEXT NOT NULL, task_id INTEGER DEFAULT (0), creator TEXT NOT NULL, created TEXT DEFAULT (datetime('now')), modified TEXT DEFAULT (datetime('now')));"""
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
    """All attributes of a Dataset are included; however, two are not used - namely id, and data."""

    dto: DTO

    sql: str = """INSERT INTO dataset (name, description, source, workspace, stage, version, cost, nrows, ncols, null_counts, memory_size_mb, filename, filepath, task_id, creator, created, modified) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.description,
            self.dto.source,
            self.dto.workspace,
            self.dto.stage,
            self.dto.version,
            self.dto.cost,
            self.dto.nrows,
            self.dto.ncols,
            self.dto.null_counts,
            self.dto.memory_size_mb,
            self.dto.filename,
            self.dto.filepath,
            self.dto.task_id,
            self.dto.creator,
            self.dto.created,
            self.dto.modified,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UpdateDataset(SQL):
    dto: DTO
    sql: str = """UPDATE dataset SET name = ?, description = ?, source = ?, workspace = ?, stage = ?, version = ?, cost = ?, nrows = ?, ncols = ?, null_counts = ?, memory_size_mb = ?, filename = ?, filepath = ?, task_id = ?, creator = ?, created = ?, modified = ? WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.description,
            self.dto.source,
            self.dto.workspace,
            self.dto.stage,
            self.dto.version,
            self.dto.cost,
            self.dto.nrows,
            self.dto.ncols,
            self.dto.null_counts,
            self.dto.memory_size_mb,
            self.dto.filename,
            self.dto.filepath,
            self.dto.task_id,
            self.dto.creator,
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
