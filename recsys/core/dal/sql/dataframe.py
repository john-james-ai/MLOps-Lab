#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/dal/sql/dataframe.py                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 06:37:18 am                                                #
# Modified   : Wednesday December 28th 2022 03:05:11 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass
from recsys.core.dal.sql.base import SQL, DDL, DML
from recsys.core.dal.dto import DTO
# ================================================================================================ #
#                                        DATASET                                                   #
# ================================================================================================ #


# ------------------------------------------------------------------------------------------------ #
#                                          DDL                                                     #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateDataFrameTable(SQL):
    name: str = "dataframe"
    sql: str = """CREATE TABLE IF NOT EXISTS dataframe (id INTEGER PRIMARY KEY, oid TEXT GENERATED ALWAYS AS ('DataFrame_' || id), name TEXT NOT NULL, description TEXT, datasource TEXT NOT NULL, mode TEXT NOT NULL, stage TEXT NOT NULL, size INTEGER, nrows INTEGER, ncols INTEGER, nulls INTEGER, pct_nulls REAL, parent_id INTEGER, created timestamp, modified timestamp);CREATE UNIQUE INDEX name_mode ON dataframe(name, mode);"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DropDataFrameTable(SQL):
    name: str = "dataframe"
    sql: str = """DROP TABLE IF EXISTS dataframe;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class DataFrameTableExists(SQL):
    name: str = "dataframe"
    sql: str = """SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.name,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataFrameDDL(DDL):
    create: SQL = CreateDataFrameTable()
    drop: SQL = DropDataFrameTable()
    exists: SQL = DataFrameTableExists()


# ------------------------------------------------------------------------------------------------ #
#                                          DML                                                     #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class InsertDataFrame(SQL):
    dto: DTO
    sql: str = """REPLACE INTO dataframe (name, description, datasource, mode, stage, size, nrows, ncols, nulls, pct_nulls, parent_id, created, modified) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.description,
            self.dto.datasource,
            self.dto.mode,
            self.dto.stage,
            self.dto.size,
            self.dto.nrows,
            self.dto.ncols,
            self.dto.nulls,
            self.dto.pct_nulls,
            self.dto.parent_id,
            self.dto.created,
            self.dto.modified,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UpdateDataFrame(SQL):
    dto: DTO
    sql: str = """UPDATE dataframe SET name = ?, description = ?, datasource = ?, mode = ?, stage = ?, size = ?, nrows = ?, ncols = ?, nulls = ?, pct_nulls = ?, parent_id = ?, created = ?, modified = ?  WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.description,
            self.dto.datasource,
            self.dto.mode,
            self.dto.stage,
            self.dto.size,
            self.dto.nrows,
            self.dto.ncols,
            self.dto.nulls,
            self.dto.pct_nulls,
            self.dto.parent_id,
            self.dto.created,
            self.dto.modified,
            self.dto.id,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectDataFrame(SQL):
    id: int
    sql: str = """SELECT * FROM dataframe WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)

# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectDataFrameByNameMode(SQL):
    name: str
    mode: str
    sql: str = """SELECT * FROM dataframe WHERE name = ? AND mode = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.name, self.mode,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectAllDataset(SQL):
    sql: str = """SELECT * FROM dataframe;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class DataFrameExists(SQL):
    id: int
    sql: str = """SELECT COUNT(*) FROM dataframe WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DeleteDataFrame(SQL):
    id: int
    sql: str = """DELETE FROM dataframe WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataFrameDML(DML):
    insert: type(SQL) = InsertDataFrame
    update: type(SQL) = UpdateDataFrame
    select: type(SQL) = SelectDataFrame
    select_by_name_mode: type(SQL) = SelectDataFrameByNameMode
    select_all: type(SQL) = SelectAllDataset
    exists: type(SQL) = DataFrameExists
    delete: type(SQL) = DeleteDataFrame
