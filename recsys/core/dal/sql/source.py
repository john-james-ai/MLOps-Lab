#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/dal/sql/source.py                                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 06:37:18 am                                                #
# Modified   : Tuesday December 6th 2022 06:07:24 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass
from recsys.core.dal.base import SQL, DDL, DML
from recsys.core.dal.dto import DTO


# ================================================================================================ #
#                                        DATASOURCE                                                #
# ================================================================================================ #


# ------------------------------------------------------------------------------------------------ #
#                                          DDL                                                     #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateDataSourceTable(SQL):
    name: str = "source"
    sql: str = """CREATE TABLE IF NOT EXISTS source (id INTEGER PRIMARY KEY, kind TEXT NOT NULL, name TEXT NOT NULL, description TEXT, website TEXT NOT NULL, link TEXT NOT NULL, filepath TEXT NOT NULL);"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DropDataSourceTable(SQL):
    name: str = "source"
    sql: str = """DROP TABLE IF EXISTS source;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class DataSourceTableExists(SQL):
    name: str = "source"
    sql: str = """SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.name,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataSourceDDL(DDL):
    create: SQL = CreateDataSourceTable()
    drop: SQL = DropDataSourceTable()
    exists: SQL = DataSourceTableExists()


# ------------------------------------------------------------------------------------------------ #
#                                          DML                                                     #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class InsertDataSource(SQL):
    dto: DTO

    sql: str = """INSERT INTO source (kind, name, description, website, link, filepath) VALUES (?,?,?,?,?,?,?);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.kind,
            self.dto.name,
            self.dto.description,
            self.dto.website,
            self.dto.link,
            self.dto.filepath,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UpdateDataSource(SQL):
    dto: DTO
    sql: str = """UPDATE source SET kind = ?, name = ?, description = ?, website = ?, link = ?, filepath = ? WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.kind,
            self.dto.name,
            self.dto.description,
            self.dto.website,
            self.dto.link,
            self.dto.filepath,
            self.dto.id,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectDataSource(SQL):
    id: int
    sql: str = """SELECT * FROM source WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectAllDataSource(SQL):
    sql: str = """SELECT * FROM source;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class DataSourceExists(SQL):
    id: int
    sql: str = """SELECT COUNT(*) FROM source WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DeleteDataSource(SQL):
    id: int
    sql: str = """DELETE FROM source WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataSourceDML(DML):
    insert: type(SQL) = InsertDataSource
    update: type(SQL) = UpdateDataSource
    select: type(SQL) = SelectDataSource
    select_all: type(SQL) = SelectAllDataSource
    exists: type(SQL) = DataSourceExists
    delete: type(SQL) = DeleteDataSource
