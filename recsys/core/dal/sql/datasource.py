#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/dal/sql/datasource.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 06:37:18 am                                                #
# Modified   : Saturday December 10th 2022 02:43:56 am                                             #
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
    name: str = "datasource"
    sql: str = """CREATE TABLE IF NOT EXISTS datasource (id INTEGER PRIMARY KEY, name TEXT NOT NULL, publisher TEXT NOT NULL, description TEXT NOT NULL, website TEXT NOT NULL, url TEXT NOT NULL, created timestamp, modified timestamp);"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DropDataSourceTable(SQL):
    name: str = "datasource"
    sql: str = """DROP TABLE IF EXISTS datasource;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class DataSourceTableExists(SQL):
    name: str = "datasource"
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
    sql: str = """INSERT INTO datasource (id, name, publisher, description, website, url, created, modified) VALUES (?,?,?,?,?,?,?,?);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.id,
            self.dto.name,
            self.dto.publisher,
            self.dto.description,
            self.dto.website,
            self.dto.url,
            self.dto.created,
            self.dto.modified,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UpdateDataSource(SQL):
    dto: DTO
    sql: str = """UPDATE datasource SET name = ?, publisher = ?, description = ?, website = ?, url = ?, created = ?, modified = ? WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.publisher,
            self.dto.description,
            self.dto.website,
            self.dto.url,
            self.dto.created,
            self.dto.modified,
            self.dto.id,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectDataSource(SQL):
    id: int
    sql: str = """SELECT * FROM datasource WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectAllDataSource(SQL):
    sql: str = """SELECT * FROM datasource;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #

@dataclass
class SelectAllDataSourceNames(SQL):
    sql: str = """SELECT name FROM datasource;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class DataSourceExists(SQL):
    id: int
    sql: str = """SELECT COUNT(*) FROM datasource WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DeleteDataSource(SQL):
    id: int
    sql: str = """DELETE FROM datasource WHERE id = ?;"""
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
    select_all_names: type(SQL) = SelectAllDataSourceNames
    exists: type(SQL) = DataSourceExists
    delete: type(SQL) = DeleteDataSource
