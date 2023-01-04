#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/dal/sql/datasource_url.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 06:37:18 am                                                #
# Modified   : Tuesday January 3rd 2023 05:21:40 pm                                                #
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
class CreateDataSourceURLTable(SQL):
    name: str = "datasource_url"
    sql: str = """CREATE TABLE IF NOT EXISTS datasource_url (id MEDIUMINT PRIMARY KEY, oid VARCHAR(64) GENERATED ALWAYS AS CONCAT('datasource_url_', name, "_", id, "_", mode), name VARCHAR(64) NOT NULL, description VARCHAR(64), url VARCHAR(255) NOT NULL, mode VARCHAR(64) NOT NULL DEFAULT 'prod', datasource_id MEDIUMINT, created DATETIME, modified DATETIME, UNIQUE(name, mode));"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DropDataSourceURLTable(SQL):
    name: str = "datasource_url"
    sql: str = """DROP TABLE IF EXISTS datasource_url;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class DataSourceURLTableExists(SQL):
    name: str = "datasource_url"
    sql: str = """SELECT COUNT(TABLE_NAME) FROM information_schema.TABLES WHERE TABLE_NAME = 'datasource_url';"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataSourceURLDDL(DDL):
    create: SQL = CreateDataSourceURLTable()
    drop: SQL = DropDataSourceURLTable()
    exists: SQL = DataSourceURLTableExists()


# ------------------------------------------------------------------------------------------------ #
#                                          DML                                                     #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class InsertDataSourceURL(SQL):
    dto: DTO
    sql: str = """REPLACE INTO datasource_url (name, description, url, mode, datasource_id, created, modified) VALUES (?,?,?,?,?,?,?);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.description,
            self.dto.url,
            self.dto.mode,
            self.dto.datasource_id,
            self.dto.created,
            self.dto.modified,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UpdateDataSourceURL(SQL):
    dto: DTO
    sql: str = """UPDATE datasource_url SET name = ?, description = ?, url = ?, mode = ?, datasource_id = ?, created = ?, modified = ? WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.description,
            self.dto.url,
            self.dto.mode,
            self.dto.datasource_id,
            self.dto.created,
            self.dto.modified,
            self.dto.id,
        )


# ------------------------------------------------------------------------------------------------ #

@dataclass
class SelectDataSourceURL(SQL):
    id: int
    sql: str = """SELECT * FROM datasource_url WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #

@dataclass
class SelectDataSourceURLByParentId(SQL):
    datasource_id: int
    sql: str = """SELECT * FROM datasource_url WHERE datasource_id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.datasource_id,)

# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectDataSourceURLByNameMode(SQL):
    name: str
    mode: str
    sql: str = """SELECT * FROM datasource_url WHERE name = ? AND mode = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.name, self.mode,)


# ------------------------------------------------------------------------------------------------ #

@dataclass
class SelectAllDataSourceURL(SQL):
    sql: str = """SELECT * FROM datasource_url;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class DataSourceURLExists(SQL):
    id: int
    sql: str = """SELECT COUNT(*) FROM datasource_url WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DeleteDataSourceURL(SQL):
    id: int
    sql: str = """DELETE FROM datasource_url WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataSourceURLDML(DML):
    insert: type(SQL) = InsertDataSourceURL
    update: type(SQL) = UpdateDataSourceURL
    select: type(SQL) = SelectDataSourceURL
    select_by_name_mode: type(SQL) = SelectDataSourceURLByNameMode
    select_by_parent_id: type(SQL) = SelectDataSourceURLByParentId
    select_all: type(SQL) = SelectAllDataSourceURL
    exists: type(SQL) = DataSourceURLExists
    delete: type(SQL) = DeleteDataSourceURL
