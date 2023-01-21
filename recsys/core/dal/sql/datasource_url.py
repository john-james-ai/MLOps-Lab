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
# Modified   : Saturday January 21st 2023 02:51:21 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import dotenv

from dataclasses import dataclass
from recsys.core.dal.sql.base import SQL, DDL, DML
from recsys.core.dal.dto import DTO
from recsys.core.entity.base import Entity
from recsys.core.entity.datasource import DataSourceURL

# ================================================================================================ #
#                                        DATASET                                                   #
# ================================================================================================ #


# ------------------------------------------------------------------------------------------------ #
#                                          DDL                                                     #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateDataSourceURLTable(SQL):
    name: str = "datasource_url"
    sql: str = """CREATE TABLE IF NOT EXISTS datasource_url (id MEDIUMINT PRIMARY KEY AUTO_INCREMENT, oid VARCHAR(255) NOT NULL, name VARCHAR(128) NOT NULL, description VARCHAR(255), url VARCHAR(255) NOT NULL, parent_oid VARCHAR(128) NOT NULL, created DATETIME DEFAULT CURRENT_TIMESTAMP, modified DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, UNIQUE(name));"""
    args: tuple = ()
    description: str = "Created the datasource URL table."


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DropDataSourceURLTable(SQL):
    name: str = "datasource_url"
    sql: str = """DROP TABLE IF EXISTS datasource_url;"""
    args: tuple = ()
    description: str = "Dropped the datasource URL table."


# ------------------------------------------------------------------------------------------------ #


@dataclass
class DataSourceURLTableExists(SQL):
    name: str = "datasource_url"
    sql: str = None
    args: tuple = ()
    description: str = "Checked existence of datasource_url table."

    def __post_init__(self) -> None:
        dotenv.load_dotenv()
        mode = os.getenv("MODE")
        self.sql = f"""SELECT COUNT(TABLE_NAME) FROM information_schema.TABLES WHERE TABLE_SCHEMA LIKE 'recsys_{mode}' AND TABLE_NAME = 'datasource_url';"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataSourceURLDDL(DDL):
    entity: type[Entity] = DataSourceURL
    create: SQL = CreateDataSourceURLTable()
    drop: SQL = DropDataSourceURLTable()
    exists: SQL = DataSourceURLTableExists()


# ------------------------------------------------------------------------------------------------ #
#                                          DML                                                     #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class InsertDataSourceURL(SQL):
    dto: DTO
    sql: str = """INSERT INTO datasource_url (oid, name, description, url, parent_oid) VALUES (%s, %s, %s, %s, %s);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.oid,
            self.dto.name,
            self.dto.description,
            self.dto.url,
            self.dto.parent_oid,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UpdateDataSourceURL(SQL):
    dto: DTO
    sql: str = """UPDATE datasource_url SET oid = %s, name = %s, description = %s, url = %s, parent_oid = %s WHERE id = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.oid,
            self.dto.name,
            self.dto.description,
            self.dto.url,
            self.dto.parent_oid,
            self.dto.id,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectDataSourceURL(SQL):
    id: int
    sql: str = """SELECT * FROM datasource_url WHERE id = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectDataSourceURLByParentId(SQL):
    parent_oid: str
    sql: str = """SELECT * FROM datasource_url WHERE parent_oid = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.parent_oid,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectDataSourceURLByName(SQL):
    name: str
    sql: str = """SELECT * FROM datasource_url WHERE name = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.name,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectAllDataSourceURL(SQL):
    sql: str = """SELECT * FROM datasource_url;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class DataSourceURLExists(SQL):
    id: int
    sql: str = """SELECT EXISTS(SELECT 1 FROM datasource_url WHERE id = %s LIMIT 1);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DeleteDataSourceURL(SQL):
    id: int
    sql: str = """DELETE FROM datasource_url WHERE id = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataSourceURLDML(DML):
    entity: type[Entity] = DataSourceURL
    insert: type[SQL] = InsertDataSourceURL
    update: type[SQL] = UpdateDataSourceURL
    select: type[SQL] = SelectDataSourceURL
    select_by_name: type[SQL] = SelectDataSourceURLByName
    select_by_parent_oid: type[SQL] = SelectDataSourceURLByParentId
    select_all: type[SQL] = SelectAllDataSourceURL
    exists: type[SQL] = DataSourceURLExists
    delete: type[SQL] = DeleteDataSourceURL
