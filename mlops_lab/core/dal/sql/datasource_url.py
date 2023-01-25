#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /mlops_lab/core/dal/sql/datasource_url.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 06:37:18 am                                                #
# Modified   : Tuesday January 24th 2023 08:13:42 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import dotenv

from dataclasses import dataclass
from mlops_lab.core.dal.sql.base import SQL, DDL, DML
from mlops_lab.core.dal.dto import DTO
from mlops_lab.core.entity.base import Entity
from mlops_lab.core.entity.datasource import DataSourceURL

# ================================================================================================ #
#                                        DATASET                                                   #
# ================================================================================================ #


# ------------------------------------------------------------------------------------------------ #
#                                          DDL                                                     #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateDataSourceURLTable(SQL):
    name: str = "datasource_url"
    sql: str = """CREATE TABLE IF NOT EXISTS datasource_url (id MEDIUMINT PRIMARY KEY AUTO_INCREMENT, oid VARCHAR(255) NOT NULL, name VARCHAR(128) NOT NULL, description VARCHAR(255), url VARCHAR(255) NOT NULL, datasource_oid VARCHAR(128) NOT NULL, created DATETIME DEFAULT CURRENT_TIMESTAMP, modified DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, UNIQUE(name));"""
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
        self.sql = f"""SELECT COUNT(TABLE_NAME) FROM information_schema.TABLES WHERE TABLE_SCHEMA LIKE 'mlops_lab_{mode}' AND TABLE_NAME = 'datasource_url';"""


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
    sql: str = """INSERT INTO datasource_url (oid, name, description, url, datasource_oid) VALUES (%s, %s, %s, %s, %s);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.oid,
            self.dto.name,
            self.dto.description,
            self.dto.url,
            self.dto.datasource_oid,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UpdateDataSourceURL(SQL):
    dto: DTO
    sql: str = """UPDATE datasource_url SET oid = %s, name = %s, description = %s, url = %s, datasource_oid = %s WHERE id = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.oid,
            self.dto.name,
            self.dto.description,
            self.dto.url,
            self.dto.datasource_oid,
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
class SelectDataSourceURLByParentOid(SQL):
    datasource_oid: str
    sql: str = """SELECT * FROM datasource_url WHERE datasource_oid = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.datasource_oid,)


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
class LoadDataSourceURL(SQL):
    filename: str
    tablename: str = "datasource_url"
    sql: str = None
    args: tuple = ()

    def __post_init__(self) -> None:
        self.sql = f"""LOAD DATA LOCAL INFILE '{self.filename}' INTO TABLE {self.tablename} FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataSourceURLDML(DML):
    entity: type[Entity] = DataSourceURL
    insert: type[SQL] = InsertDataSourceURL
    update: type[SQL] = UpdateDataSourceURL
    select: type[SQL] = SelectDataSourceURL
    select_by_name: type[SQL] = SelectDataSourceURLByName
    select_by_parent_oid: type[SQL] = SelectDataSourceURLByParentOid
    select_all: type[SQL] = SelectAllDataSourceURL
    exists: type[SQL] = DataSourceURLExists
    delete: type[SQL] = DeleteDataSourceURL
    load: type[SQL] = LoadDataSourceURL
