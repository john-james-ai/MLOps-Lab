#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# DataSourcename   : /mlops_lab/core/dal/sql/datasource.py                                            #
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
from mlops_lab.core.entity.datasource import DataSource

# ================================================================================================ #
#                                        DATASET                                                   #
# ================================================================================================ #


# ------------------------------------------------------------------------------------------------ #
#                                          DDL                                                     #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateDataSourceTable(SQL):
    name: str = "datasource"
    sql: str = """CREATE TABLE IF NOT EXISTS datasource (id MEDIUMINT PRIMARY KEY AUTO_INCREMENT, oid VARCHAR(255) NOT NULL, name VARCHAR(128) NOT NULL, description VARCHAR(255), website VARCHAR(255) NOT NULL, created DATETIME DEFAULT CURRENT_TIMESTAMP, modified DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, UNIQUE(name));"""
    args: tuple = ()
    description: str = "Created the datasource table"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DropDataSourceTable(SQL):
    name: str = "datasource"
    sql: str = """DROP TABLE IF EXISTS datasource;"""
    args: tuple = ()
    description: str = "Dropped the datasource table"


# ------------------------------------------------------------------------------------------------ #


@dataclass
class DataSourceTableExists(SQL):
    name: str = "datasource"
    sql: str = None
    args: tuple = ()
    description: str = "Checked existence of datasource table."

    def __post_init__(self) -> None:
        dotenv.load_dotenv()
        mode = os.getenv("MODE")
        self.sql = f"""SELECT COUNT(TABLE_NAME) FROM information_schema.TABLES WHERE TABLE_SCHEMA LIKE 'mlops_lab_{mode}' AND TABLE_NAME = 'datasource';"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataSourceDDL(DDL):
    entity: type[Entity] = DataSource
    create: SQL = CreateDataSourceTable()
    drop: SQL = DropDataSourceTable()
    exists: SQL = DataSourceTableExists()


# ------------------------------------------------------------------------------------------------ #
#                                          DML                                                     #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class InsertDataSource(SQL):
    dto: DTO
    sql: str = (
        """INSERT INTO datasource (oid, name, description, website) VALUES (%s, %s, %s, %s);"""
    )
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.oid,
            self.dto.name,
            self.dto.description,
            self.dto.website,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UpdateDataSource(SQL):
    dto: DTO
    sql: str = """UPDATE datasource SET oid = %s, name = %s, description = %s, website = %s WHERE id = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.oid,
            self.dto.name,
            self.dto.description,
            self.dto.website,
            self.dto.id,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectDataSource(SQL):
    id: int
    sql: str = """SELECT * FROM datasource WHERE id = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectDataSourceByName(SQL):
    name: str
    sql: str = """SELECT * FROM datasource WHERE name = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.name,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectAllDataSource(SQL):
    sql: str = """SELECT * FROM datasource;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class DataSourceExists(SQL):
    id: int
    sql: str = """SELECT EXISTS(SELECT 1 FROM datasource WHERE id = %s LIMIT 1);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DeleteDataSource(SQL):
    id: int
    sql: str = """DELETE FROM datasource WHERE id = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LoadDataSource(SQL):
    filename: str
    tablename: str = "datasource"
    sql: str = None
    args: tuple = ()

    def __post_init__(self) -> None:
        self.sql = f"""LOAD DATA LOCAL INFILE '{self.filename}' INTO TABLE {self.tablename} FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataSourceDML(DML):
    entity: type[Entity] = DataSource
    insert: type[SQL] = InsertDataSource
    update: type[SQL] = UpdateDataSource
    select: type[SQL] = SelectDataSource
    select_by_name: type[SQL] = SelectDataSourceByName
    select_all: type[SQL] = SelectAllDataSource
    exists: type[SQL] = DataSourceExists
    delete: type[SQL] = DeleteDataSource
    load: type[SQL] = LoadDataSource
