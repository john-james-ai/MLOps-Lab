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
# Modified   : Sunday January 22nd 2023 02:19:22 pm                                                #
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
from recsys.core.entity.dataset import DataFrame

# ================================================================================================ #
#                                        DATASET                                                   #
# ================================================================================================ #


# ------------------------------------------------------------------------------------------------ #
#                                          DDL                                                     #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateDataFrameTable(SQL):
    name: str = "dataframe"
    sql: str = """CREATE TABLE IF NOT EXISTS dataframe (id MEDIUMINT PRIMARY KEY AUTO_INCREMENT, oid VARCHAR(255) NOT NULL, name VARCHAR(128) NOT NULL, description VARCHAR(255), stage VARCHAR(64) NOT NULL, size BIGINT, nrows BIGINT, ncols SMALLINT, nulls SMALLINT, pct_nulls FLOAT, dataset_oid VARCHAR(128) NOT NULL, created DATETIME DEFAULT CURRENT_TIMESTAMP, modified DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, UNIQUE(name));"""
    args: tuple = ()
    description: str = "Created the dataframe table."


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DropDataFrameTable(SQL):
    name: str = "dataframe"
    sql: str = """DROP TABLE IF EXISTS dataframe;"""
    args: tuple = ()
    description: str = "Dropped the dataframe table."


# ------------------------------------------------------------------------------------------------ #


@dataclass
class DataFrameTableExists(SQL):
    name: str = "dataframe"
    sql: str = None
    args: tuple = ()
    description: str = "Checked existence of dataframe table."

    def __post_init__(self) -> None:
        dotenv.load_dotenv()
        mode = os.getenv("MODE")
        self.sql = f"""SELECT COUNT(TABLE_NAME) FROM information_schema.TABLES WHERE TABLE_SCHEMA LIKE 'recsys_{mode}' AND TABLE_NAME = 'dataframe';"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataFrameDDL(DDL):
    entity: type[Entity] = DataFrame
    create: SQL = CreateDataFrameTable()
    drop: SQL = DropDataFrameTable()
    exists: SQL = DataFrameTableExists()


# ------------------------------------------------------------------------------------------------ #
#                                          DML                                                     #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class InsertDataFrame(SQL):
    dto: DTO
    sql: str = """INSERT INTO dataframe (oid, name, description, stage, size, nrows, ncols, nulls, pct_nulls, dataset_oid) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.oid,
            self.dto.name,
            self.dto.description,
            self.dto.stage,
            self.dto.size,
            self.dto.nrows,
            self.dto.ncols,
            self.dto.nulls,
            self.dto.pct_nulls,
            self.dto.dataset_oid,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UpdateDataFrame(SQL):
    dto: DTO
    sql: str = """UPDATE dataframe SET oid = %s, name = %s, description = %s, stage = %s, size = %s, nrows = %s, ncols = %s, nulls = %s, pct_nulls = %s, dataset_oid = %s WHERE id = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.oid,
            self.dto.name,
            self.dto.description,
            self.dto.stage,
            self.dto.size,
            self.dto.nrows,
            self.dto.ncols,
            self.dto.nulls,
            self.dto.pct_nulls,
            self.dto.dataset_oid,
            self.dto.id,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectDataFrame(SQL):
    id: int
    sql: str = """SELECT * FROM dataframe WHERE id = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectDataFrameByParentOid(SQL):
    dataset_oid: str
    sql: str = """SELECT * FROM dataframe WHERE dataset_oid = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.dataset_oid,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectDataFrameByName(SQL):
    name: str
    sql: str = """SELECT * FROM dataframe WHERE name = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = self.name


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectAllDataset(SQL):
    sql: str = """SELECT * FROM dataframe;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class DataFrameExists(SQL):
    id: int
    sql: str = """SELECT EXISTS(SELECT 1 FROM dataframe WHERE id = %s LIMIT 1);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DeleteDataFrame(SQL):
    id: int
    sql: str = """DELETE FROM dataframe WHERE id = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LoadDataFrame(SQL):
    filename: str
    tablename: str = "dataframe"
    sql: str = None
    args: tuple = ()

    def __post_init__(self) -> None:
        self.sql = f"""LOAD DATA LOCAL INFILE '{self.filename}' INTO TABLE {self.tablename} FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataFrameDML(DML):
    entity: type[Entity] = DataFrame
    insert: type[SQL] = InsertDataFrame
    update: type[SQL] = UpdateDataFrame
    select: type[SQL] = SelectDataFrame
    select_by_name: type[SQL] = SelectDataFrameByName
    select_by_dataset_id: type[SQL] = SelectDataFrameByParentOid
    select_all: type[SQL] = SelectAllDataset
    exists: type[SQL] = DataFrameExists
    delete: type[SQL] = DeleteDataFrame
    load: type[SQL] = LoadDataFrame
