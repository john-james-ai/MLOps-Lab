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
# Modified   : Saturday January 14th 2023 05:06:03 am                                              #
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
from recsys.core.entity.dataset import Dataset

# ================================================================================================ #
#                                        DATASET                                                   #
# ================================================================================================ #


# ------------------------------------------------------------------------------------------------ #
#                                          DDL                                                     #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateDatasetTable(SQL):
    name: str = "dataset"
    sql: str = """CREATE TABLE IF NOT EXISTS dataset (id MEDIUMINT PRIMARY KEY AUTO_INCREMENT, oid VARCHAR(255) NOT NULL, name VARCHAR(128) NOT NULL, description VARCHAR(255), datasource_oid VARCHAR(255) NOT NULL, stage VARCHAR(64) NOT NULL, task_oid VARCHAR(255), created DATETIME DEFAULT CURRENT_TIMESTAMP, modified DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, UNIQUE(name));"""
    args: tuple = ()
    description: str = "Created the dataset table."


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DropDatasetTable(SQL):
    name: str = "dataset"
    sql: str = """DROP TABLE IF EXISTS dataset;"""
    args: tuple = ()
    description: str = "Dropped the dataset table."


# ------------------------------------------------------------------------------------------------ #


@dataclass
class DatasetTableExists(SQL):
    name: str = "dataset"
    sql: str = None
    args: tuple = ()
    description: str = "Checked existence of dataset table."

    def __post_init__(self) -> None:
        dotenv.load_dotenv()
        mode = os.getenv("MODE")
        self.sql = f"""SELECT COUNT(TABLE_NAME) FROM information_schema.TABLES WHERE TABLE_SCHEMA LIKE 'recsys_{mode}' AND TABLE_NAME = 'dataset';"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetDDL(DDL):
    entity: type[Entity] = Dataset
    create: SQL = CreateDatasetTable()
    drop: SQL = DropDatasetTable()
    exists: SQL = DatasetTableExists()


# ------------------------------------------------------------------------------------------------ #
#                                          DML                                                     #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class InsertDataset(SQL):
    dto: DTO
    sql: str = """INSERT INTO dataset (oid, name, description, datasource_oid, stage, task_oid) VALUES (%s, %s, %s, %s, %s, %s);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.oid,
            self.dto.name,
            self.dto.description,
            self.dto.datasource_oid,
            self.dto.stage,
            self.dto.task_oid,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UpdateDataset(SQL):
    dto: DTO
    sql: str = """UPDATE dataset SET oid = %s, name = %s, description = %s, datasource_oid = %s, stage = %s, task_oid = %s WHERE id = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.oid,
            self.dto.name,
            self.dto.description,
            self.dto.datasource_oid,
            self.dto.stage,
            self.dto.task_oid,
            self.dto.id,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectDataset(SQL):
    id: int
    sql: str = """SELECT * FROM dataset WHERE id = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectDatasetByName(SQL):
    name: str
    sql: str = """SELECT * FROM dataset WHERE name = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.name,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectAllDataset(SQL):
    sql: str = """SELECT * FROM dataset;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class DatasetExists(SQL):
    id: int
    sql: str = """SELECT EXISTS(SELECT 1 FROM dataset WHERE id = %s LIMIT 1);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DeleteDataset(SQL):
    id: int
    sql: str = """DELETE FROM dataset WHERE id = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetDML(DML):
    entity: type[Entity] = Dataset
    insert: type[SQL] = InsertDataset
    update: type[SQL] = UpdateDataset
    select: type[SQL] = SelectDataset
    select_by_name: type[SQL] = SelectDatasetByName
    select_all: type[SQL] = SelectAllDataset
    exists: type[SQL] = DatasetExists
    delete: type[SQL] = DeleteDataset
