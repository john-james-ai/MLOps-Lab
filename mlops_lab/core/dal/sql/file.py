#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /mlops_lab/core/dal/sql/file.py                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 06:37:18 am                                                #
# Modified   : Tuesday January 24th 2023 08:13:43 pm                                               #
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
from mlops_lab.core.entity.file import File

# ================================================================================================ #
#                                          FILE                                                    #
# ================================================================================================ #


# ------------------------------------------------------------------------------------------------ #
#                                          DDL                                                     #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateFileTable(SQL):
    name: str = "file"
    sql: str = """CREATE TABLE IF NOT EXISTS file (id MEDIUMINT PRIMARY KEY AUTO_INCREMENT, oid VARCHAR(255) NOT NULL, name VARCHAR(128) NOT NULL, description VARCHAR(255), datasource_oid SMALLINT NOT NULL, stage VARCHAR(64) NOT NULL, uri VARCHAR(255) NOT NULL, size BIGINT DEFAULT 0, task_oid VARCHAR(255), created DATETIME DEFAULT CURRENT_TIMESTAMP, modified DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, UNIQUE(name));"""
    args: tuple = ()
    description: str = "Created the file table"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DropFileTable(SQL):
    name: str = "file"
    sql: str = """DROP TABLE IF EXISTS file;"""
    args: tuple = ()
    description: str = "Dropped the file table."


# ------------------------------------------------------------------------------------------------ #


@dataclass
class FileTableExists(SQL):
    name: str = "file"
    sql: str = None
    args: tuple = ()
    description: str = "Checked existence of file table."

    def __post_init__(self) -> None:
        dotenv.load_dotenv()
        mode = os.getenv("MODE")
        self.sql = f"""SELECT COUNT(TABLE_NAME) FROM information_schema.TABLES WHERE TABLE_SCHEMA LIKE 'mlops_lab_{mode}' AND TABLE_NAME = 'file';"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class FileDDL(DDL):
    entity: type[Entity] = File
    create: SQL = CreateFileTable()
    drop: SQL = DropFileTable()
    exists: SQL = FileTableExists()


# ------------------------------------------------------------------------------------------------ #
#                                          DML                                                     #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class InsertFile(SQL):
    dto: DTO
    sql: str = """INSERT INTO file (oid, name, description, datasource_oid, stage, uri, size, task_oid) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.oid,
            self.dto.name,
            self.dto.description,
            self.dto.datasource_oid,
            self.dto.stage,
            self.dto.uri,
            self.dto.size,
            self.dto.task_oid,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UpdateFile(SQL):
    dto: DTO
    sql: str = """UPDATE file SET oid = %s, name = %s, description = %s, datasource_oid = %s, stage = %s, uri = %s, size = %s, task_oid = %s WHERE id = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.oid,
            self.dto.name,
            self.dto.description,
            self.dto.datasource_oid,
            self.dto.stage,
            self.dto.uri,
            self.dto.size,
            self.dto.task_oid,
            self.dto.id,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectFile(SQL):
    id: int
    sql: str = """SELECT * FROM file WHERE id = %s ;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectFileByName(SQL):
    name: str
    sql: str = """SELECT * FROM file WHERE name = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.name,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectAllFile(SQL):
    sql: str = """SELECT * FROM file;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class FileExists(SQL):
    id: int
    sql: str = """SELECT EXISTS(SELECT * FROM file WHERE id = %s);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DeleteFile(SQL):
    id: int
    sql: str = """DELETE FROM file WHERE id = %s ;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LoadFile(SQL):
    filename: str
    tablename: str = "file"
    sql: str = None
    args: tuple = ()

    def __post_init__(self) -> None:
        self.sql = f"""LOAD DATA LOCAL INFILE '{self.filename}' INTO TABLE {self.tablename} FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class FileDML(DML):
    entity: type[Entity] = File
    insert: type[SQL] = InsertFile
    update: type[SQL] = UpdateFile
    select: type[SQL] = SelectFile
    select_by_name: type[SQL] = SelectFileByName
    select_all: type[SQL] = SelectAllFile
    exists: type[SQL] = FileExists
    delete: type[SQL] = DeleteFile
    load: type[SQL] = LoadFile
