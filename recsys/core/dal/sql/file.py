#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/dal/sql/file.py                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 06:37:18 am                                                #
# Modified   : Wednesday January 11th 2023 06:49:49 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass
from recsys.core.dal.sql.base import SQL, DDL, DML
from recsys.core.dal.dto import DTO
from recsys.core.entity.base import Entity
from recsys.core.entity.file import File
# ================================================================================================ #
#                                          FILE                                                    #
# ================================================================================================ #


# ------------------------------------------------------------------------------------------------ #
#                                          DDL                                                     #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateFileTable(SQL):
    name: str = "file"
    sql: str = """CREATE TABLE IF NOT EXISTS file (id MEDIUMINT PRIMARY KEY AUTO_INCREMENT, oid VARCHAR(255) NOT NULL, name VARCHAR(128) NOT NULL, description VARCHAR(255), datasource_id SMALLINT NOT NULL, stage VARCHAR(64) NOT NULL, uri VARCHAR(255) NOT NULL, size BIGINT DEFAULT 0, task_id MEDIUMINT DEFAULT 0, created DATETIME, modified DATETIME, UNIQUE(name));"""
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
    sql: str = """SELECT COUNT(TABLE_NAME) FROM information_schema.TABLES WHERE TABLE_NAME = 'file';"""
    args: tuple = ()
    description: str = "Checked existence of file table."


# ------------------------------------------------------------------------------------------------ #
@dataclass
class FileDDL(DDL):
    entity: type(Entity) = File
    create: SQL = CreateFileTable()
    drop: SQL = DropFileTable()
    exists: SQL = FileTableExists()


# ------------------------------------------------------------------------------------------------ #
#                                          DML                                                     #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class InsertFile(SQL):
    dto: DTO
    sql: str = """INSERT INTO file (oid, name, description, datasource_id, stage, uri, size, task_id, created, modified) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.oid,
            self.dto.name,
            self.dto.description,
            self.dto.datasource_id,
            self.dto.stage,
            self.dto.uri,
            self.dto.size,
            self.dto.task_id,
            self.dto.created,
            self.dto.modified,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UpdateFile(SQL):
    dto: DTO
    sql: str = """UPDATE file SET oid = %s, name = %s, description = %s, datasource_id = %s, stage = %s, uri = %s, size = %s, task_id = %s, created = %s, modified = %s WHERE id = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.oid,
            self.dto.name,
            self.dto.description,
            self.dto.datasource_id,
            self.dto.stage,
            self.dto.uri,
            self.dto.size,
            self.dto.task_id,
            self.dto.created,
            self.dto.modified,
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
class FileDML(DML):
    entity: type(Entity) = File
    insert: type(SQL) = InsertFile
    update: type(SQL) = UpdateFile
    select: type(SQL) = SelectFile
    select_by_name: type(SQL) = SelectFileByName
    select_all: type(SQL) = SelectAllFile
    exists: type(SQL) = FileExists
    delete: type(SQL) = DeleteFile
