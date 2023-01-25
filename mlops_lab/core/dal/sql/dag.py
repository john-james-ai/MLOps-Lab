#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /mlops_lab/core/dal/sql/dag.py                                                      #
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
from mlops_lab.core.workflow.dag import DAG

# ================================================================================================ #
#                                         JOB                                                      #
# ================================================================================================ #


# ------------------------------------------------------------------------------------------------ #
#                                          DDL                                                     #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateDAGTable(SQL):
    name: str = "dag"
    sql: str = """CREATE TABLE IF NOT EXISTS dag (id MEDIUMINT PRIMARY KEY AUTO_INCREMENT, oid VARCHAR(255) NOT NULL, name VARCHAR(128) NOT NULL, description VARCHAR(255), state VARCHAR(32), created DATETIME DEFAULT CURRENT_TIMESTAMP, modified DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, UNIQUE(name));"""
    args: tuple = ()
    description: str = "Created the dag table."


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DropDAGTable(SQL):
    name: str = "dag"
    sql: str = """DROP TABLE IF EXISTS dag;"""
    args: tuple = ()
    description: str = "Dropped the dag table."


# ------------------------------------------------------------------------------------------------ #


@dataclass
class DAGTableExists(SQL):
    name: str = "dag"
    sql: str = None
    args: tuple = ()
    description: str = "Checked existence of dag table."

    def __post_init__(self) -> None:
        dotenv.load_dotenv()
        mode = os.getenv("MODE")
        self.sql = f"""SELECT COUNT(TABLE_NAME) FROM information_schema.TABLES WHERE TABLE_SCHEMA LIKE 'mlops_lab_{mode}_events' AND TABLE_NAME = 'dag';"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DAGDDL(DDL):
    entity: type[Entity] = DAG
    create: SQL = CreateDAGTable()
    drop: SQL = DropDAGTable()
    exists: SQL = DAGTableExists()


# ------------------------------------------------------------------------------------------------ #
#                                          DML                                                     #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class InsertDAG(SQL):
    dto: DTO

    sql: str = """INSERT INTO dag (oid, name, description, state) VALUES (%s, %s, %s, %s);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.oid,
            self.dto.name,
            self.dto.description,
            self.dto.state,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UpdateDAG(SQL):
    dto: DTO
    sql: str = """UPDATE dag SET oid = %s, name = %s, description = %s, state = %s WHERE id = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.oid,
            self.dto.name,
            self.dto.description,
            self.dto.state,
            self.dto.id,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectDAG(SQL):
    id: int
    sql: str = """SELECT * FROM dag WHERE id = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectDAGByName(SQL):
    name: str
    sql: str = """SELECT * FROM dag WHERE name = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.name,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectAllDAG(SQL):
    sql: str = """SELECT * FROM dag;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class DAGExists(SQL):
    id: int
    sql: str = """SELECT EXISTS(SELECT 1 FROM dag WHERE id = %s LIMIT 1);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DeleteDAG(SQL):
    id: int
    sql: str = """DELETE FROM dag WHERE id = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LoadDAG(SQL):
    filename: str
    tablename: str = "dag"
    sql: str = None
    args: tuple = ()

    def __post_init__(self) -> None:
        self.sql = f"""LOAD DATA LOCAL INFILE '{self.filename}' INTO TABLE {self.tablename} FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DAGDML(DML):
    entity: type[Entity] = DAG
    insert: type[SQL] = InsertDAG
    update: type[SQL] = UpdateDAG
    select: type[SQL] = SelectDAG
    select_by_name: type[SQL] = SelectDAGByName
    select_all: type[SQL] = SelectAllDAG
    exists: type[SQL] = DAGExists
    delete: type[SQL] = DeleteDAG
    load: type[SQL] = LoadDAG
