#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/dal/sql/event.py                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday December 8th 2022 02:06:04 pm                                              #
# Modified   : Saturday January 21st 2023 03:41:53 am                                              #
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
from recsys.core.workflow.event import Event

# ================================================================================================ #
#                                        PROFILE                                                   #
# ================================================================================================ #


# ------------------------------------------------------------------------------------------------ #
#                                          DDL                                                     #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateEventTable(SQL):
    name: str = "event"
    sql: str = """CREATE TABLE IF NOT EXISTS event (id MEDIUMINT PRIMARY KEY AUTO_INCREMENT, oid VARCHAR(255) NOT NULL, name VARCHAR(128) NOT NULL, description VARCHAR(64), process_type VARCHAR(128) NOT NULL, process_oid VARCHAR(128) NOT NULL, parent_oid VARCHAR(128) NOT NULL, created DATETIME DEFAULT CURRENT_TIMESTAMP, modified DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP);"""
    args: tuple = ()
    description: str = "Created the event table."


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DropEventTable(SQL):
    name: str = "event"
    sql: str = """DROP TABLE IF EXISTS event;"""
    args: tuple = ()
    description: str = "Dropped the event table."


# ------------------------------------------------------------------------------------------------ #


@dataclass
class EventTableExists(SQL):
    name: str = "event"
    sql: str = None
    args: tuple = ()
    description: str = "Checked existence of event table."

    def __post_init__(self) -> None:
        dotenv.load_dotenv()
        mode = os.getenv("MODE")
        self.sql = f"""SELECT COUNT(TABLE_NAME) FROM information_schema.TABLES WHERE TABLE_SCHEMA LIKE 'recsys_{mode}_events' AND TABLE_NAME = 'event';"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class EventDDL(DDL):
    entity: type[Entity] = Event
    create: SQL = CreateEventTable()
    drop: SQL = DropEventTable()
    exists: SQL = EventTableExists()


# ------------------------------------------------------------------------------------------------ #
#                                          DML                                                     #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class InsertEvent(SQL):
    dto: DTO
    sql: str = """INSERT INTO event (oid, name, description, process_type, process_oid, parent_oid) VALUES (%s, %s, %s, %s, %s, %s);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.oid,
            self.dto.name,
            self.dto.description,
            self.dto.process_type,
            self.dto.process_oid,
            self.dto.parent_oid,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UpdateEvent(SQL):
    dto: DTO
    sql: str = """UPDATE event SET oid = %s, name = %s, description = %s, process_type = %s, process_oid = %s, parent_oid = %s WHERE id = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.oid,
            self.dto.name,
            self.dto.description,
            self.dto.process_type,
            self.dto.process_oid,
            self.dto.parent_oid,
            self.dto.id,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectEvent(SQL):
    id: int
    sql: str = """SELECT * FROM event WHERE id = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectEventByName(SQL):
    name: str
    sql: str = """SELECT * FROM event WHERE name = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.name,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectAllEvents(SQL):
    sql: str = """SELECT * FROM event;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class EventExists(SQL):
    id: int
    sql: str = """SELECT EXISTS(SELECT 1 FROM event WHERE id = %s LIMIT 1);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DeleteEvent(SQL):
    id: int
    sql: str = """DELETE FROM event WHERE id = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class EventDML(DML):
    entity: type[Entity] = Event
    insert: type[SQL] = InsertEvent
    update: type[SQL] = UpdateEvent
    select: type[SQL] = SelectEvent
    select_by_name: type[SQL] = SelectEventByName
    select_all: type[SQL] = SelectAllEvents
    exists: type[SQL] = EventExists
    delete: type[SQL] = DeleteEvent
