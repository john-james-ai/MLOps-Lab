#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/dal/sql/odb.py                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday January 6th 2023 11:45:23 pm                                                 #
# Modified   : Sunday January 8th 2023 04:43:56 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Language for Querying and Manipulating the Object Database."""
from dataclasses import dataclass
from typing import Any

from recsys.core.entity.base import Entity
from .base import OQL, OML, ODL


# ------------------------------------------------------------------------------------------------ #
#                                OBJECT DEFINITION LANGUAGE                                        #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class Create(ODL):
    location: str = "tests/core/dal/odb/odb.db"
    autocommit: bool = True
    cmd: str = "create"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class Drop(ODL):
    location: str = "tests/core/dal/odb/odb.db"
    cmd: str = "drop"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class Exists(ODL):
    location: str = "tests/core/dal/odb/odb.db"
    cmd: str = "exists"


# ------------------------------------------------------------------------------------------------ #
class ObjectODL(ODL):
    create: type(Create) = Create
    drop: type(Drop) = Drop
    exists: type(Exists) = Exists


# ------------------------------------------------------------------------------------------------ #
#                            OBJECT QUERY / MANIPULATION LANGUAGE                                  #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class Insert(OML):
    entity: Entity
    oid: str = None
    cmd: str = "insert"

    def __post_init__(self) -> None:
        self.oid = self.entity.oid


# ------------------------------------------------------------------------------------------------ #
@dataclass
class Select(OQL):
    oid: str
    cmd: str = "select"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class SelectByNameMode(OQL):
    entity: type(Entity)
    name: str
    mode: str
    oid: str = None
    cmd: str = "select_by_name_mode"
    args: Any = None

    def __post_init__(self) -> None:
        self.oid = self.entity.lower() + "_" + self.name + "_" + self.mode


# ------------------------------------------------------------------------------------------------ #
@dataclass
class Update(OML):
    entity: Entity
    oid: str = None
    cmd: str = "update"

    def __post_init__(self) -> None:
        self.oid = self.entity.oid


# ------------------------------------------------------------------------------------------------ #
@dataclass
class Delete(OML):
    oid: str
    cmd: str = "delete"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class Exists(OQL):
    oid: str
    cmd: str = "exists"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class ObjectOML:
    insert: type(Insert) = Insert
    update: type(Update) = Update
    select: type(Select) = Select
    select_by_name_mode: type(SelectByNameMode) = SelectByNameMode
    delete: type(Delete) = Delete
    exists: type(Exists) = Exists
