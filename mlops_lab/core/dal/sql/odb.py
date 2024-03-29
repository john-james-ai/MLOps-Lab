#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /mlops_lab/core/dal/sql/odb.py                                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday January 6th 2023 11:45:23 pm                                                 #
# Modified   : Tuesday January 24th 2023 08:13:43 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Language for Querying and Manipulating the Object Database."""
from dataclasses import dataclass
from typing import Any

from mlops_lab.core.entity.base import Entity
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
class SelectByName(OQL):
    entity: type[Entity]
    name: str
    oid: str = None
    cmd: str = "select_by_name"
    args: Any = None

    def __post_init__(self) -> None:
        self.oid = self.entity.lower() + "_" + self.name


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
    select_by_name: type(SelectByName) = SelectByName
    delete: type(Delete) = Delete
    exists: type(Exists) = Exists
