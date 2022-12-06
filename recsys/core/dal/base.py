#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/dal/base.py                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday December 3rd 2022 12:44:06 pm                                              #
# Modified   : Monday December 5th 2022 01:36:36 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Base Data Access Module"""
from dataclasses import dataclass
import logging
from abc import ABC


# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
#                                  SQL COMMAND ABC                                                 #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class SQL(ABC):
    """Base class for SQL Command Objects."""


# ------------------------------------------------------------------------------------------------ #
#                             DDL AGGREGATION BASE CLASS                                           #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DDL(ABC):
    """Base class for entity Data Definition Language (DDL)."""

    create: SQL
    drop: SQL
    exists: SQL


# ------------------------------------------------------------------------------------------------ #
#                             DML AGGREGATION BASE CLASS                                           #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DML(ABC):
    """Base class for entity Data Manipulation Language (DML)."""

    insert: type(SQL)
    update: type(SQL)
    select: type(SQL)
    select_all: type(SQL)
    exists: type(SQL)
    delete: type(SQL)


# ------------------------------------------------------------------------------------------------ #
#                                   SERVICE BASE CLASS                                             #
# ------------------------------------------------------------------------------------------------ #
class Service(ABC):
    def __init__(self) -> None:
        self._logger = logging.getLogger(
            f"{__name__}.{self.__class__.__name__}",
        )
