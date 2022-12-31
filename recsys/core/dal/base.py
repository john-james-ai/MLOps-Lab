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
# Modified   : Friday December 30th 2022 07:42:28 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Base Data Access Module"""
from abc import ABC
from dataclasses import dataclass


# ------------------------------------------------------------------------------------------------ #
#                                  SQL COMMAND ABC                                                 #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class SQL(ABC):  # pragma: no cover
    """Base class for SQL Command Objects."""


# ------------------------------------------------------------------------------------------------ #
#                             DDL AGGREGATION BASE CLASS                                           #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DDL(ABC):  # pragma: no cover
    """Base class for entity Data Definition Language (DDL)."""

    create: SQL
    drop: SQL
    exists: SQL


# ------------------------------------------------------------------------------------------------ #
#                             DML AGGREGATION BASE CLASS                                           #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DML(ABC):  # pragma: no cover
    """Base class for entity Data Manipulation Language (DML)."""

    insert: type(SQL)
    update: type(SQL)
    select: type(SQL)
    select_all: type(SQL)
    exists: type(SQL)
    delete: type(SQL)
