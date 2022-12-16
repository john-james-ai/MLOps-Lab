#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/services/context.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday December 13th 2022 05:12:39 am                                              #
# Modified   : Friday December 16th 2022 03:27:58 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #

from dependency_injector.wiring import Provide, inject

from .base import Service
from recsys.containers import Recsys
from recsys.core.data.connection import Database
# ------------------------------------------------------------------------------------------------ #


class Context(Service):
    """Context contains multiple repositories connected to a single database context."""

    @inject
    def __init__(self, database: Database = Provide[Recsys.data.database]) -> None:
        self._database = database
