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
# Modified   : Tuesday December 13th 2022 05:43:34 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dependency_injector.wiring import Provide, inject
from .base import Service
from recsys.core.entity.dataset import Dataset
from recsys.core.entity.fileset import Fileset
from recsys.core.entity.task import Task
from recsys.core.entity.job import Job
# ------------------------------------------------------------------------------------------------ #

class Context(Service):
    """Context contains multiple repositories connected to a single database context."""

    @inject
    def __init__(self,
    dataset: Dataset) -> None:
        self._database = databse