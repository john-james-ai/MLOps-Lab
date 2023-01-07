#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/repo/context.py                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday December 31st 2022 10:51:29 pm                                             #
# Modified   : Friday January 6th 2023 10:36:27 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Context Module."""
from dependency_injector import containers
from dependency_injector.wiring import Provide, inject

from recsys.core.dal.dao import DAO
from recsys.core.entity.base import Entity
from recsys.core.entity.dataset import Dataset, DataFrame
from recsys.core.entity.datasource import DataSource, DataSourceURL
from recsys.core.entity.job import Task, Job
from recsys.core.entity.file import File
from recsys.core.entity.profile import Profile
from recsys.containers import Recsys


# ------------------------------------------------------------------------------------------------ #
#                                       CONTEXT                                                    #
# ------------------------------------------------------------------------------------------------ #
class Context:

    @inject
    def __init__(self, dal: containers = Provide[Recsys.dal]) -> None:
        self._dal = dal
        self._database = dal.database()

    @property
    def in_transaction(self) -> bool:
        """Returns True if database transaction is extant."""
        return self._database.in_transaction

    def begin(self) -> None:
        """Begin a transaction on the context."""
        self._database.begin()

    def rollback(self) -> None:
        """Rolls back the database to the state at last save."""
        self._database.rollback()

    def save(self) -> None:
        """Saves the context."""
        self._database.save()

    def close(self) -> None:
        """Saves the context."""
        self._database.close()

    def get_dao(self, entity: type(Entity)) -> DAO:
        """Provides a data access object for the given entity."""
        daos = {Dataset: self._dal.dataset, DataFrame: self._dal.dataframe,
                DataSource: self._dal.datasource, DataSourceURL: self._dal.datasource_url,
                Task: self._dal.task, Job: self._dal.job, Profile: self._dal.profile,
                File: self._dal.file}

        return daos[entity]()
