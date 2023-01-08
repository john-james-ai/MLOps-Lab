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
# Modified   : Sunday January 8th 2023 03:26:52 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Context Module."""
from dependency_injector import containers
from dependency_injector.wiring import Provide, inject

from recsys.core.dal.dao import DAO
from recsys.core.dal.oao import OAO
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
        self._rdb = dal.rdb()
        self._odb = dal.odb()

    @property
    def in_transaction(self) -> bool:
        """Returns True if database transaction is extant."""
        return self._rdb.in_transaction

    def begin(self) -> None:
        """Begin a transaction on the context."""
        self._rdb.begin()
        self._odb.begin()

    def rollback(self) -> None:
        """Rolls back the database to the state at last save."""
        self._rdb.rollback()
        self._odb.rollback()

    def save(self) -> None:
        """Saves the context."""
        self._rdb.save()
        self._odb.save()

    def close(self) -> None:
        """Saves the context."""
        self._rdb.close()
        self._odb.close()

    def get_dao(self, entity: type(Entity)) -> DAO:
        """Provides a data access object for the given entity."""
        daos = {Dataset: self._dal.dataset, DataFrame: self._dal.dataframe,
                DataSource: self._dal.datasource, DataSourceURL: self._dal.datasource_url,
                Task: self._dal.task, Job: self._dal.job, Profile: self._dal.profile,
                File: self._dal.file}

        return daos[entity]()

    def get_oao(self) -> OAO:
        return self._dal.object()
