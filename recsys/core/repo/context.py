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
# Modified   : Saturday January 21st 2023 07:34:46 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Context Module."""
import logging

from dependency_injector import containers

from recsys.core.dal.dao import DAO
from recsys.core.dal.oao import OAO


# ------------------------------------------------------------------------------------------------ #
#                                       CONTEXT                                                    #
# ------------------------------------------------------------------------------------------------ #
class Context:
    def __init__(self, dal: containers.DeclarativeContainer) -> None:

        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )
        self._dal = dal
        self._rdb = dal.rdb()
        self._odb = dal.odb()
        self._edb = dal.edb()
        msg = f"Context instantiated with\n\tDAL location: {id(self._dal)}\n\trdb location: {id(self._rdb)}\n\tedb location: {id(self._edb)}"
        self._logger.debug(msg)

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
        self._edb.save()

    def close(self) -> None:
        """Saves the context."""
        self._rdb.close()
        self._odb.close()

    def get_dao(self, name: str) -> DAO:
        """Provides a data access object for the given entity."""
        daos = {
            "dataset": self._dal.dataset,
            "dataframe": self._dal.dataframe,
            "datasource": self._dal.datasource,
            "datasourceurl": self._dal.datasource_url,
            "event": self._dal.event,
            "task": self._dal.task,
            "dag": self._dal.dag,
            "profile": self._dal.profile,
            "file": self._dal.file,
        }

        return daos[name]()

    def get_oao(self) -> OAO:
        return self._dal.object()
