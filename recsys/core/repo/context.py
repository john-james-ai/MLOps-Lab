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
# Modified   : Tuesday January 3rd 2023 03:20:05 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Context Module."""
from dependency_injector import containers

from recsys.core.dal.dao import DAO
from recsys.core.entity.base import Entity
from recsys.core.entity.dataset import Dataset, DataFrame
from recsys.core.entity.datasource import DataSource, DataSourceURL
from recsys.core.entity.job import Task, Job
from recsys.core.entity.file import File
from recsys.core.entity.profile import Profile


# ------------------------------------------------------------------------------------------------ #
#                                        CONVARCHAR(64)                                                   #
# ------------------------------------------------------------------------------------------------ #
class Context:

    def __init__(self, dao: containers) -> None:
        self._dao = dao

    def get_dao(self, entity: type(Entity)) -> DAO:
        daos = {Dataset: self._dao.dataset, DataFrame: self._dao.dataframe,
                DataSource: self._dao.datasource, DataSourceURL: self._dao.datasource_url,
                Task: self._dao.task, Job: self._dao.job, Profile: self._dao.profile,
                File: self._dao.file}

        return daos[entity]()
