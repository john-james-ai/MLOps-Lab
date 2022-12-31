#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/dal/uow.py                                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 25th 2022 12:55:35 pm                                               #
# Modified   : Friday December 30th 2022 08:47:59 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Unit of Work Module"""
from abc import ABC, abstractmethod

#from dependency_injector.wiring import Provide, inject

from recsys.core.dal.repo import Context
#from recsys.containers import Recsys
from recsys.core.dal.repo import Repo
from recsys.core.entity.base import Entity
from recsys.core.entity.dataset import Dataset, DataFrame
from recsys.core.entity.datasource import DataSource, DataSourceURL
from recsys.core.entity.job import Task, Job
from recsys.core.entity.file import File
from recsys.core.entity.profile import Profile


# ------------------------------------------------------------------------------------------------ #
#                                 UNIT OF WORK ABC                                                 #
# ------------------------------------------------------------------------------------------------ #
class UnitOfWorkAbstract(ABC):

    @property
    @abstractmethod
    def datasource(self) -> Repo:
        """Returns an instantiated datasource repository."""

    @property
    @abstractmethod
    def datasource_url(self) -> Repo:
        """Returns an instantiated datasource_url repository."""

    @property
    @abstractmethod
    def file(self) -> Repo:
        """Returns an instantiated file repository."""

    @property
    @abstractmethod
    def dataset(self) -> Repo:
        """Returns an instantiated dataset repository."""

    @property
    @abstractmethod
    def dataframe(self) -> Repo:
        """Returns an instantiated dataframe repository."""

    @property
    @abstractmethod
    def job(self) -> Repo:
        """Returns an instantiated Job repository."""

    @property
    @abstractmethod
    def task(self) -> Repo:
        """Returns an instantiated Task repository."""

    @property
    @abstractmethod
    def profile(self) -> Repo:
        """Returns an instantiated Task repository."""

    @abstractmethod
    def save(self):
        """Save changes."""

# ------------------------------------------------------------------------------------------------ #
#                                     UNIT OF WORK ABC                                             #
# ------------------------------------------------------------------------------------------------ #


class UnitOfWork(UnitOfWorkAbstract):
    """Unit of Work object containing all Entity repositories and the current context entity.

    Args:
        context (Context): Contains the database context in terms of Database Access Objects.

    """

    def __init__(self, context: Context = Context()) -> None:
        self._context = context
        self._file = None
        self._datasource = None
        self._datasource_url = None
        self._dataset = None
        self._dataframe = None
        self._job = None
        self._task = None
        self._profile = None
        self._current = None

    @property
    def current(self) -> Entity:
        return self._current

    @current.setter
    def current(self, current: Entity) -> None:
        self._current = current

    @property
    def file(self) -> Repo:
        if self._file is None:
            self._file = Repo(context=self._context, entity=File)
        return self._file

    @property
    def datasource(self) -> Repo:
        if self._datasource is None:
            self._datasource = Repo(context=self._context, entity=DataSource)
        return self._datasource

    @property
    def datasource_url(self) -> Repo:
        if self._datasource_url is None:
            self._datasource_url = Repo(context=self._context, entity=DataSourceURL)
        return self._datasource_url

    @property
    def dataset(self) -> Repo:
        if self._dataset is None:
            self._dataset = Repo(context=self._context, entity=Dataset)
        return self._dataset

    @property
    def dataframe(self) -> Repo:
        if self._dataframe is None:
            self._dataframe = Repo(context=self._context, entity=DataFrame)
        return self._dataframe

    @property
    def job(self) -> Repo:
        if self._job is None:
            self._job = Repo(context=self._context, entity=Job)
        return self._job

    @property
    def task(self) -> Repo:
        if self._task is None:
            self._task = Repo(context=self._context, entity=Task)
        return self._task

    @property
    def profile(self) -> Repo:
        if self._profile is None:
            self._profile = Repo(context=self._context, entity=Profile)
        return self._profile

    def save(self) -> None:
        self._context.save()
