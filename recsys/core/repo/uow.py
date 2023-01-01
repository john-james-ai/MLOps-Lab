#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/repo/uow.py                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 25th 2022 12:55:35 pm                                               #
# Modified   : Sunday January 1st 2023 01:58:05 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Unit of Work Module"""
from abc import ABC, abstractmethod

from dependency_injector.wiring import inject, Provide

from recsys.containers import RepoContainer
from recsys.containers import Recsys
from recsys.core.repo.entity import Repo
from recsys.core.repo.context import Context
from recsys.core.entity.base import Entity


# ------------------------------------------------------------------------------------------------ #
#                                 UNIT OF WORK ABC                                                 #
# ------------------------------------------------------------------------------------------------ #
class UnitOfWorkAbstract(ABC):

    @property
    @abstractmethod
    def datasource(self) -> Repo:
        """Returns an instantiated datasource repossitory."""

    @property
    @abstractmethod
    def datasource_url(self) -> Repo:
        """Returns an instantiated datasource_url repossitory."""

    @property
    @abstractmethod
    def file(self) -> Repo:
        """Returns an instantiated file repossitory."""

    @property
    @abstractmethod
    def dataset(self) -> Repo:
        """Returns an instantiated dataset repossitory."""

    @property
    @abstractmethod
    def dataframe(self) -> Repo:
        """Returns an instantiated dataframe repossitory."""

    @property
    @abstractmethod
    def job(self) -> Repo:
        """Returns an instantiated Job repossitory."""

    @property
    @abstractmethod
    def task(self) -> Repo:
        """Returns an instantiated Task repossitory."""

    @property
    @abstractmethod
    def profile(self) -> Repo:
        """Returns an instantiated Task repossitory."""

    @abstractmethod
    def save(self):
        """Save changes."""

# ------------------------------------------------------------------------------------------------ #
#                                     UNIT OF WORK ABC                                             #
# ------------------------------------------------------------------------------------------------ #


class UnitOfWork(UnitOfWorkAbstract):
    """Unit of Work object containing all Entity repossitories and the current context entity.

    Args:
        context (Context): Contains the database context in terms of Database Access Objects.

    """

    @inject
    def __init__(
        self,
        context: Context = Provide[Recsys.context.context],
        repos: RepoContainer = Provide[Recsys.repos],
    ) -> None:
        self._context = context()
        self._repos = repos

        self._current_job = None

    @property
    def current_job(self) -> Entity:
        return self._current_job

    @current_job.setter
    def current_job(self, job: Entity) -> None:
        self._current_job = job

    @property
    def file(self) -> Repo:
        return self._repos.file()

    @property
    def datasource(self) -> Repo:
        return self._repos.datasource()

    @property
    def datasource_url(self) -> Repo:
        return self._repos.datasource_url()

    @property
    def dataset(self) -> Repo:
        return self._repos.dataset()

    @property
    def dataframe(self) -> Repo:
        return self._repos.dataframe()

    @property
    def job(self) -> Repo:
        return self._repos.job()

    @property
    def task(self) -> Repo:
        return self._repos.task()

    @property
    def profile(self) -> Repo:
        return self._repos.profile()

    def save(self) -> None:
        self._context.save()
