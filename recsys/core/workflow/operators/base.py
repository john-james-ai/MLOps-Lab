#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/workflow/operators/base.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 08:30:24 pm                                                #
# Modified   : Saturday December 31st 2022 07:09:22 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod
import pandas as pd
import logging

from recsys import STATES
from recsys.core.entity.job import Task
from recsys.core.dal.uow import UnitOfWork
from recsys.core.entity.dataset import Dataset


# ================================================================================================ #
#                                    OPERATOR BASE CLASS                                           #
# ================================================================================================ #
class Operator(ABC):
    """Operator Base Class"""
    def __init__(self, name: str, mode: str, description: str = None) -> None:
        self._name = name
        self._mode = mode
        self._description = description
        self._uow = None

        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )

    def __str__(self) -> str:
        return f"Operator:\n\tModule: {self.__module__}\n\tClass: {self.__class__.__name__}\n\tName: {self._name}\n\tDescription: {self._description}\n\tMode: {self._mode}"

    def __repr__(self) -> str:
        return f"{self.__module__}, {self.__class__.__name__}, {self._name}, {self._description}, {self._mode}"

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return self._description

    @property
    def mode(self) -> str:
        return self._mode

    @property
    def uow(self) -> UnitOfWork:
        return self._uow

    @uow.setter
    def uow(self, uow: UnitOfWork) -> None:
        self._uow = uow

    @abstractmethod
    def execute(self, *args, **kwargs) -> None:
        """Executes the operation."""

    def _as_task(self) -> Task:
        """Creates a task reprsentation of the Operation, sets the state to in-progress, and returns it."""
        # Create the Task object
        task = Task(name=self._name, parent=self._uow.current_job, description=self._description, mode=self._mode)
        # Set the task state to in-progress and return it.
        task.state = STATES[2]
        return task

    def _setup(self) -> Task:
        """Create task and write to repository."""
        task = self._as_task()
        task = self._uow.task.add(task, persist=False)
        self._logger.debug(f"Just added the following task:\n{task}")
        self._uow.current_job.add_task(task)
        self._uow.save()
        return task

    def _teardown(self, task: Task) -> None:
        """Updates the state of the Task and updates the repository."""
        # Set task state to COMPLETE
        task.state = STATES[-1]
        self._uow.task.update(task, persist=False)
        self._uow.current_job.update_task(task)
        self._logger.debug(f"Just updated the following task:\n{task}")
        self._uow.save()

    def _get_dataframe(self) -> pd.DataFrame:
        """Retrieves a pandas DataFrame from the Dataset repository."""
        dataset = self._uow.dataset.get(self._input_params.id)
        return dataset.get_dataframe(name=dataset.name)

    def _put_dataset(self, dataset: Dataset) -> None:
        """Stores the Dataset in the Repository."""
        self._uow.dataset.create(dataset)
