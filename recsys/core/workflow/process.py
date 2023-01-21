#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/workflow/process.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 07:32:54 pm                                                #
# Modified   : Saturday January 21st 2023 05:07:09 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Process Module"""
from typing import Union, Any
import pandas as pd
from datetime import datetime

from dependency_injector.wiring import Provide, inject

from recsys.core.workflow.base import Process
from recsys.core.workflow.callback import JobCallback, TaskCallback
from recsys.core.workflow.operator.base import Operator
from recsys.core.dal.dao import DTO, JobDTO, TaskDTO
from recsys.core.repo.uow import UnitOfWork
from recsys import STATES


# ------------------------------------------------------------------------------------------------ #
#                                          JOB                                                     #
# ------------------------------------------------------------------------------------------------ #
class Job(Process):
    """Collection of Task objects.

    Args:
        name (str): Job name
        description (str): Job Description
    """

    @inject
    def __init__(self, name: str, description: str = None, callback=Callback) -> None:
        super().__init__(name=name, description=description)

        self._callback = callback
        self._tasks = {}
        self._state = STATES[0]
        self._is_composite = True
        self._on_create()

    def __str__(self) -> str:
        return f"Job Id: {self._id}\n\tName: {self._name}\n\tDescription: {self._description}\n\tState: {self._state}\n\tCreated: {self._created}\n\tModified: {self._modified}"

    def __repr__(self) -> str:
        return f"{self._id}, {self._name}, {self._description}, {self._state}, {self._created}, {self._modified}"

    def __eq__(self, other: Process) -> bool:
        if self.__class__.__name__ == other.__class__.__name__:
            return (
                self.is_composite == other.is_composite
                and self.name == other.name
                and self.description == other.description
                and self.state == other.state
            )
        else:
            return False

    def __len__(self) -> int:
        return len(self._tasks.values())

    # -------------------------------------------------------------------------------------------- #
    @property
    def is_composite(self) -> str:
        return self._is_composite

    # -------------------------------------------------------------------------------------------- #
    def run(self, uow: UnitOfWork, data: Any = None) -> None:
        self._on_start()

        for task in self._tasks.values():
            try:
                data = task.run(uow=uow, data=data)
            except Exception:
                self._on_fail()
                raise

        self._on_end()
        return data

    # -------------------------------------------------------------------------------------------- #
    @property
    def tasks(self) -> dict:
        return self._tasks

    # -------------------------------------------------------------------------------------------- #
    def add_task(self, task: Process) -> None:
        task.parent = self
        self._tasks[task.name] = task
        self._modified = datetime.now()
        self._logger.debug(f"just added task {task.name} to {self._name}")

    # -------------------------------------------------------------------------------------------- #
    def get_task(self, name: str = None) -> None:
        try:
            return self._tasks[name]
        except KeyError:
            msg = f"Job {self._name} has no task with name = {name}."
            self._logger.error(msg)
            raise FileNotFoundError(msg)

    # -------------------------------------------------------------------------------------------- #
    def get_tasks(self) -> pd.DataFrame:
        d = {}
        for name, task in self._tasks.items():
            d[name] = task.as_dto().as_dict()
        df = pd.DataFrame.from_dict(data=d, orient="index")
        return df

    # -------------------------------------------------------------------------------------------- #
    def update_task(self, task: Process) -> None:
        if task.name in self._tasks.keys():
            task.parent = self
            self._tasks[task.name] = task
            self._modified = datetime.now()
            self._logger.debug(f"just updated task {task.name} in {self._name}")
        else:
            msg = f"Task {task.name} does not exist in job {self._name}. Did you mean add_task?"
            self._logger.error(msg)
            raise KeyError(msg)

    # -------------------------------------------------------------------------------------------- #
    def remove_task(self, name: str) -> None:
        try:
            del self._tasks[name]
            self._modified = datetime.now()
        except KeyError:
            msg = f"Unable to delete task. Task {name} does not exist in job {self._name}."
            self._logger.error(msg)
            raise KeyError(msg)

    # -------------------------------------------------------------------------------------------- #
    def as_dto(self) -> DTO:

        dto = JobDTO(
            id=self._id,
            oid=self._oid,
            name=self._name,
            description=self._description,
            state=self._state,
            created=self._created,
            modified=self._modified,
        )
        return dto


# ------------------------------------------------------------------------------------------------ #
#                                          TASK                                                    #
# ------------------------------------------------------------------------------------------------ #
class Task(Process):
    """Task is a pipeline step or operation in execution.

    Args:
        name (str): Short, yet descriptive lowercase name for Task object.
        description (str): Describes the Task object. Default's to job's description if None.
        operator (Operator): An instance of an operator object.
        job (Job): The parent Job instance.

    """

    def __init__(
        self,
        name: str,
        operator: Operator = None,
        description: str = None,
        callback=Provide[Recsys.callback.task],
    ) -> None:
        super().__init__(name=name, description=description)

        self._callback = callback
        self._operator = operator
        self._parent = None
        self._is_composite = False
        self._state = STATES[0]

    def __str__(self) -> str:
        return f"Task Id: {self._id}\n\tName: {self._name}\n\tDescription: {self._description}\n\tState: {self._state}\n\tCreated: {self._created}\n\tModified: {self._modified}"

    def __repr__(self) -> str:
        return f"{self._id}, {self._name}, {self._description}, {self._state}, {self._created}, {self._modified}"

    def __eq__(self, other) -> bool:
        """Compares two Job for equality.
        Job are considered equal solely if their underlying data are equal.

        Args:
            other (Task): The Task object to compare.
        """

        if isinstance(other, Task):
            return (
                self._name == other.name
                and self._operator == other._operator
                and self._description == other.description
                and self._parent == other.parent
            )
        else:
            return False

    def __len__(self) -> int:
        return 1

    # -------------------------------------------------------------------------------------------- #
    @property
    def is_composite(self) -> str:
        return self._is_composite

    # -------------------------------------------------------------------------------------------- #
    @property
    def parent(self) -> Job:
        return self._parent

    # -------------------------------------------------------------------------------------------- #
    @parent.setter
    def parent(self, parent: Job) -> None:
        self._parent = parent

    # -------------------------------------------------------------------------------------------- #
    @property
    def operator(self) -> Operator:
        return self._operator

    # -------------------------------------------------------------------------------------------- #
    @operator.setter
    def operator(self, operator: Operator) -> None:
        self._operator = operator

    # -------------------------------------------------------------------------------------------- #
    def run(self, uow: UnitOfWork, data: Any = None) -> Union[None, Any]:
        """Runs the task through delegation to an operator."""
        self._on_start()
        try:
            data = self._operator.execute(uow=uow, data=data)
        except Exception:
            self._on_fail()
            raise

        self._on_end()
        return data

    # -------------------------------------------------------------------------------------------- #
    def as_dto(self) -> TaskDTO:
        return TaskDTO(
            id=self._id,
            oid=self._oid,
            name=self._name,
            description=self._description,
            state=self._state,
            parent_oid=self._parent.oid,
            created=self._created,
            modified=self._modified,
        )
