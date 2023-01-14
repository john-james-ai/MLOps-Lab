#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/entity/job.py                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 07:32:54 pm                                                #
# Modified   : Saturday January 14th 2023 03:03:02 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Task Entity Module"""
from abc import abstractmethod
from typing import Union, Dict, Any
import pandas as pd
from datetime import datetime

from recsys.core.entity.base import Entity
from recsys.core.entity.event import Event
from recsys.core.workflow.operator.base import Operator
from recsys.core.dal.dto import TaskDTO, JobDTO
from recsys.core.repo.uow import UnitOfWork
from recsys import STATES


# ------------------------------------------------------------------------------------------------ #
#                                      JOB  COMPONENT                                             #
# ------------------------------------------------------------------------------------------------ #
class JobComponent(Entity):
    """Base component class from which Task (Leaf) and Job (Composite) objects derive."""

    def __init__(self, name: str, description: str = None) -> None:
        super().__init__(name=name, description=description)

    # -------------------------------------------------------------------------------------------- #
    @property
    def state(self) -> str:
        """Returns the state of the component."""
        return self._state

    # -------------------------------------------------------------------------------------------- #
    @state.setter
    def state(self, state: str) -> None:
        """Sets the state of the component."""
        self._state = state
        self._validate()

    # -------------------------------------------------------------------------------------------- #
    @property
    @abstractmethod
    def is_composite(self) -> str:
        """True for Jobs and False for Tasks."""

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    def as_dto(self) -> Union[TaskDTO, Dict[int, TaskDTO]]:
        """Creates a dto representation of the Job Component."""

    # -------------------------------------------------------------------------------------------- #
    def _validate(self) -> None:
        super()._validate()


# ------------------------------------------------------------------------------------------------ #
#                                          JOB S                                                  #
# ------------------------------------------------------------------------------------------------ #
class Job(JobComponent):
    """Collection of Task objects.

    Args:
        name (str): Job name
        description (str): Job Description
    """

    def __init__(
        self,
        name: str,
        description: str = None,
    ) -> None:
        super().__init__(name=name, description=description)

        self._tasks = {}
        self._state = STATES[0]
        self._is_composite = True

        self._validate()

    def __str__(self) -> str:
        return f"Job Id: {self._id}\n\tName: {self._name}\n\tDescription: {self._description}\n\tState: {self._state}\n\tCreated: {self._created}\n\tModified: {self._modified}"

    def __repr__(self) -> str:
        return f"{self._id}, {self._name}, {self._description}, {self._state}, {self._created}, {self._modified}"

    def __eq__(self, other: JobComponent) -> bool:
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
        for task in self._tasks.values():
            try:
                self._start(task=task, uow=uow)
                data = task.run(uow=uow, data=data)
                self._end(task=task, uow=uow)
            except Exception:
                self._failed(task=task, uow=uow)
                raise
        return data

    # -------------------------------------------------------------------------------------------- #
    @property
    def tasks(self) -> dict:
        return self._tasks

    # -------------------------------------------------------------------------------------------- #
    def add_task(self, task: JobComponent) -> None:
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
    def update_task(self, task: JobComponent) -> None:
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
    def as_dto(self) -> JobDTO:

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

    # -------------------------------------------------------------------------------------------- #
    def _start(self, task: Entity, uow: UnitOfWork) -> None:
        task.state = STATES[2]
        self.update_task(task)
        self.save(uow)

    # -------------------------------------------------------------------------------------------- #
    def _end(self, task: Entity, uow: UnitOfWork) -> None:
        task.state = STATES[4]
        self.update_task(task)
        self.save(uow)

    # -------------------------------------------------------------------------------------------- #
    def _failed(self, task: Entity, uow: UnitOfWork) -> None:
        task.state = STATES[3]
        self.update_task(task)
        self.save(uow)

    # -------------------------------------------------------------------------------------------- #
    def save(self, uow=UnitOfWork) -> None:
        repo = uow.get_repo("job")
        repo.update(self)


# ------------------------------------------------------------------------------------------------ #
#                                          TASK                                                    #
# ------------------------------------------------------------------------------------------------ #
class Task(JobComponent):
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
        operator: Operator,
        description: str = None,
    ) -> None:
        super().__init__(name=name, description=description)

        self._operator = operator
        self._parent = None
        self._is_composite = False
        self._state = STATES[0]

        self._validate()

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
    @property
    def operator(self) -> Job:
        return self._operator

    # -------------------------------------------------------------------------------------------- #
    @parent.setter
    def parent(self, parent: Job) -> None:
        self._parent = parent

    # -------------------------------------------------------------------------------------------- #
    def run(self, uow: UnitOfWork, data: Any = None) -> Union[None, Any]:
        """Runs the task through delegation to an operator."""
        try:
            self._start(uow=uow)
            data = self._operator.execute(uow=uow, data=data)
            self._end(uow=uow)
            return data

        except Exception:
            self._failed(uow=uow)
            raise

    # -------------------------------------------------------------------------------------------- #
    def as_dto(self) -> TaskDTO:
        return TaskDTO(
            id=self._id,
            oid=self._oid,
            name=self._name,
            description=self._description,
            state=self._state,
            parent_id=self._parent.id,
            created=self._created,
            modified=self._modified,
        )

    # -------------------------------------------------------------------------------------------- #
    def _start(self, uow: UnitOfWork) -> None:
        eventname = "start_" + self._name
        eventdesc = "Started " + self._description
        event = Event(
            name=eventname, description=eventdesc, job_oid=self._parent.oid, task_oid=self._oid
        )
        repo = uow.get_repo("event")
        repo.add(event)
        self._logger.info(eventdesc)

    def _end(self, uow: UnitOfWork) -> None:
        eventname = "completed_" + self._name
        eventdesc = "Completed " + self._description
        event = Event(
            name=eventname, description=eventdesc, job_oid=self._parent.oid, task_oid=self._oid
        )
        repo = uow.get_repo("event")
        repo.add(event)
        self._logger.info(eventdesc)

    def _failed(self, uow: UnitOfWork) -> None:
        eventname = "failed_" + self._name
        eventdesc = "Failed " + self._description
        event = Event(
            name=eventname, description=eventdesc, job_oid=self._parent.oid, task_oid=self._oid
        )
        repo = uow.get_repo("event")
        repo.add(event)
        self._logger.info(eventdesc)


# ------------------------------------------------------------------------------------------------ #
#                                          JOB                                                    #
# ------------------------------------------------------------------------------------------------ #
