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
# Modified   : Saturday December 31st 2022 08:47:04 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Task Entity Module"""
from abc import abstractmethod
from typing import Union, Dict
from datetime import datetime

from recsys.core.entity.base import Entity
from recsys.core.dal.dto import TaskDTO, JobDTO


# ------------------------------------------------------------------------------------------------ #
#                                    DATASET COMPONENT                                             #
# ------------------------------------------------------------------------------------------------ #
class JobComponent(Entity):
    """Base component class from which Task (Leaf) and Job (Composite) objects derive."""

    def __init__(self, name: str, description: str = None, mode: str = None) -> None:
        super().__init__(name=name, description=description, mode=mode)

    # -------------------------------------------------------------------------------------------- #
    @property
    @abstractmethod
    def state(self) -> str:
        """Data processing stage in which the Job Component is created."""

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
#                                        DATASETS                                                  #
# ------------------------------------------------------------------------------------------------ #
class Job(JobComponent):
    """Collection of Task objects.

    Args:
        name (str): Job name
        description (str): Job Description
        mode (str): Mode for which the Task is created. If None, defaults to mode from environment
            variable.
    """

    def __init__(
        self,
        name: str,
        description: str = None,
        mode: str = None,
    ) -> None:
        super().__init__(name=name, description=description, mode=mode)

        self._tasks = {}
        self._state = 'CREATED'
        self._task_count = 0
        self._is_composite = True

        self._validate()

    def __str__(self) -> str:
        return f"Job Id: {self._id}\n\tName: {self._name}\n\tDescription: {self._description}\n\tMode: {self._mode}\n\tState: {self._state}\n\tTasks: {self._task_count}\n\tCreated: {self._created}\n\tModified: {self._modified}"

    def __repr__(self) -> str:
        return f"{self._id}, {self._name}, {self._description}, {self._mode}, {self._state}, {self._task_count}, {self._created}, {self._modified}"

    def __eq__(self, other: JobComponent) -> bool:
        if self.__class__.__name__ == other.__class__.__name__:
            return (self.is_composite == other.is_composite
                    and self.name == other.name
                    and self.description == other.description
                    and self.mode == other.mode
                    and self.state == other.state)
        else:
            return False

    def __len__(self) -> int:
        return len(self._tasks)

    @property
    def task_count(self) -> int:
        self._task_count = len(self._tasks)
        return self._task_count

    # -------------------------------------------------------------------------------------------- #
    @property
    def is_composite(self) -> str:
        return self._is_composite

    # -------------------------------------------------------------------------------------------- #
    @property
    def state(self) -> str:
        """The overall state of the job."""
        return self._state

    @state.setter
    def state(self, state: str) -> None:
        self._state = state
        self._modified = datetime.now()

    # -------------------------------------------------------------------------------------------- #
    @property
    def task_names(self) -> list:
        return list(self._tasks.keys())

    # -------------------------------------------------------------------------------------------- #
    def create_task(self, name: str, description: str) -> JobComponent:
        return Task(name=name, parent=self, description=description)

    # -------------------------------------------------------------------------------------------- #
    def add_task(self, task: JobComponent) -> None:
        self._tasks[task.name] = task
        self._modified = datetime.now()

    # -------------------------------------------------------------------------------------------- #
    def get_task(self, name: str = None) -> None:
        try:
            return self._tasks[name]
        except KeyError:
            msg = f"Job {self._name} has no task with name = {name}."
            self._logger.error(msg)
            raise FileNotFoundError(msg)

    # -------------------------------------------------------------------------------------------- #
    def update_task(self, task: JobComponent) -> None:
        self._tasks[task.name] = task
        self._modified = datetime.now()

    # -------------------------------------------------------------------------------------------- #
    def remove_task(self, name: str) -> None:
        del self._tasks[name]
        self._modified = datetime.now()

    # -------------------------------------------------------------------------------------------- #
    def as_dto(self) -> JobDTO:

        dto = JobDTO(
            id=self._id,
            oid=self._oid,
            name=self._name,
            description=self._description,
            mode=self._mode,
            state=self._state,
            created=self._created,
            modified=self._modified,
        )
        return dto


# ------------------------------------------------------------------------------------------------ #
#                                        DATASET                                                   #
# ------------------------------------------------------------------------------------------------ #
class Task(JobComponent):
    """Task is a pipeline step or operation in execution.

    Args:
        name (str): Short, yet descriptive lowercase name for Task object.
        description (str): Describes the Task object. Default's to parent's description if None.
        parent (Job): The parent Job instance. Optional.
        mode (str): Mode for which the Task is created. If None, defaults to mode from environment
            variable.

    """

    def __init__(
        self,
        name: str,
        parent: Job,
        description: str = None,
        mode: str = None,
    ) -> None:
        super().__init__(name=name, description=description, mode=mode)

        self._parent = parent
        self._is_composite = False

        self._validate()

    def __str__(self) -> str:
        return f"Task Id: {self._id}\n\tName: {self._name}\n\tDescription: {self._description}\n\tMode: {self._mode}\n\tState: {self._state}\n\tCreated: {self._created}\n\tModified: {self._modified}"

    def __repr__(self) -> str:
        return f"{self._id}, {self._name}, {self._description}, {self._mode}, {self._state}, {self._created}, {self._modified}"

    def __eq__(self, other) -> bool:
        """Compares two Job for equality.
        Job are considered equal solely if their underlying data are equal.

        Args:
            other (Task): The Task object to compare.
        """

        if isinstance(other, Task):
            return (self._name == other.name
                    and self._description == other.description
                    and self._mode == other.mode
                    and self._parent == other.parent)
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
    def state(self) -> str:
        """The overall state of the task."""
        return self._state

    @state.setter
    def state(self, state: str) -> None:
        self._state = state
        self._modified = datetime.now()

    # -------------------------------------------------------------------------------------------- #
    @property
    def parent(self) -> Job:
        return self._parent

    # ------------------------------------------------------------------------------------------------ #
    def as_dto(self) -> TaskDTO:
        return TaskDTO(
            id=self._id,
            oid=self._oid,
            name=self._name,
            description=self._description,
            mode=self._mode,
            state=self._state,
            job_id=self._parent.id,
            created=self._created,
            modified=self._modified,
        )
