#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/entity/task.py                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday December 7th 2022 08:03:23 pm                                             #
# Modified   : Sunday December 18th 2022 09:32:37 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Task Entity Module"""
from typing import Union, Any
from datetime import datetime

from recsys.core.dal.dto import TaskDTO
from recsys.core.entity.dataset import Dataset
from recsys.core.entity.operation import Operation
from .base import Entity
from recsys.core.dal.base import DTO
from recsys.core.dal.repo import Context


# ------------------------------------------------------------------------------------------------ #


class Task(Entity):  # pragma: no cover
    """Task is an atomic unit of work, performed by an operation, given some input to produce an output.

    Args:
        name (str): Short, yet descriptive lowercase name for Task object.
        description (str): Description of fileset
        mode (str): The mode in which the task is performed.
        operation (Operation): Operation that executes the task.
        job_id (int): The id for the job of which, the task is part.


    Attributes:
        profile (Profile): The profile object containing, cpu, memory, disk and network utilization.
    """

    def __init__(self, name: str, mode: str, operation: Operation, job_id: int, description: str = None) -> None:
        super().__init__(self, name=name, description=description)
        self._mode = mode
        self._operation = operation
        self._job_id = job_id
        self._id = None

        self._started = None
        self._ended = None
        self._duration = None

        self._input = None
        self._output = None

    def __str__(self) -> str:
        return f"\n\nTask Id: {self._id}\n\tJob Id: {self._job_id}\n\tName: {self._name}\n\tDescription: {self._description}\n\tWorkspace: {self._mode}\n\tOperation: {self._operation.__class__.__name__}"

    def __repr__(self) -> str:
        return f"{self._id}, {self._job_id}, {self._name}, {self._description}, {self._mode}, {self._operation.__class__.__name__}"

    def __eq__(self, other) -> bool:
        """Compares two Tasks for equality."""

        if isinstance(other, Task):
            return (
                self._job_id == other.job_id
                and self._name == other.name
                and self._description == other.description
                and self._mode == other.mode
                and self._operation == other.operation
                and self._input == other.input
                and self._output == other.output
            )
        else:
            return False

    # ------------------------------------------------------------------------------------------------ #
    @property
    def job_id(self) -> int:
        return self._job_id

    @property
    def mode(self) -> str:
        return self._mode

    @property
    def started(self) -> str:
        return self._started

    @property
    def ended(self) -> str:
        return self._ended

    @property
    def duration(self) -> str:
        return self._duration

    @property
    def operation(self) -> Operation:
        return self._operation

    @operation.setter
    def operation(self, operation: Operation) -> None:
        if self._operation is None:
            self._operation = operation
            self._modified = datetime.now()
        elif not self._operation == operation:
            msg = "Item reassignment is not supported for the 'operation' member."
            self._logger.error(msg)
            raise TypeError(msg)

    @property
    def input(self) -> Dataset:
        return self._input

    @input.setter
    def input(self, input: Dataset) -> None:
        if self._input is None:
            self._input = input
            self._modified = datetime.now()
        elif not self._input == input:
            msg = "Item reassignment is not supported for the 'input' member."
            self._logger.error(msg)
            raise TypeError(msg)

    @property
    def output(self) -> int:
        return self._output

    @output.setter
    def output(self, output: Dataset) -> None:
        if self._output is None:
            self._output = output
            self._modified = datetime.now()
        elif not self._output == output:
            msg = "Item reassignment is not supported for the 'output' member."
            self._logger.error(msg)
            raise TypeError(msg)

    # ------------------------------------------------------------------------------------------------ #
    def run(self, data: Union[None, Dataset] = None, context: Context = None) -> Any:
        self._setup()
        result = self._operation.execute(data=data, context=context)
        data = result or data
        self._teardown()

    # ------------------------------------------------------------------------------------------------ #
    def _setup(self) -> None:
        self._started = datetime.now()

    def _teardown(self) -> None:
        self._ended = datetime.now()
        self._duration = (self._ended - self._started).total_seconds()

    # ------------------------------------------------------------------------------------------------ #
    def _from_dto(self, dto: DTO) -> Entity:
        self._id = dto.id
        self._name = dto.name
        self._description = dto.description
        self._mode = dto.mode
        self._stage = dto.stage
        self._job_id = dto.job_id
        self._started = dto.started
        self._ended = dto.ended
        self._duration = dto.duration
        self._created = dto.created
        self._modified = dto.modified

        self._operation = None
        self._input = None
        self._output = None

        self._validate()

    def as_dto(self) -> TaskDTO:
        """Returns a TaskDTO object."""
        return TaskDTO(
            id=self._id,
            name=self._name,
            description=self._description,
            mode=self._mode,
            stage=self._stage,
            job_id=self._job_id,
            started=self._started,
            ended=self._ended,
            duration=self._duration,
            created=self._created,
            modified=self._modified,
        )

    def _validate(self) -> None:
        super()._validate()

        def announce_and_raise(msg):
            self._logger.error(msg)
            raise TypeError(msg)

        if not isinstance(self._job_id, int):
            msg = f"Error instantiating {self.__class__.__name__}. Attribute 'job_id' must be of type 'int', not {type(self._job_id)}."
            announce_and_raise(msg)

        if not isinstance(self._operation, str):
            msg = f"Error instantiating {self.__class__.__name__}. Attribute 'operation' must be of type str,  not {type(self._operation)}."
            announce_and_raise(msg)
