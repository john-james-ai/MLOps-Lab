#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/workflow/task.py                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday December 7th 2022 08:03:23 pm                                             #
# Modified   : Tuesday December 20th 2022 08:12:38 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Task Entity Module"""
from typing import Union, Any
from datetime import datetime

from recsys.core.dal.dto import TaskDTO
from recsys.core.entity.dataset import Dataset
from recsys.core.workflow.operator import Operator
from .base import Process
from recsys.core.dal.base import DTO
from recsys.core.dal.repo import Context


# ------------------------------------------------------------------------------------------------ #
class Task(Process):  # pragma: no cover
    """Task is an atomic unit of work, performed by an operator, given some input to produce an output"""

    def __init__(self) -> None:
        super().__init__()
        self._input = None
        self._operator = None
        self._output = None
        self._job_id = None

    # ------------------------------------------------------------------------------------------------ #
    @property
    def job_id(self) -> int:
        return self._job_id

    @job_id.setter
    def job_id(self, job_id: int) -> None:
        if self._job_id is None:
            self._job_id = job_id
            self._modified = datetime.now()
        else:
            msg = "Item reassignment is not supported for the 'job_id' member."
            self._logger.error(msg)
            raise TypeError(msg)

    @property
    def operator(self) -> Operator:
        return self._operator

    @operator.setter
    def operator(self, operator: Operator) -> None:
        if self._operator is None:
            self._operator = operator
            self._modified = datetime.now()
        elif not self._operator == operator:
            msg = "Item reassignment is not supported for the 'operator' member."
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
        result = self._operator.execute(data=data, context=context)
        data = result or data
        self._teardown()
        return data

    # ------------------------------------------------------------------------------------------------ #
    def _from_dto(self, dto: DTO) -> Process:
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

        self._operator = None
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

        if not isinstance(self._job_id, int):
            msg = f"Error instantiating {self.__class__.__name__}. Attribute 'job_id' must be of type 'int', not {type(self._job_id)}."
            self._logger.error(msg)
            raise TypeError(msg)

        if not isinstance(self._operator, str):
            msg = f"Error instantiating {self.__class__.__name__}. Attribute 'operator' must be of type str,  not {type(self._operator)}."
            self._logger.error(msg)
            raise TypeError(msg)
