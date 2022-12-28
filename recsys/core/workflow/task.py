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
# Modified   : Wednesday December 28th 2022 06:27:56 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Task Entity Module"""
from typing import Union, Any
from datetime import datetime

from recsys.core.dal.dto import TaskDTO
from recsys.core.entity.base import Entity, Spec
from recsys.core.workflow.operator import Operator
from .base import Process
from recsys.core.dal.base import DTO
from recsys.core.repo.base import Context


# ------------------------------------------------------------------------------------------------ #
class Task(Process):  # pragma: no cover
    """Task is an atomic unit of work, performed by an operator, given some input to produce an output

    Args:
        name (str): Task name
        description (str): Task description
        input_spec (Spec): Specification for input to task
        output_spec (Spec): Specification for output of the task
        operator (Operator): The instance performing the task operation.
        job_id (int): The job to which the task belongs.
    """

    def __init__(self, name: str, job_id: int, description: str = None, mode: str = None) -> None:
        super().__init__(name=name, description=description, mode=mode)
        self._job_id = job_id

        self._input_spec = None
        self._output_spec = None
        self._operator = None

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
    def stage(self) -> str:
        return self._stage

    @property
    def operator(self) -> Operator:
        return self._operator

    @operator.setter
    def operator(self, operator: Operator) -> None:
        self._operator = operator

    @property
    def input_spec(self) -> Entity:
        return self._input_spec

    @input_spec.setter
    def input_spec(self, input_spec: Spec) -> None:
        self._input_spec = input_spec

    @property
    def output_spec(self) -> int:
        return self._output_spec

    @output_spec.setter
    def output_spec(self, output_spec: Spec) -> None:
        self._output_spec = output_spec

    # ------------------------------------------------------------------------------------------------ #
    def run(self, data: Union[None, Entity] = None, context: Context = None) -> Any:
        self._setup()
        result = self._operator.execute(data=data, context=context)
        data = result or data
        self._teardown()
        return data

    # ------------------------------------------------------------------------------------------------ #
    def _from_dto(self, dto: DTO) -> Process:
        self._id = dto.id
        self._oid = dto.oid
        self._name = dto.name
        self._description = dto.description
        self._mode = dto.mode
        self._stage = dto.stage
        self._job_id = dto.job_id
        self._started = dto.started
        self._ended = dto.ended
        self._duration = dto.duration
        self._state = dto.state
        self._created = dto.created
        self._modified = dto.modified

        self._operator = None
        self._input_spec = None
        self._output_spec = None

        self._validate()

    def as_dto(self) -> TaskDTO:
        """Returns a TaskDTO object."""
        return TaskDTO(
            id=self._id,
            oid=self._oid,
            name=self._name,
            description=self._description,
            mode=self._mode,
            stage=self._stage,
            job_id=self._job_id,
            started=self._started,
            ended=self._ended,
            duration=self._duration,
            state=self._state,
            created=self._created,
            modified=self._modified,
        )

    def _validate(self) -> None:
        super()._validate()

        if not isinstance(self._job_id, int):
            msg = f"Error instantiating {self.__class__.__name__}. Attribute 'job_id' must be of type 'int', not {type(self._job_id)}."
            self._logger.error(msg)
            raise TypeError(msg)

        if not isinstance(self._operator, Operator):
            msg = f"Error instantiating {self.__class__.__name__}. Attribute 'operator' must be of type str,  not {type(self._operator)}."
            self._logger.error(msg)
            raise TypeError(msg)
