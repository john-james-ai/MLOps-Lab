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
# Modified   : Sunday December 11th 2022 01:03:47 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Fileset Entity Module"""
from typing import Union

from recsys.core.dal.dto import TaskDTO, FilesetDTO, DatasetDTO
from .base import Entity, Profile, DTO

# ------------------------------------------------------------------------------------------------ #


class Task(Entity):  # pragma: no cover
    """Task is an atomic unit of work, performed by an operator, given some input to produce an output.

    Args:
        job_id (int): The id for the job of which, the task is a part.
        name (str): Short, yet descriptive lowercase name for Fileset object.
        description (str): Description of fileset
        workspace (str): The workspace in which the task is performed.
        operator (str): The class name for the operator performing the task.
        module (str): The module containing the operator.


    Attributes:
        profile (Profile): The profile object containing, cpu, memory, disk and network utilization.
    """

    def __init__(self, job_id: int, name: str, operator: str, module: str, input_kind: str, input_id: int, output_kind: str, output_id: int, workspace: str = None, description: str = None) -> None:
        super().__init__(self, name=name, description=description)
        self._job_id = job_id
        self._workspace = workspace
        self._operator = operator
        self._module = module

        self._input_id = None
        self._input_kind = None
        self._output_id = None
        self._output_kind = None

    def __str__(self) -> str:
        return f"\n\nTask Id: {self._id}\n\tJob Id: {self._job_id}\n\tName: {self._name}\n\tDescription: {self._description}\n\tWorkspace: {self._workspace}\n\tOperator: {self._operator}\n\tModule: {self._module}\n\tInput_kind: {self._input_kind}\n\tInput_id: {self._input_id}\n\tOutput_kind: {self._output_kind}\n\tOutput_id: {self._output_id}"

    def __repr__(self) -> str:
        return f"{self._id}, {self._job_id}, {self._name}, {self._description}, {self._workspace}, {self._operator}, {self._module}, {self._input_kind}, {self._input_id}, {self._output_kind}, {self._output_id}"

    def __eq__(self, other) -> bool:
        """Compares two Filesets for equality."""

        if isinstance(other, Task):
            return (
                self._job_id == other.job_id
                and self._name == other.name
                and self._description == other.description
                and self._workspace == other.workspace
                and self._operator == other.operator
                and self._module == other.module
                and self._input_kind == other.input_kind
                and self._input_id == other.input_id
                and self._output_kind == other.output_kind
                and self._output_id == other.output_id
            )

    # ------------------------------------------------------------------------------------------------ #
    @property
    def job_id(self) -> int:
        return self._job_id

    @property
    def workspace(self) -> str:
        return self._workspace

    @property
    def operator(self) -> str:
        return self._operator

    @property
    def module(self) -> str:
        return self._module

    @property
    def input_kind(self) -> str:
        return self._input_kind

    @property
    def input_id(self) -> int:
        return self._input_id

    @property
    def output_kind(self) -> str:
        return self._output_kind

    @property
    def output_id(self) -> int:
        return self._output_id

    @property
    def profile(self) -> Profile:
        self._profile

    @profile.setter
    def profile(self, profile: Profile) -> None:
        self._profile = profile

    # ------------------------------------------------------------------------------------------------ #
    def add_input(self, input_data: Union[FilesetDTO, DatasetDTO]) -> None:
        self._input_kind = input_data.__class__.__name__
        self._input_id = input_data.id

    def add_output(self, output_data: Union[FilesetDTO, DatasetDTO]) -> None:
        self._output_kind = output_data.__class__.__name__
        self._output_id = output_data.id

    # ------------------------------------------------------------------------------------------------ #
    def _from_dto(self, dto: DTO) -> Entity:
        self._id = dto.id
        self._job_id = dto.job_id
        self._name = dto.name
        self._description = dto.description
        self._workspace = dto.workspace
        self._operator = dto.operator
        self._module = dto.module
        self._input_kind = dto.input_kind
        self._input_id = dto.input_id
        self._output_kind = dto.output_kind
        self._output_id = dto.output_id
        self._profile = dto.profile
        self._created = dto.created
        self._modified = dto.modified

        self._validate()

    def as_dto(self) -> TaskDTO:
        """Returns a TaskDTO object."""
        return TaskDTO(
            id=self._id,
            job_id=self._job_id,
            name=self._name,
            description=self._description,
            workspace=self._workspace,
            operator=self._operator,
            module=self._module,
            input_kind=self._input_kind,
            input_id=self._input_id,
            output_kind=self._output_kind,
            output_id=self._output_id,
            profile=self._profile,
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

        if not isinstance(self._operator, str):
            msg = f"Error instantiating {self.__class__.__name__}. Attribute 'operator' must be of type str,  not {type(self._operator)}."
            announce_and_raise(msg)

        if not isinstance(self._module, str):
            msg = f"Error instantiating {self.__class__.__name__}. Attribute 'module' must be of type str,  not {type(self._module)}."
            announce_and_raise(msg)

        if not isinstance(self._input_kind, str):
            msg = f"Error instantiating {self.__class__.__name__}. Attribute 'input_kind' must be of type str,  not {type(self._input_kind)}."
            announce_and_raise(msg)

        if not isinstance(self._input_id, int):
            msg = f"Error instantiating {self.__class__.__name__}. Attribute 'input_id' must be of type int,  not {type(self._input_id)}."
            announce_and_raise(msg)

        if not isinstance(self._output_kind, str):
            msg = f"Error instantiating {self.__class__.__name__}. Attribute 'output_kind' must be of type str,  not {type(self._output_kind)}."
            announce_and_raise(msg)

        if not isinstance(self._output_id, int):
            msg = f"Error instantiating {self.__class__.__name__}. Attribute 'output_id' must be of type int,  not {type(self._output_id)}."
            announce_and_raise(msg)
