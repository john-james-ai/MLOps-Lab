#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/entity/operation.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 07:32:54 pm                                                #
# Modified   : Saturday December 17th 2022 08:50:55 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Operation Entity Module"""
from recsys.core.dal.dto import OperationDTO
from recsys.core.services.operator import Operator
from .base import Entity, DTO


# ------------------------------------------------------------------------------------------------ #
#                                        DATASET                                                   #
# ------------------------------------------------------------------------------------------------ #
class Operation(Entity):
    """Operation encapsulates tabular data, metadata, and access behaviors for data used in this package.

    Args:
        name (str): Short, yet descriptive lowercase name for Operation object.
        description (str): Describes the Operation object.
        workspace (str): The workspace in which the operation is executed.
        stage (str): The stage in which the operation is executed
        operator (Operator): The Operator Instance
        task_id (int): The identifier for the task in which this operation takes place.

    """

    def __init__(
        self,
        name: str,
        workspace: str,
        stage: str,
        operator: Operator,
        task_id: int = None,
        description: str = None,
    ) -> None:
        super().__init__(name=name, description=description)

        self._workspace = workspace
        self._stage = stage
        self._operator = operator
        self._task_id = task_id

        # Assigned by repo
        self._uri = self._get_uri()

        # Validate entity
        self._validate()

    def __str__(self) -> str:
        return f"\n\nOperation Id: {self._id}\n\tName: {self._name}\n\tDescription: {self._description}\n\t\n\tWorkspace: {self._workspace}\n\tStage: {self._stage}\n\tTask Id: {self._task_id}\n\tCreated: {self._created}\n\tModified: {self._modified}"

    def __repr__(self) -> str:
        return f"{self._id}, {self._name}, {self._description}, {self._workspace}, {self._stage}, {self._task_id}, {self._created}, {self._modified}"

    def __eq__(self, other) -> bool:
        """Compares two Operations for equality.
        Operations are considered equal solely if their underlying data are equal.

        Args:
            other (Operation): The Operation object to compare.
        """

        if isinstance(other, Operation):
            return (self._name == other.name
                    and self._workspace == other.workspace
                    and self._stage == other.stage
                    and self._operator == other._operator
                    and self._task_id == other.task_id)
        else:
            return False

    # ------------------------------------------------------------------------------------------------ #
    @property
    def task_id(self) -> int:
        return self._task_id

    @task_id.setter
    def task_id(self, task_id: int) -> None:
        if self._task_id is None:
            self._task_id = task_id
        elif not self._task_id == task_id:
            msg = "Item reassignment is not supported for the 'task_id' member."
            self._logger.error(msg)
            raise TypeError(msg)

    @property
    def workspace(self) -> str:
        return self._workspace

    @property
    def stage(self) -> str:
        return self._stage

    @property
    def uri(self) -> str:
        return self._uri

    @property
    def operator(self) -> Operator:
        return self._operator

    # ------------------------------------------------------------------------------------------------ #
    def as_dto(self) -> OperationDTO:
        return OperationDTO(
            id=self._id,
            name=self._name,
            description=self._description,
            workspace=self._workspace,
            stage=self._stage,
            uri=self._uri,
            task_id=self._task_id,
            created=self._created,
            modified=self._modified,
        )

    # ------------------------------------------------------------------------------------------------ #
    def _from_dto(self, dto: DTO) -> None:
        super().__init__(name=dto.name, description=dto.description)
        self._id = dto.id
        self._name = dto.name
        self._description = dto.description
        self._workspace = dto.workspace
        self._stage = dto.stage
        self._uri = dto.uri
        self._task_id = dto.task_id
        self._created = dto.created
        self._modified = dto.modified
        self._operator = None
        self._validate()

    # ------------------------------------------------------------------------------------------------ #
    def _validate(self) -> None:
        super()._validate()
        if self._operator is not None:
            if not isinstance(self._operator, Operator):
                msg = f"operator must be of type Operator, not {type(self._operator)}"
                self._logger.error(msg)
                raise TypeError(msg)
        if self._task_id is not None:
            if not isinstance(self._task_id, int):
                msg = f"task_id must be an integer, not {type(self._task_id)}"
                self._logger.error(msg)
                raise TypeError(msg)
