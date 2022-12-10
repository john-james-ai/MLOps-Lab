#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/entity/fileset.py                                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 07:32:54 pm                                                #
# Modified   : Saturday December 10th 2022 04:16:09 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Fileset Entity Module"""

from recsys.core.dal.dto import FilesetDTO
from .base import Entity

# ------------------------------------------------------------------------------------------------ #


class Fileset(Entity):
    """Fileset encapsulates the files stored in the app.
    Args:
        name (str): Short, yet descriptive lowercase name for Fileset object.
        description (str): Description of fileset
        datasource (str): The data datasource
        workspace (str): Either ['data','prod','dev','test']
        stage (str): The data processing stage
        filepath (str): The path to the file relative to the project root directory.
        task_id (int): The step within a pipeline task that produced the Fileset object.

    """

    def __init__(
        self,
        name: str,
        datasource: str,
        stage: str,
        filepath: str,
        task_id: int,
        workspace: str = None,
        description: str = None,
    ) -> None:
        super().__init__(name=name, description=description)
        self._stage = stage
        self._workspace = workspace
        self._datasource = datasource
        self._filepath = filepath
        self._task_id = task_id

        self._validate()

    def __str__(self) -> str:
        return f"\n\nFileset Id: {self._id}\n\tName: {self._name}\n\tDescription: {self._description}\n\tData Source: {self._datasource}\n\tWorkspace: {self._workspace}\n\tStage: {self._stage}\n\tFilepath: {self._filepath}\n\tTask Id: {self._task_id}\n\tCreated: {self._created}\n\tModified: {self._modified}"

    def __repr__(self) -> str:
        return f"{self._id},{self._name},{self._description},{self._datasource},{self._workspace}, {self._stage}, {self._filepath},{self._task_id},{self._created},{self._modified}"

    def __eq__(self, other) -> bool:
        """Compares two Filesets for equality.
        """

        if isinstance(other, Fileset):
            return (
                self._name == other.name
                and self._description == other.description
                and self._datasource == other.datasource


            )
        else:
            return False

    # ------------------------------------------------------------------------------------------------ #
    @property
    def datasource(self) -> str:
        return self._datasource

    @property
    def workspace(self) -> str:
        return self._workspace

    @property
    def stage(self) -> str:
        return self._stage

    @property
    def filepath(self) -> str:
        return self._filepath

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

    # ------------------------------------------------------------------------------------------------ #

    def as_dto(self) -> FilesetDTO:
        return FilesetDTO(
            id=self._id,
            name=self._name,
            description=self._description,
            datasource=self._datasource,
            workspace=self._workspace,
            stage=self._stage,
            filepath=self._filepath,
            task_id=self._task_id,
            created=self._created,
            modified=self._modified,
        )

    # ------------------------------------------------------------------------------------------------ #

    def _from_dto(self, dto: FilesetDTO) -> None:
        super().__init__(name=dto.name, description=dto.description)
        self._id = dto.id
        self._datasource = dto.datasource
        self._workspace = dto.workspace
        self._stage = dto.stage
        self._filepath = dto.filepath
        self._task_id = dto.task_id
        self._created = dto.created
        self._modified = dto.modified
        self._validate()
