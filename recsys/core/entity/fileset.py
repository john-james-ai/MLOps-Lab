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
# Modified   : Saturday December 10th 2022 06:40:23 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Fileset Entity Module"""
import os

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
        uri (str): The path to the file relative to the project root directory.
        task_id (int): The step within a pipeline task that produced the Fileset object.

    """

    def __init__(
        self,
        name: str,
        datasource: str,
        stage: str,
        uri: str,
        task_id: int,
        workspace: str = None,
        description: str = None,
    ) -> None:
        super().__init__(name=name, description=description)
        self._stage = stage
        self._workspace = workspace
        self._datasource = datasource
        self._uri = uri
        self._task_id = task_id
        self._filesize = None

        self._validate()
        self._set_metadata()

    def __str__(self) -> str:
        return f"\n\nFileset Id: {self._id}\n\tName: {self._name}\n\tDescription: {self._description}\n\tData Source: {self._datasource}\n\tWorkspace: {self._workspace}\n\tStage: {self._stage}\n\tFilepath: {self._uri}\n\tTask Id: {self._task_id}\n\tCreated: {self._created}\n\tModified: {self._modified}"

    def __repr__(self) -> str:
        return f"{self._id},{self._name},{self._description},{self._datasource},{self._workspace}, {self._stage}, {self._uri},{self._task_id},{self._created},{self._modified}"

    def __eq__(self, other) -> bool:
        """Compares two Filesets for equality.
        """

        if isinstance(other, Fileset):
            return (
                self._name == other.name
                and self._datasource == other.datasource
                and self._filesize == other.filesize
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
    def uri(self) -> str:
        return self._uri

    @property
    def filesize(self) -> str:
        return self._filesize

    @property
    def task_id(self) -> int:
        return self._task_id

    # ------------------------------------------------------------------------------------------------ #

    def as_dto(self) -> FilesetDTO:
        return FilesetDTO(
            id=self._id,
            name=self._name,
            description=self._description,
            datasource=self._datasource,
            workspace=self._workspace,
            stage=self._stage,
            uri=self._uri,
            filesize=self._filesize,
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
        self._uri = dto.uri
        self._filesize = dto.filesize
        self._task_id = dto.task_id
        self._created = dto.created
        self._modified = dto.modified
        self._validate()

    # ------------------------------------------------------------------------------------------------ #
    def _set_metadata(self) -> None:
        if self._uri is not None:
            if os.path.isfile(self._uri):
                if os.path.exists(self._uri):
                    self._filesize = os.path.getsize(self._uri)
