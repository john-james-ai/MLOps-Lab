#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/entity/file.py                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 07:32:54 pm                                                #
# Modified   : Friday December 30th 2022 07:45:53 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""File Entity Module"""
import os

from recsys.core.entity.base import Entity
from recsys.core.dal.dto import FileDTO


# ------------------------------------------------------------------------------------------------ #
#                                         FILE                                                     #
# ------------------------------------------------------------------------------------------------ #
class File(Entity):
    """File encapsulates a single file on disk.

    Args:
        name (str): Short, yet descriptive lowercase name for File object.
        description (str): Describes the File object. Default's to parent's description if None.
        datasource (str): Source of File
        mode (str): Mode for which the File is created. If None, defaults to mode from environment
            variable.
        stage (str): Data processing stage, i.e. extract or raw.
        uri (str): The path to the file on disk.
        task_id (int): The identifier for the Task object which created the File object.

    """

    def __init__(
        self,
        name: str,
        datasource: str,
        stage: str,
        uri: str,
        description: str = None,
        mode: str = None,
        task_id: int = 0
    ) -> None:
        super().__init__(name=name, description=description, mode=mode)
        self._datasource = datasource
        self._stage = stage
        self._uri = uri
        self._task_id = task_id

        self._size = None
        self._set_metadata()
        self._validate()

    def __str__(self) -> str:
        return f"File Id: {self._id}\n\tData source: {self._datasource}\n\tName: {self._name}\n\tDescription: {self._description}\n\tMode: {self._mode}\n\tStage: {self._stage}\n\tSize: {self._size}\n\tTask Id: {self._task_id}\n\tCreated: {self._created}\n\tModified: {self._modified}"

    def __repr__(self) -> str:
        return f"{self._id}, {self._datasource}, {self._name}, {self._description}, {self._mode}, {self._stage}, {self._size}, {self._task_id}, {self._created}, {self._modified}"

    def __eq__(self, other) -> bool:
        """Compares two File for equality.
        File are considered equal solely if their underlying data are equal.

        Args:
            other (File): The File object to compare.
        """

        if isinstance(other, File):
            return self._uri == other.uri

    # -------------------------------------------------------------------------------------------- #
    @property
    def datasource(self) -> str:
        """Datasource from which the File Component has derived."""
        return self._datasource

    # -------------------------------------------------------------------------------------------- #
    @property
    def stage(self) -> str:
        """Data processing stage in which the File Component is created."""
        return self._stage

    # -------------------------------------------------------------------------------------------- #
    @property
    def uri(self) -> str:
        return self._uri

    @uri.setter
    def uri(self, uri: str) -> None:
        self._uri = uri
        self._set_metadata()

    # -------------------------------------------------------------------------------------------- #
    @property
    def size(self) -> int:
        return self._size

    # -------------------------------------------------------------------------------------------- #
    @property
    def task_id(self) -> int:
        return self._task_id

    # ------------------------------------------------------------------------------------------------ #
    def as_dto(self) -> FileDTO:
        return FileDTO(
            id=self._id,
            oid=self._oid,
            name=self._name,
            description=self._description,
            datasource=self._datasource,
            mode=self._mode,
            stage=self._stage,
            uri=self._uri,
            size=self._size,
            task_id=self._task_id,
            created=self._created,
            modified=self._modified,
        )

    # ------------------------------------------------------------------------------------------------ #
    def _set_metadata(self) -> None:
        if self._uri is not None:
            if os.path.exists(self._uri):
                self._size = os.path.getsize(self._uri)
