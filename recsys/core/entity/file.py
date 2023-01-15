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
# Modified   : Saturday January 14th 2023 05:10:31 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""File Entity Module"""
import os
from datetime import datetime

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
        datasource_oid (str): Source of File
        stage (str): Data processing stage, i.e. extract or raw.
        uri (str): The path to the file on disk.
        task_oid (int): The identifier for the Task object which created the File object.

    """

    def __init__(
        self,
        name: str,
        datasource_oid: int,
        stage: str,
        uri: str,
        description: str = None,
        task_oid: str = 0,
    ) -> None:
        super().__init__(name=name, description=description)
        self._datasource_oid = datasource_oid
        self._stage = stage
        self._uri = uri
        self._task_oid = task_oid

        self._size = None
        self._set_metadata()
        self._validate()

    def __str__(self) -> str:
        return f"File Id: {self._id}\n\t OID: {self._oid}\n\tData source: {self._datasource_oid}\n\tName: {self._name}\n\tDescription: {self._description}\n\tStage: {self._stage}\n\tSize: {self._size}\n\tTask Id: {self._task_oid}\n\tCreated: {self._created}\n\tModified: {self._modified}"

    def __repr__(self) -> str:
        return f"{self._id}, {self._oid}, {self._datasource_oid}, {self._name}, {self._description}, {self._stage}, {self._size}, {self._task_oid}, {self._created}, {self._modified}"

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
    def datasource_oid(self) -> str:
        """Datasource from which the File Component has derived."""
        return self._datasource_oid

    # -------------------------------------------------------------------------------------------- #
    @property
    def stage(self) -> str:
        """Data processing stage in which the File Component is created."""
        return self._stage

    # -------------------------------------------------------------------------------------------- #
    @property
    def uri(self) -> str:
        return self._uri

    # -------------------------------------------------------------------------------------------- #
    @property
    def size(self) -> int:
        return self._size

    # -------------------------------------------------------------------------------------------- #
    @property
    def task_oid(self) -> int:
        return self._task_oid

    @task_oid.setter
    def task_oid(self, task_oid: str) -> None:
        self._task_oid = task_oid
        self._modified = datetime.now()

    # ------------------------------------------------------------------------------------------------ #
    def as_dto(self) -> FileDTO:
        return FileDTO(
            id=self._id,
            oid=self._oid,
            name=self._name,
            description=self._description,
            datasource_oid=self._datasource_oid,
            stage=self._stage,
            uri=self._uri,
            size=self._size,
            task_oid=self._task_oid,
            created=self._created,
            modified=self._modified,
        )

    # ------------------------------------------------------------------------------------------------ #
    def _set_metadata(self) -> None:
        if self._uri is not None:
            if os.path.exists(self._uri):
                self._size = os.path.getsize(self._uri)
