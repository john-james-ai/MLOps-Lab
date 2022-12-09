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
# Modified   : Thursday December 8th 2022 05:02:36 pm                                              #
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
    """Fileset encapsulates the files during data acquisition.
    Args:
        *id (int): Unique integer identifier for the Fileset object.
        *name (str): Short, yet descriptive lowercase name for Fileset object.
        *description (str): Description of fileset
        *source (str): The data source
        url (str): Optional. The URL associated with the Fileset. For websources.
        filesize (float): The size of the file in Mb
        filepath (str): The path to the file relative to the project root directory.
        task_id (int): The step within a pipeline task that produced the Fileset object.
        *created (datetime): Datetime the Fileset was created.
        **modified (datetime): Datetime the Fileset was modified.

        * Managed by base class
        ** Instantiated by base class


    """

    def __init__(
        self,
        name: str,
        source: str,
        url: str,
        filepath: str,
        task_id: int,
        description: str = None,
    ) -> None:
        super().__init__(name=name, source=source, description=description)
        self._filepath = filepath
        self._task_id = task_id
        self._filepath = filepath
        self._filesize = None

        self._set_metadata()
        self._validate()

    def __str__(self) -> str:
        return f"\n\nFileset Id: {self._id}\n\tName: {self._name}\n\tDescription: {self._description}\n\tData Source: {self._source}\n\tFilesize: {self._filesize}\n\tFilepath: {self._filepath}\n\tStep_Id: {self._task_id}\n\tCreated: {self._created}\n\tModified: {self._modified}"

    def __repr__(self) -> str:
        return f"{self._id},{self._name},{self._description},{self._source},{self._filesize},{self._filepath},{self._task_id},{self._created},{self._modified}"

    def __eq__(self, other) -> bool:
        """Compares two Filesets for equality.
        Equality is defined by common name, source, and filesize.
        """

        if isinstance(other, Fileset):
            return (
                self._name == other.name
                and self._source == other.datasource
                and self._filesize == other.filesize
            )
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
    def filesize(self) -> str:
        return self._filesize

    @property
    def filepath(self) -> str:
        return self._filepath

    @property
    def task_id(self) -> int:
        return self._task_id

    # ------------------------------------------------------------------------------------------------ #
    def _set_metadata(self) -> None:
        if self._filepath:
            if os.path.exists(self._filepath):
                if os.path.isfile(self._filepath):
                    self._filesize = os.path.getsize(self._filepath)

    # ------------------------------------------------------------------------------------------------ #
    def as_dto(self) -> FilesetDTO:
        return FilesetDTO(
            id=self._id,
            name=self._name,
            description=self._description,
            source=self._source,
            filesize=self._filesize,
            filepath=self._filepath,
            task_id=self._task_id,
            created=self._created,
            modified=self._modified,
        )

    # ------------------------------------------------------------------------------------------------ #

    def _from_dto(self, dto: FilesetDTO) -> None:
        super().__init__(name=dto.name, description=dto.description)
        self._id = dto.id
        self._source = dto.datasource
        self._filesize = dto.filesize
        self._filepath = dto.filepath
        self._task_id = dto.task_id
        self._created = dto.created
        self._modified = dto.modified
        self._validate()
