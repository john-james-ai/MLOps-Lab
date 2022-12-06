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
# Modified   : Monday December 5th 2022 03:01:09 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Fileset Entity Module"""
import os
import inspect
from datetime import datetime

from recsys import SOURCES
from recsys.core.dal.dataset import FilesetDTO
from .base import Entity

# ------------------------------------------------------------------------------------------------ #


class Fileset(Entity):
    """Fileset encapsulates the files during data acquisition.
    Args:
        id (int): Unique integer identifier for the Fileset object.
        name (str): Short, yet descriptive lowercase name for Fileset object.
        source (str): The data source
        filesize (float): The size of the file in Mb
        filepath (str): The path to the file relative to the project root directory.
        task_id (int): The step within a pipeline task that produced the Fileset object.
        creator (str): The class of the object that created the Fileset.
        created (datetime): Datetime the Fileset was created
        modified (datetime): Datetime the Fileset was modified.


    """

    def __init__(
        self,
        id: int = None,
        name: str = None,
        source: str = None,
        filesize: float = None,
        filepath: str = None,
        task_id: int = None,
        creator: str = None,
        created: datetime = None,
        modified: datetime = None,
    ) -> None:
        self._id = id
        self._name = name
        self._source = source
        self._filesize = filesize
        self._filepath = filepath
        self._task_id = task_id
        self._creator = creator
        self._created = created
        self._modified = modified
        super().__init__()

        # Set metadata
        self._set_operational_metadata()
        if os.path.exists(self._filepath):
            self._set_data_metadata()

    def __str__(self) -> str:
        return f"\n\nFileset Id: {self._id}\n\tName: {self._name}\n\tData Source: {self._source}\n\tFilesize: {self._filesize}\n\tFilepath: {self._filepath}\n\tStep_Id: {self._task_id}\n\tCreator: {self._creator}\n\tCreated: {self._created}\n\tModified: {self._modified}"

    def __repr__(self) -> str:
        return f"{self._id},{self._name},{self._source},{self._filesize},{self._filepath},{self._task_id},{self._creator},{self._created},{self._modified}"

    def __eq__(self, other) -> bool:
        """Compares two Filesets for equality.
        Equality is defined by common name, source, and filesize.
        """

        if isinstance(other, Fileset):
            return (
                self._name == other.name
                and self._source == other.source
                and self._filesize == other.filesize
            )

    # ------------------------------------------------------------------------------------------------ #
    # Id var and variables assigned at instantiation
    @property
    def id(self) -> int:
        return self._id

    @id.setter
    def id(self, id: int) -> None:
        if self._id is None:
            self._id = id
            self._modified = datetime.now()
        elif not self._id == id:
            msg = "Item reassignment is not supported for the 'id' member."
            self._logger.error(msg)
            raise TypeError(msg)

    @property
    def name(self) -> str:
        return self._name

    @property
    def source(self) -> str:
        return self._source

    @property
    def filesize(self) -> str:
        return self._filesize

    @property
    def filepath(self) -> str:
        return self._filepath

    @property
    def task_id(self) -> int:
        return self._task_id

    @property
    def creator(self) -> str:
        return self._creator

    @property
    def created(self) -> datetime:
        return self._created

    @property
    def modified(self) -> datetime:
        return self._modified

    # ------------------------------------------------------------------------------------------------ #
    # Validation logic

    def validate(self) -> None:
        def announce_and_raise_value_error(msg: str) -> None:
            self._logger.error(msg)
            raise ValueError(msg)

        def announce_and_raise_type_error(msg: str) -> None:
            self._logger.error(msg)
            raise TypeError(msg)

        if self._name is None:
            msg = "Name is a required value for Fileset objects."
            announce_and_raise_value_error(msg)

        if self._source not in SOURCES:
            msg = f"Workspace is {self._source} is invalid. Must be one of {SOURCES}."
            announce_and_raise_value_error(msg)

        if not isinstance(self._filesize, float):
            msg = "Invalid filesize. Filesize must be a type float."
            announce_and_raise_type_error(msg)

    # ------------------------------------------------------------------------------------------------ #
    def _set_operational_metadata(self) -> None:
        self._created = self._created or datetime.now()
        self._modified = datetime.now()
        stack = inspect.stack()
        try:
            self._creator = self._creator or stack[3][0].f_locals["self"].__class__.__name__
        except KeyError:
            self._creator = "Not Designated"

    # ------------------------------------------------------------------------------------------------ #
    def _set_data_metadata(self) -> None:
        self._filesize = os.path.getsize(self._filepath)

    # ------------------------------------------------------------------------------------------------ #
    def as_dto(self) -> FilesetDTO:
        return FilesetDTO(
            id=self._id,
            name=self._name,
            source=self._source,
            filesize=self._filesize,
            filepath=self._filepath,
            task_id=self._task_id,
            creator=self._creator,
            created=self._created,
            modified=self._modified,
        )

    # ------------------------------------------------------------------------------------------------ #
    def from_dto(self, dto: FilesetDTO) -> None:
        self._id = dto.id
        self._name = dto.name
        self._source = dto.source
        self._filesize = dto.filesize
        self._filepath = dto.filepath
        self._task_id = dto.task_id
        self._creator = dto.creator
        self._created = dto.created
        self._modified = datetime.now()
