#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/entity/dataset.py                                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 07:32:54 pm                                                #
# Modified   : Tuesday December 13th 2022 07:37:37 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Dataset Entity Module"""
import os
import dotenv
from dataclasses import dataclass
from datetime import datetime
import pandas as pd

from recsys.core.dal.dto import DatasetDTO
from .base import Entity, DTO

# ------------------------------------------------------------------------------------------------ #


@dataclass
class DatasetSpec:
    name: str
    description: str
    workspace: str
    stage: str


class Dataset(Entity):
    """Dataset encapsulates tabular data, metadata, and access behaviors for data used in this package.

    Args:
        id (int): Unique integer identifier for the Dataset object.
        name (str): Short, yet descriptive lowercase name for Dataset object.
        description (str): Describes the Dataset object.
        datasource (str): The data datasource
        workspace (str): One of ['prod', 'dev', 'test']
        stage (str): The stage of the data processing lifecycle to which the Dataset belongs.
        fileset_id (int): The id for the associated Fileset object.
        data (pd.DataFrame): A pandas DataFrame containing the data.
        uri (str): The location for persistence
        task_id (int): The step within a pipeline task that produced the Dataset object.
        *created (datetime): Datetime the Dataset was created
        **modified (datetime): Datetime the Dataset was modified.

        * Managed by base class
        ** Instantiated by base class


    """

    def __init__(
        self,
        name: str,
        datasource: str,
        stage: str,
        fileset_id: int = None,
        task_id: int = None,
        data: pd.DataFrame = None,
        workspace: str = None,
        description: str = None,
    ) -> None:
        super().__init__(name=name, description=description)

        self._datasource = datasource
        self._workspace = workspace
        self._stage = stage
        self._data = data
        self._task_id = task_id

        # Added by user
        self._fileset = None

        # Assigned by repo
        self._size = None
        self._nrows = None
        self._ncols = None
        self._nulls = None
        self._pct_nulls = None

        # Set metadata
        self._set_metadata()

        # Validate entity
        self._validate()

    def __str__(self) -> str:
        return f"\n\nDataset Id: {self._id}\n\tName: {self._name}\n\tDescription: {self._description}\n\tData Source: {self._datasource}\n\tWorkspace: {self._workspace}\n\tStage: {self._stage}\n\tFilepath: {self._uri}\n\tStep_Id: {self._task_id}\n\tCreated: {self._created}\n\tModified: {self._modified}"

    def __repr__(self) -> str:
        return f"{self._id},{self._name},{self._description},{self._datasource},{self._workspace},{self._stage},{self._uri},{self._task_id},{self._created},{self._modified}"

    def __eq__(self, other) -> bool:
        """Compares two Datasets for equality.
        Datasets are considered equal solely if their underlying data are equal.

        Args:
            other (Dataset): The Dataset object to compare.
        """

        if isinstance(other, Dataset):
            return self._data.equals(other.data)

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
    def datasource(self) -> str:
        return self._datasource

    @property
    def stage(self) -> str:
        return self._stage

    @property
    def data(self) -> pd.DataFrame:
        return self._data

    @data.setter
    def data(self, data: pd.DataFrame) -> None:
        if self._data is None:
            self._data = data
            self._update_and_validate()
        else:
            msg = (
                f"The 'data'attribute on Dataset {self._id} does not support item re-assignment."
            )
            self._logger.error(msg)
            raise TypeError(msg)

    @property
    def fileset(self) -> str:
        return self._fileset

    @fileset.setter
    def fileset(self, fileset: int) -> None:
        if self._fileset is None:
            self._fileset = fileset
            self._update_and_validate()
        else:
            msg = (
                f"The 'fileset' attribute on Dataset {self._id} does not support item re-assignment."
            )
            self._logger.error(msg)
            raise TypeError(msg)

    @property
    def size(self) -> int:
        return self._size

    @property
    def nrows(self) -> int:
        return self._nrows

    @property
    def ncols(self) -> int:
        return self._ncols

    @property
    def nulls(self) -> int:
        return self._nulls

    @property
    def pct_nulls(self) -> int:
        return self._pct_nulls
    # ------------------------------------------------------------------------------------------------ #
    # Data Access methods

    def info(self) -> None:
        try:
            self._data.info(verbose=True, memory_usage=True, show_counts=True)
        except AttributeError:
            msg = "The data member is None or not a valid pandas DataFrame object."
            self._logger.warning(msg)

    def head(self, n: int = 5) -> pd.DataFrame:
        try:
            return self._data.head(n)
        except AttributeError:
            msg = "The data member is None or not a valid pandas DataFrame object."
            self._logger.warning(msg)

    def tail(self, n: int = 5) -> pd.DataFrame:
        try:
            return self._data.tail(n)
        except AttributeError:
            msg = "The data member is None or not a valid pandas DataFrame object."
            self._logger.warning(msg)

    # ------------------------------------------------------------------------------------------------ #
    def as_dto(self) -> DatasetDTO:
        return DatasetDTO(
            id=self._id,
            name=self._name,
            description=self._description,
            datasource=self._datasource,
            workspace=self._workspace,
            stage=self._stage,
            task_id=self._task_id,
            size=self._size,
            nrows=self._nrows,
            ncols=self._ncols,
            nulls=self._nulls,
            pct_nulls=self._pct_nulls,
            created=self._created,
            modified=self._modified,
        )

    # ------------------------------------------------------------------------------------------------ #
    def _from_dto(self, dto: DTO) -> None:
        super().__init__(name=dto.name, description=dto.description)
        self._id = dto.id
        self._datasource = dto.datasource
        self._workspace = dto.workspace
        self._stage = dto.stage
        self._task_id = dto.task_id
        self._size = dto.size
        self._nrows = dto.nrows
        self._ncols = dto.ncols
        self._nulls = dto.nulls
        self._pct_nulls = dto.pct_nulls
        self._created = dto.created
        self._modified = dto.modified
        self._data = None
        self._validate()

    # ------------------------------------------------------------------------------------------------ #
    def _set_metadata(self) -> None:

        dotenv.load_dotenv()
        self._workspace = self._workspace or os.getenv("WORKSPACE")
        self._description = self._description or f"{self.__class__.__name__}.{self._name}"

        if self._data is not None:
            if isinstance(self._data, pd.DataFrame):
                self._size = self._data.memory_usage(deep=True).sum()
                self._nrows = self._data.shape[0]
                self._ncols = self._data.shape[1]
                self._nulls = self._data.isnull().sum().sum()
                self._pct_nulls = (self._nulls / (self._nrows * self._ncols)) * 100

    def _update_and_validate(self) -> None:
        self._modified = datetime.now()
        self._validate()

    # ------------------------------------------------------------------------------------------------ #
    def _validate(self) -> None:
        super()._validate()
        if self._data is not None:
            if not isinstance(self._data, pd.DataFrame):
                msg = f"The data member must be a pandas Dataframe, not {type(self._data)}."
                self._logger.error(msg)
                raise TypeError(msg)
        if self._task_id is not None:
            if not isinstance(self._task_id, int):
                msg = f"Task_id must be an integer, not {type(self._task_id)}"
                self._logger.error(msg)
                raise TypeError(msg)
