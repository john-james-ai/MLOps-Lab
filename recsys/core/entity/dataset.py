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
# Modified   : Saturday December 24th 2022 02:31:46 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Dataset Entity Module"""
import os
import dotenv
from datetime import datetime
import pandas as pd

from .base import Entity
from recsys.core.dal.dto import DatasetDTO


# ------------------------------------------------------------------------------------------------ #
#                                        DATASET                                                   #
# ------------------------------------------------------------------------------------------------ #
class Dataset(Entity):
    """Dataset encapsulates tabular data, metadata, and access behaviors for data used in this package.

    Args:
        name (str): Short, yet descriptive lowercase name for Dataset object.
        description (str): Describes the Dataset object.
        datasource (str): The data datasource.
        mode (str): One of the registered modes, ie. 'input', 'prod', 'dev', or 'test'.
        stage (str): The stage of the data processing lifecycle to which the Dataset belongs.
        data (pd.DataFrame): Payload in pandas DataFrame format.
        task_id (int): The identifier for the Task that is creating the Dataset.
        parent_id (int): The identifier for the Dataset to which this Dataset belongs if any. Default = 0

    """

    def __init__(
        self,
        name: str,
        datasource: str,
        mode: str,
        stage: str,
        data: pd.DataFrame = None,
        description: str = None,
        task_id: int = None,
        parent_id: int = 0,
    ) -> None:
        super().__init__(name=name, mode=mode, description=description)
        self._datasource = datasource
        self._stage = stage
        self._data = data
        self._task_id = task_id
        self._parent_id = parent_id

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
        return f"\n\nId: {self._id}\n\tOid: {self._oid}\n\tName: {self._name}\n\tDescription: {self._description}\n\tDatasource: {self._datasource}\n\tMode: {self._mode}\n\tStage: {self._stage}\n\tSize: {self._size}\n\tNrows: {self._nrows}\n\tNcols: {self._ncols}\n\tNulls: {self._nulls}\n\tPct_Nulls: {self._pct_nulls}\n\tTask_Id: {self._task_id}\n\tParent_Id: {self._parent_id}\n\tCreated: {self._created}\n\tModified: {self._modified}"

    def __repr__(self) -> str:
        return f"{self._id}, {self._oid}, {self._name}, {self._description}, {self._datasource}, {self._mode}, {self._stage}, {self._size}, {self._nrows}, {self._ncols}, {self._nulls}, {self._pct_nulls}, {self._task_id}, {self._parent_id}, {self._created}, {self._modified}"

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

    # ------------------------------------------------------------------------------------------------ #
    @property
    def parent_id(self) -> int:
        return self._parent_id

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
            oid=self._oid,
            name=self._name,
            description=self._description,
            datasource=self._datasource,
            mode=self._mode,
            stage=self._stage,
            size=self._size,
            nrows=self._nrows,
            ncols=self._ncols,
            nulls=self._nulls,
            pct_nulls=self._pct_nulls,
            task_id=self._task_id,
            parent_id=self._parent_id,
            created=self._created,
            modified=self._modified,
        )

    # ------------------------------------------------------------------------------------------------ #
    def _set_metadata(self) -> None:

        dotenv.load_dotenv()
        self._mode = self._mode or os.getenv("MODE")
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
