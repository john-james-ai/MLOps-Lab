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
# Modified   : Friday December 9th 2022 06:49:33 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Dataset Entity Module"""
import os
import dotenv
from datetime import datetime
import pandas as pd

from recsys.core.dal.dto import DatasetDTO
from .base import Entity

# ------------------------------------------------------------------------------------------------ #


class Dataset(Entity):
    """Dataset encapsulates tabular data, metadata, and access behaviors for data used in this package.

    Args:
        *id (int): Unique integer identifier for the Dataset object.
        *name (str): Short, yet descriptive lowercase name for Dataset object.
        *description (str): Describes the Dataset object.
        *datasource (str): The data datasource
        workspace (str): One of ['prod', 'dev', 'test']
        stage (str): The stage of the data processing lifecycle to which the Dataset belongs.
        version (int): Version is initialized at 1 and bumped by the repo if the Dataset object exists.*
        data (pd.DataFrame): A pandas DataFrame containing the data.
        cost (int): Time in seconds required to produce the Dataset object.
        nrows (int): The number of rows in the Dataset
        ncols (int): The number of columns in the Dataset
        null_counts (int): Number of null values in the Dataset
        memory_size_mb (int): The number of megabytes of memory the DataFrame consumes.
        filepath (str): The location for persistence
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
        task_id: int,
        version: int = 1,
        data: pd.DataFrame = None,
        workspace: str = None,
        description: str = None,
    ) -> None:
        super().__init__(name=name, description=description)

        self. _datasource = datasource
        self._workspace = workspace
        self._stage = stage
        self._version = version
        self._data = data
        self._task_id = task_id

        # Assigned by repo
        self._filepath = None

        # Assigned by metadata method
        self._cost = None
        self._nrows = None
        self._ncols = None
        self._null_counts = None
        self._memory_size_mb = None

        # Set metadata
        self._set_metadata()

        # Validate entity
        self._validate()

    def __str__(self) -> str:
        return f"\n\nDataset Id: {self._id}\n\tName: {self._name}\n\tDescription: {self._description}\n\tData Source: {self. _datasource}\n\tWorkspace: {self._workspace}\n\tStage: {self._stage}\n\tVersion: {self._version}\n\tData: {self._data}\n\tCost: {self._cost}\n\tNrows: {self._nrows}\n\tNcols: {self._ncols}\n\tNull_Counts: {self._null_counts}\n\tMemory_Size_Mb: {self._memory_size_mb}\n\tFilepath: {self._filepath}\n\tStep_Id: {self._task_id}\n\tCreated: {self._created}\n\tModified: {self._modified}"

    def __repr__(self) -> str:
        return f"{self._id},{self._name},{self._description},{self. _datasource},{self._workspace},{self._stage},{self._version},{self._data},{self._cost},{self._nrows},{self._ncols},{self._null_counts},{self._memory_size_mb},{self._filepath},{self._task_id},{self._created},{self._modified}"

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
        return self. _datasource

    @property
    def stage(self) -> str:
        return self._stage

    @property
    def version(self) -> str:
        return self._version

    @version.setter
    def version(self, version: int) -> None:
        self._version = version
        self._update_and_validate()

    @property
    def data(self) -> pd.DataFrame:
        return self._data

    @data.setter
    def data(self, data: pd.DataFrame) -> None:
        self._logger.debug(self._data)
        if self._data is None:
            self._data = data
            self._set_data_metadata()
            self._update_and_validate()
        else:
            msg = (
                f"The 'data' attribute on  Dataset {self._id} does not support item re-assignment."
            )
            self._logger.error(msg)
            raise TypeError(msg)

    @property
    def cost(self) -> str:
        return self._cost

    @cost.setter
    def cost(self, cost: int) -> None:
        self._cost = cost
        self._update_and_validate()

    @property
    def nrows(self) -> int:
        return self._nrows

    @property
    def ncols(self) -> int:
        return self._ncols

    @property
    def null_counts(self) -> int:
        return self._null_counts

    @property
    def memory_size_mb(self) -> int:
        return self._memory_size_mb

    @property
    def filepath(self) -> str:
        return self._filepath

    @filepath.setter
    def filepath(self, filepath: int) -> None:
        self._filepath = filepath
        self._update_and_validate()

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
            datasource=self. _datasource,
            workspace=self._workspace,
            stage=self._stage,
            version=self._version,
            cost=self._cost,
            nrows=self._nrows,
            ncols=self._ncols,
            null_counts=self._null_counts,
            memory_size_mb=self._memory_size_mb,
            filepath=self._filepath,
            task_id=self._task_id,
            created=self._created,
            modified=self._modified,
        )

    # ------------------------------------------------------------------------------------------------ #
    def _from_dto(self, dto: DatasetDTO) -> None:
        super().__init__(name=dto.name, description=dto.description)
        self._id = dto.id
        self. _datasource = dto.datasource
        self._workspace = dto.workspace
        self._stage = dto.stage
        self._version = dto.version
        self._cost = dto.cost
        self._nrows = dto.nrows
        self._ncols = dto.ncols
        self._null_counts = dto.null_counts
        self._memory_size_mb = dto.memory_size_mb
        self._filepath = dto.filepath
        self._task_id = dto.task_id
        self._created = dto.created
        self._modified = dto.modified
        self._data = None  # DTO's don't carry the actual data. It is stored at filepath.

        self._validate()

    # ------------------------------------------------------------------------------------------------ #
    def _set_metadata(self) -> None:
        self._set_operational_metadata()
        self._set_data_metadata()

    def _set_operational_metadata(self) -> None:

        # Operational Metadata
        dotenv.load_dotenv()
        self._workspace = self._workspace or os.getenv("WORKSPACE")
        self._description = self._description or f"{self.__class__.__name__}.{self._name}"

    def _set_data_metadata(self) -> None:
        # Data Metadata
        if isinstance(self._data, pd.DataFrame):
            self._memory_size_mb = float(
                round(self._data.memory_usage(deep=True, index=True).sum() / 1024**2, 2)
            )
            self._nrows = int(self._data.shape[0])
            self._ncols = int(self._data.shape[1])
            self._null_counts = int(self._data.isnull().sum().sum())

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

        if not isinstance(self._version, int):
            msg = f"Version must be an integer, not {type(self._version)}"
            self._logger.error(msg)
            raise TypeError(msg)

        if not isinstance(self._task_id, int):
            msg = f"Task_id must be an integer, not {type(self._version)}"
            self._logger.error(msg)
            raise TypeError(msg)
