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
# Modified   : Saturday December 31st 2022 07:30:54 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""DataFrame Entity Module"""
from abc import abstractmethod
from datetime import datetime
import pandas as pd
from typing import Union, Dict

from recsys.core.entity.base import Entity
from recsys.core.dal.dto import DataFrameDTO, DatasetDTO


# ------------------------------------------------------------------------------------------------ #
#                                    DATASET COMPONENT                                             #
# ------------------------------------------------------------------------------------------------ #
class DataComponent(Entity):
    """Base component class from which DataFrame (Leaf) and Dataset (Composite) objects derive."""

    def __init__(self, name: str, description: str = None, mode: str = None) -> None:
        super().__init__(name=name, description=description, mode=mode)

    # -------------------------------------------------------------------------------------------- #
    @property
    @abstractmethod
    def datasource(self) -> str:
        """Datasource from which the Dataset Component has derived."""

    # -------------------------------------------------------------------------------------------- #
    @property
    @abstractmethod
    def stage(self) -> str:
        """Data processing stage in which the Dataset Component is created."""

    # -------------------------------------------------------------------------------------------- #
    @property
    @abstractmethod
    def is_composite(self) -> str:
        """True for Datasets and False for DataFrames."""

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    def as_dto(self) -> Union[DataFrameDTO, Dict[int, DataFrameDTO]]:
        """Creates a dto representation of the Dataset Component."""
    # -------------------------------------------------------------------------------------------- #
    def _validate(self) -> None:
        super()._validate()


# ------------------------------------------------------------------------------------------------ #
#                                        DATASETS                                                  #
# ------------------------------------------------------------------------------------------------ #
class Dataset(DataComponent):
    """Composite collection of DataFrame objects.

    Args:
        name (str): Short, yet descriptive lowercase name for Dataset object.
        description (str): Describes the Dataset object.
        datasource (str): The data datasource.
        stage (str): The stage of the data processing lifecycle to which the Dataset belongs.
        task_id (Task): The Id for the Task that created the Dataset. Optional. Defaults to 0.
        data (pd.DataFrame): Data payload. If provided, a DataFrame object is automatically
            created and added to the Dataset.
        mode (str): Mode for which the DataFrame is created. If None, defaults to mode from environment
            variable.
    """

    def __init__(
        self,
        name: str,
        datasource: str,
        stage: str,
        description: str = None,
        mode: str = None,
        task_id: int = 0,
        data: pd.DataFrame = None,
    ) -> None:
        super().__init__(name=name, description=description, mode=mode)
        self._datasource = datasource
        self._stage = stage
        self._task_id = task_id

        self._dataframes = {}
        self._is_composite = True

        self._validate()

        if data is not None:
            dataframe = self.create_dataframe(data)
            self.add_dataframe(dataframe)

    def __str__(self) -> str:
        return f"Dataset Id: {self._id}\n\tData source: {self._datasource}\n\tName: {self._name}\n\tDescription: {self._description}\n\tMode: {self._mode}\n\tStage: {self._stage}\n\tDataFrames: {self.dataframe_count}\n\tCreated: {self._created}\n\tModified: {self._modified}"

    def __repr__(self) -> str:
        return f"{self._id}, {self._datasource}, {self._name}, {self._description}, {self._mode}, {self._stage}, {self.dataframe_count}, {self._created}, {self._modified}"

    def __eq__(self, other: DataComponent) -> bool:
        if self.__class__.__name__ == other.__class__.__name__:
            return (self.is_composite == other.is_composite
                    and self.name == other.name
                    and self.description == other.description
                    and self.datasource == other.datasource
                    and self.mode == other.mode
                    and self.stage == other.stage
                    and self.task_id == other.task_id)
        else:
            return False

    @property
    def dataframe_count(self) -> int:
        return len(self._dataframes)

    # -------------------------------------------------------------------------------------------- #
    @property
    def is_composite(self) -> str:
        return self._is_composite

    # -------------------------------------------------------------------------------------------- #
    @property
    def datasource(self) -> str:
        """Datasource from which the Dataset Component has derived."""
        return self._datasource

    # -------------------------------------------------------------------------------------------- #
    @property
    def stage(self) -> str:
        """Data processing stage in which the Dataset Component is created."""
        return self._stage

    # -------------------------------------------------------------------------------------------- #
    @property
    def dataframe_names(self) -> list:
        return list(self._dataframes.keys())

    # -------------------------------------------------------------------------------------------- #
    @property
    def task_id(self) -> str:
        """Id for the Task that created the Dataset."""
        return self._task_id

    @task_id.setter
    def task_id(self, task_id: int) -> None:
        self._task_id = task_id
        self._modified = datetime.now()

    # -------------------------------------------------------------------------------------------- #
    def create_dataframe(self, data: pd.DataFrame, name: str = None, description: str = None) -> DataComponent:
        name = name or self._name
        description = description or self._description
        return DataFrame(name=name, data=data, parent=self, description=description)

    # -------------------------------------------------------------------------------------------- #
    def add_dataframe(self, dataframe: DataComponent) -> None:
        dataframe.parent = self
        self._dataframes[dataframe.name] = dataframe
        self._modified = datetime.now()

    # -------------------------------------------------------------------------------------------- #
    def get_dataframe(self, name: str = None) -> None:
        try:
            if name is None:
                name = self._name
            return self._dataframes[name]
        except KeyError:
            msg = f"Dataset {self._name} has no dataframe with name = {name}."
            self._logger.error(msg)
            raise FileNotFoundError(msg)

    # -------------------------------------------------------------------------------------------- #
    def remove_dataframe(self, name: str) -> None:
        del self._dataframes[name]
        self._modified = datetime.now()

    # -------------------------------------------------------------------------------------------- #
    def as_dto(self) -> DatasetDTO:

        dto = DatasetDTO(
            id=self._id,
            oid=self._oid,
            name=self._name,
            description=self._description,
            datasource=self._datasource,
            mode=self._mode,
            stage=self._stage,
            task_id=self._task_id,
            created=self._created,
            modified=self._modified,
        )
        return dto


# ------------------------------------------------------------------------------------------------ #
#                                        DATASET                                                   #
# ------------------------------------------------------------------------------------------------ #
class DataFrame(DataComponent):
    """DataFrame encapsulates tabular data, metadata, and access behaviors for data used in this package.

    Args:
        name (str): Short, yet descriptive lowercase name for DataFrame object.
        description (str): Describes the DataFrame object. Default's to parent's description if None.
        data (pd.DataFrame): Payload in pandas DataFrame format.
        parent (Dataset): The parent Dataset instance. Optional.
        mode (str): Mode for which the DataFrame is created. If None, defaults to mode from environment
            variable.

    """

    def __init__(
        self,
        name: str,
        data: pd.DataFrame,
        parent: Dataset,
        description: str = None,
        mode: str = None,
    ) -> None:
        super().__init__(name=name, description=description, mode=mode)

        self._data = data
        self._parent = parent
        self._is_composite = False

        # Inherited from parent and assigned in set_metadata if and when parent is set.
        self._datasource = None
        self._stage = None
        self._mode = None

        self._size = None
        self._nrows = None
        self._ncols = None
        self._nulls = None
        self._pct_nulls = None

        self._set_metadata()
        self._validate()

    def __str__(self) -> str:
        return f"DataFrame Id: {self._id}\n\tData source: {self._datasource}\n\tName: {self._name}\n\tDescription: {self._description}\n\tMode: {self._mode}\n\tStage: {self._stage}\n\tSize: {self._size}\n\tRows: {self._nrows}\n\tColumns: {self._ncols}\n\tNulls: {self._nulls}\n\tPct Nulls: {self._pct_nulls}\n\tCreated: {self._created}\n\tModified: {self._modified}"

    def __repr__(self) -> str:
        return f"{self._id}, {self._datasource}, {self._name}, {self._description}, {self._mode}, {self._stage}, {self._size}, {self._nrows}, {self._ncols}, {self._nulls}, {self._pct_nulls}, {self._created}, {self._modified}"

    def __eq__(self, other) -> bool:
        """Compares two Dataset for equality.
        Dataset are considered equal solely if their underlying data are equal.

        Args:
            other (DataFrame): The DataFrame object to compare.
        """

        if isinstance(other, DataFrame):
            return self._data.equals(other.data)

    def __len__(self) -> int:
        return 0

    # -------------------------------------------------------------------------------------------- #
    @property
    def is_composite(self) -> str:
        return self._is_composite

    # -------------------------------------------------------------------------------------------- #
    @property
    def datasource(self) -> str:
        """Datasource from which the Dataset Component has derived."""
        return self._datasource

    # -------------------------------------------------------------------------------------------- #
    @property
    def stage(self) -> str:
        """Data processing stage in which the Dataset Component is created."""
        return self._stage

    # -------------------------------------------------------------------------------------------- #
    @property
    def data(self) -> pd.DataFrame:
        return self._data

    # -------------------------------------------------------------------------------------------- #
    @property
    def parent(self) -> Dataset:
        return self._parent

    @parent.setter
    def parent(self, dataset: Dataset) -> None:
        self._parent = dataset
        self._modified = datetime.now()

    # -------------------------------------------------------------------------------------------- #
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
        self._data.info(verbose=True, memory_usage=True, show_counts=True)

    def head(self, n: int = 5) -> pd.DataFrame:
        return self._data.head(n)

    def tail(self, n: int = 5) -> pd.DataFrame:
        return self._data.tail(n)

    # ------------------------------------------------------------------------------------------------ #
    def as_dto(self) -> DataFrameDTO:
        return DataFrameDTO(
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
            parent_id=self._parent.id,
            created=self._created,
            modified=self._modified,
        )

    # ------------------------------------------------------------------------------------------------ #
    def _set_metadata(self) -> None:
        self._set_parent_metadata()
        self._set_data_metadata()

    def _set_parent_metadata(self) -> None:
        if self._parent is not None:
            self._description = self._description or self._parent.description
            self._datasource = self._parent.datasource
            self._stage = self._parent.stage
            self._mode = self._parent.mode

    def _set_data_metadata(self) -> None:
        if self._data is not None:
            if isinstance(self._data, pd.DataFrame):
                self._size = self._data.memory_usage(deep=True).sum()
                self._nrows = self._data.shape[0]
                self._ncols = self._data.shape[1]
                self._nulls = self._data.isnull().sum().sum()
                self._pct_nulls = (self._nulls / (self._nrows * self._ncols)) * 100
