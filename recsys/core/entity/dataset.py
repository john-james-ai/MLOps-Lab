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
# Modified   : Monday January 9th 2023 11:21:38 pm                                                 #
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
        datasource_id (int): The id for the datasource.
        stage (str): The stage of the data processing lifecycle to which the Dataset belongs.
        task_id (Task): The Id for the Task that created the Dataset. Optional. Defaults to 0.
        data (pd.DataFrame): Data payload. If provided, a DataFrame object is automatically
            generated and added to the Dataset.
        mode (str): Mode for which the DataFrame is created. If None, defaults to mode from environment
            variable.
    """

    def __init__(
        self,
        name: str,
        datasource_id: int,
        stage: str,
        description: str = None,
        mode: str = None,
        task_id: int = 0,
        data: pd.DataFrame = None,
    ) -> None:
        super().__init__(name=name, description=description, mode=mode)
        self._datasource_id = datasource_id
        self._stage = stage
        self._task_id = task_id

        self._dataframes = {}
        self._is_composite = True

        self._validate()

        if data is not None:
            # Generate and add DataFrame
            self._spawn_dataframe(data)

    def __len__(self) -> int:
        return len(self._dataframes)

    def __str__(self) -> str:
        return f"Dataset Id: {self._id}\n\tData source: {self._datasource_id}\n\tName: {self._name}\n\tDescription: {self._description}\n\tMode: {self._mode}\n\tStage: {self._stage}\n\tDataFrames: {self.dataframe_count}\n\tCreated: {self._created}\n\tModified: {self._modified}"

    def __repr__(self) -> str:
        return f"{self._id}, {self._datasource_id}, {self._name}, {self._description}, {self._mode}, {self._stage}, {self.dataframe_count}, {self._created}, {self._modified}"

    def __eq__(self, other: DataComponent) -> bool:
        if self.__class__.__name__ == other.__class__.__name__:
            return (self.is_composite == other.is_composite
                    and self.name == other.name
                    and self.description == other.description
                    and self.datasource_id == other.datasource_id
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
    def datasource_id(self) -> str:
        """Datasource from which the Dataset Component has derived."""
        return self._datasource_id

    # -------------------------------------------------------------------------------------------- #
    @property
    def stage(self) -> str:
        """Data processing stage in which the Dataset Component is created."""
        return self._stage

    # -------------------------------------------------------------------------------------------- #
    @property
    def dataframes(self) -> list:
        return self._dataframes

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
    def add_dataframe(self, dataframe: DataComponent) -> None:
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
    def get_dataframes(self) -> pd.DataFrame:
        d = {}
        for name, dataframe in self._dataframes.items():
            d[name] = dataframe.as_dict()
        df = pd.DataFrame.from_dict(data=d, orient="index")
        return df

    # -------------------------------------------------------------------------------------------- #
    def update_dataframe(self, dataframe: DataComponent) -> None:
        dataframe.dataset = self
        self._dataframes[dataframe.name] = dataframe
        self._modified = datetime.now()

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
            datasource_id=self._datasource_id,
            mode=self._mode,
            stage=self._stage,
            task_id=self._task_id,
            created=self._created,
            modified=self._modified,
        )
        return dto

    # -------------------------------------------------------------------------------------------- #
    def _spawn_dataframe(self, data: pd.DataFrame) -> None:
        """Used when instantiating the Dataset with data to generate a pseudo clone DataFrame containing the data.

        The DataFrame is generated as a clone of the Dataset and added to the Dataset. It is identical
        to the Dataset, except for the data.
        """
        dataframe = DataFrame(name=self._name, data=data, parent=self, stage=self._stage,
                              description=self._description, mode=self._mode)
        self.add_dataframe(dataframe)


# ------------------------------------------------------------------------------------------------ #
#                                        DATASET                                                   #
# ------------------------------------------------------------------------------------------------ #
class DataFrame(DataComponent):
    """DataFrame encapsulates tabular data, metadata, and access behaviors for data used in this package.

    Args:
        name (str): Short, yet descriptive lowercase name for DataFrame object.
        description (str): Describes the DataFrame object. Default's to dataset's description if None.
        data (pd.DataFrame): Payload in pandas DataFrame format.
        stage (str): The data processing stage for which the Dataframe is created.
        mode (str): Mode for which the DataFrame is created. If None, defaults to mode from environment
            variable.
        parent (Dataset): The parent Dataset object in which this DataFrame is composed.

    """

    def __init__(
        self,
        name: str,
        data: pd.DataFrame,
        parent: Dataset,
        stage: str = None,
        description: str = None,
        mode: str = None,
    ) -> None:
        super().__init__(name=name, description=description, mode=mode)

        self._data = data
        self._parent = parent
        self._is_composite = False

        # Possibly inherited from dataset and assigned in set_metadata if and when dataset is set.
        self._stage = None

        self._size = None
        self._nrows = None
        self._ncols = None
        self._nulls = None
        self._pct_nulls = None

        self._set_metadata()
        self._validate()

    def __str__(self) -> str:
        return f"DataFrame Id: {self._id}\n\tName: {self._name}\n\tDescription: {self._description}\n\tMode: {self._mode}\n\tStage: {self._stage}\n\tSize: {self._size}\n\tRows: {self._nrows}\n\tColumns: {self._ncols}\n\tNulls: {self._nulls}\n\tPct Nulls: {self._pct_nulls}\n\tCreated: {self._created}\n\tModified: {self._modified}"

    def __repr__(self) -> str:
        return f"{self._id}, {self._name}, {self._description}, {self._mode}, {self._stage}, {self._size}, {self._nrows}, {self._ncols}, {self._nulls}, {self._pct_nulls}, {self._created}, {self._modified}"

    def __eq__(self, other) -> bool:
        """Compares two Dataset for equality.
        Dataset are considered equal solely if their underlying data are equal.

        Args:
            other (DataFrame): The DataFrame object to compare.
        """

        if isinstance(other, DataFrame):
            return self._data.equals(other.data)

    def __len__(self) -> int:
        return self._nrows

    # -------------------------------------------------------------------------------------------- #
    @property
    def is_composite(self) -> str:
        return self._is_composite

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
            stage=self._stage,
            mode=self._mode,
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
            self._name = self._name or self._parent.name
            self._description = self._description or self._parent.description
            self._stage = self._stage or self._parent.stage
            self._mode = self._mode or self._parent.mode

    def _set_data_metadata(self) -> None:
        if self._data is not None:
            if isinstance(self._data, pd.DataFrame):
                self._size = self._data.memory_usage(deep=True).sum()
                self._nrows = self._data.shape[0]
                self._ncols = self._data.shape[1]
                self._nulls = self._data.isnull().sum().sum()
                self._pct_nulls = (self._nulls / (self._nrows * self._ncols)) * 100
