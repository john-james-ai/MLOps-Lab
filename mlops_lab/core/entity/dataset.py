#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /mlops_lab/core/entity/dataset.py                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 07:32:54 pm                                                #
# Modified   : Tuesday January 24th 2023 08:13:45 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""DataFrame Entity Module"""
from abc import abstractmethod
from datetime import datetime
import pandas as pd
from typing import Union, Dict

from mlops_lab.core.entity.base import Entity
from mlops_lab.core.dal.dto import DataFrameDTO, DatasetDTO

# ------------------------------------------------------------------------------------------------ #
#                                         STAGES                                                   #
# ------------------------------------------------------------------------------------------------ #
STAGES = ["extract", "raw", "split", "interim", "final"]


# ------------------------------------------------------------------------------------------------ #
#                                    DATASET COMPONENT                                             #
# ------------------------------------------------------------------------------------------------ #
class DataComponent(Entity):
    """Base component class from which DataFrame (Leaf) and Dataset (Composite) objects derive."""

    def __init__(self, name: str, description: str = None) -> None:
        super().__init__(name=name, description=description)

    # -------------------------------------------------------------------------------------------- #
    @property
    @abstractmethod
    def is_composite(self) -> str:
        """True for Datasets and False for DataFrames."""

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    def as_dto(self) -> Union[DataFrameDTO, Dict[int, DataFrameDTO]]:
        """Creates a dto representation of the Dataset Component."""


# ------------------------------------------------------------------------------------------------ #
#                                        DATASETS                                                  #
# ------------------------------------------------------------------------------------------------ #
class Dataset(DataComponent):
    """Composite collection of DataFrame objects.

    Args:
        name (str): Short, yet descriptive lowercase name for Dataset object.
        description (str): Describes the Dataset object.
        datasource_oid (str): The object id for the datasource.
        stage (str): The stage of the data processing lifecycle to which the Dataset belongs.
        task_oid (str): The object id for the Task that created the Dataset. Optional. Defaults to 0.
        data (pd.DataFrame): Data payload. If provided, a DataFrame object is automatically
            generated and added to the Dataset.
    """

    def __init__(
        self,
        name: str,
        stage: str,
        description: str = None,
        datasource_oid: str = None,
        task_oid: str = None,
        data: pd.DataFrame = None,
    ) -> None:
        super().__init__(name=name, description=description)
        self._datasource_oid = datasource_oid
        self._stage = stage
        self._task_oid = task_oid

        self._dataframes = {}
        self._is_composite = True

        self._validate()

        if data is not None:
            # Generate and add DataFrame
            self._spawn_dataframe(data)

    def __len__(self) -> int:
        return len(self._dataframes)

    def __str__(self) -> str:
        return f"Dataset Id: {self._id}\n\tData source: {self._datasource_oid}\n\tName: {self._name}\n\tDescription: {self._description}\n\tStage: {self._stage}\n\tDataFrames: {self.dataframe_count}\n\tCreated: {self._created}\n\tModified: {self._modified}"

    def __repr__(self) -> str:
        return f"{self._id}, {self._datasource_oid}, {self._name}, {self._description}, {self._stage}, {self.dataframe_count}, {self._created}, {self._modified}"

    def __eq__(self, other: DataComponent) -> bool:
        if self.__class__.__name__ == other.__class__.__name__:
            return (
                self.is_composite == other.is_composite
                and self.name == other.name
                and self.description == other.description
                and self.datasource_oid == other.datasource_oid
                and self.stage == other.stage
                and self.task_oid == other.task_oid
            )
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
    def datasource_oid(self) -> str:
        """Datasource from which the Dataset Component has derived."""
        return self._datasource_oid

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
    def task_oid(self) -> str:
        """Id for the Task that created the Dataset."""
        return self._task_oid

    @task_oid.setter
    def task_oid(self, task_oid: str) -> None:
        self._task_oid = task_oid
        self._modified = datetime.now()

    # -------------------------------------------------------------------------------------------- #
    def add_dataframe(self, dataframe: DataComponent) -> None:
        dataframe.dataset = self
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
            datasource_oid=self._datasource_oid,
            stage=self._stage,
            task_oid=self._task_oid,
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
        dataframe = DataFrame(
            name=self._name,
            data=data,
            dataset=self,
            description=self._description,
        )
        self.add_dataframe(dataframe)


# ------------------------------------------------------------------------------------------------ #
#                                        DATAFRAME                                                 #
# ------------------------------------------------------------------------------------------------ #
class DataFrame(DataComponent):
    """DataFrame encapsulates tabular data, metadata, and access behaviors for data used in this package.

    Args:
        name (str): Short, yet descriptive lowercase name for DataFrame object.
        description (str): Describes the DataFrame object. Default's to dataset's description if None.
        data (pd.DataFrame): Payload in pandas DataFrame format.
        stage (str): The data processing stage for which the Dataframe is created.
        dataset (Dataset): The dataset Dataset object in which this DataFrame is composed.

    """

    def __init__(
        self,
        name: str,
        description: str = None,
        data: pd.DataFrame = None,
        dataset: Dataset = None,
    ) -> None:
        super().__init__(name=name, description=description)

        self._data = data
        self._is_composite = False
        self._dataset = dataset

        self._size = None
        self._nrows = None
        self._ncols = None
        self._nulls = None
        self._pct_nulls = None

        self._set_metadata()

    def __str__(self) -> str:
        return f"DataFrame Id: {self._id}\n\tName: {self._name}\n\tDescription: {self._description}\n\tStage: {self._stage}\n\tSize: {self._size}\n\tRows: {self._nrows}\n\tColumns: {self._ncols}\n\tNulls: {self._nulls}\n\tPct Nulls: {self._pct_nulls}\n\tCreated: {self._created}\n\tModified: {self._modified}"

    def __repr__(self) -> str:
        return f"{self._id}, {self._name}, {self._description}, {self._stage}, {self._size}, {self._nrows}, {self._ncols}, {self._nulls}, {self._pct_nulls}, {self._created}, {self._modified}"

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
    def data(self) -> pd.DataFrame:
        return self._data

    # -------------------------------------------------------------------------------------------- #
    @property
    def dataset(self) -> Dataset:
        return self._dataset

    # -------------------------------------------------------------------------------------------- #
    @dataset.setter
    def dataset(self, dataset: Dataset) -> None:
        self._datase = dataset

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
            size=self._size,
            nrows=self._nrows,
            ncols=self._ncols,
            nulls=self._nulls,
            pct_nulls=self._pct_nulls,
            dataset_oid=self._dataset.oid,
            created=self._created,
            modified=self._modified,
        )

    # ------------------------------------------------------------------------------------------------ #
    def _set_metadata(self) -> None:
        self._set_dataset_metadata()
        self._set_data_metadata()

    def _set_dataset_metadata(self) -> None:
        if self._dataset is not None:
            self._name = self._name or self._dataset.name
            self._description = self._description or self._dataset.description
            self._stage = self._stage or self._dataset.stage

    def _set_data_metadata(self) -> None:
        if self._data is not None:
            if isinstance(self._data, pd.DataFrame):
                self._size = self._data.memory_usage(deep=True).sum()
                self._nrows = self._data.shape[0]
                self._ncols = self._data.shape[1]
                self._nulls = self._data.isnull().sum().sum()
                self._pct_nulls = (self._nulls / (self._nrows * self._ncols)) * 100
