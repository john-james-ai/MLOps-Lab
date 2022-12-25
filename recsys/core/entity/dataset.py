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
# Modified   : Sunday December 25th 2022 12:38:58 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Dataset Entity Module"""
from abc import abstractmethod
import os
import dotenv
from datetime import datetime
import pandas as pd
from typing import Union, Dict

from .base import Entity
from recsys.core.dal.dto import DatasetDTO, DatasetsDTO


# ------------------------------------------------------------------------------------------------ #
#                                    DATASET COMPONENT                                             #
# ------------------------------------------------------------------------------------------------ #
class DatasetComponent(Entity):
    """Base component class from which Dataset (Leaf) and Datasets (Composite) objects derive.
    Args:
        name (str): Short, yet descriptive lowercase name for Dataset object.
        description (str): Describes the Dataset object.
        datasource (str): The data datasource.
        mode (str): One of the registered modes, ie. 'input', 'prod', 'dev', or 'test'.
        stage (str): The stage of the data processing lifecycle to which the Dataset belongs.
        task_id (int): The identifier for the Task that is creating the Dataset.
        parent_id (int): The identifier for the Dataset to which this Dataset belongs if any. Default = 0"""

    def __init__(
        self,
        name: str,
        datasource: str,
        mode: str,
        stage: str,
        description: str = None,
        task_id: int = None,
        parent: Entity = None,
    ) -> None:
        super().__init__(name=name, mode=mode, description=description)
        self._id = None
        self._datasource = datasource
        self._stage = stage
        self._task_id = task_id
        self._parent = parent
        self._is_composite = False

    # -------------------------------------------------------------------------------------------- #
    @property
    def task_id(self) -> int:
        return self._task_id

    @task_id.setter
    def task_id(self, task_id: int) -> None:
        if self._task_id is None:
            self._task_id = task_id
            self._validate()
            self._modified = datetime.now()
        elif not self._task_id == task_id:
            msg = "Item reassignment is not supported for the 'task_id' member."
            self._logger.error(msg)
            raise TypeError(msg)

    # -------------------------------------------------------------------------------------------- #
    @property
    def parent(self) -> Entity:
        return self._parent

    @parent.setter
    def parent(self, parent: Entity) -> None:
        if self._parent is None:
            self._parent = parent
        else:
            msg = "Item reassignment is not supported for the 'parent' member."
            self._logger.error(msg)
            raise TypeError(msg)

    # -------------------------------------------------------------------------------------------- #
    @property
    def datasource(self) -> str:
        return self._datasource

    # -------------------------------------------------------------------------------------------- #
    @property
    def stage(self) -> str:
        return self._stage

    # -------------------------------------------------------------------------------------------- #
    @property
    def is_composite(self) -> str:
        return self._is_composite

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    def as_dto(self) -> Union[DatasetDTO, Dict[int, DatasetDTO]]:
        """Creates a dto or a dictionary of dtos for child objects."""
    # -------------------------------------------------------------------------------------------- #
    def _set_metadata(self) -> None:

        dotenv.load_dotenv()
        self._mode = self._mode or os.getenv("MODE")
        self._description = self._description or f"{self.__class__.__name__}.{self._name}"

    # ------------------------------------------------------------------------------------------------ #
    def _validate(self) -> None:
        super()._validate()
        if self._task_id is not None:
            if not isinstance(self._task_id, int):
                msg = f"Task_id must be an integer, not {type(self._task_id)}"
                self._logger.error(msg)
                raise TypeError(msg)


# ------------------------------------------------------------------------------------------------ #
#                                        DATASETS                                                  #
# ------------------------------------------------------------------------------------------------ #
class Datasets(DatasetComponent):
    """Composite collection of Dataset or Datasets objects.

    Args:
        name (str): Short, yet descriptive lowercase name for Dataset object.
        description (str): Describes the Dataset object.
        datasource (str): The data datasource.
        mode (str): One of the registered modes, ie. 'input', 'prod', 'dev', or 'test'.
        stage (str): The stage of the data processing lifecycle to which the Dataset belongs.
        task_id (int): The identifier for the Task that is creating the Dataset.
        parent_id (int): The identifier for the Dataset to which this Dataset belongs if any. Default = 0"""

    def __init__(
        self,
        name: str,
        datasource: str,
        mode: str,
        stage: str,
        description: str = None,
        task_id: int = None,
        parent: DatasetComponent = None,
    ) -> None:
        super().__init__(name=name, datasource=datasource, mode=mode, stage=stage, description=description, task_id=task_id, parent=parent)

        self._children = []
        self._is_composite = True

        self._set_metadata()

    def __eq__(self, other: DatasetComponent) -> bool:
        if self.__class__.__name__ == other.__class__.__name__:
            return (self.is_composite == other.is_composite
                    and self.name == other.name
                    and self.description == other.description
                    and self.datasource == other.datasource
                    and self.mode == other.mode
                    and self.stage == other.stage
                    and self.task_id == other.task_id
                    and self.parent == other.parent)
        else:
            return False

    def __len__(self) -> int:
        return len(self._children)

    # -------------------------------------------------------------------------------------------- #
    def add(self, component: DatasetComponent) -> None:
        component.parent = self
        self._children.append(component)

    # -------------------------------------------------------------------------------------------- #
    def remove(self, component: DatasetComponent) -> None:
        self._children.remove(component)

    # -------------------------------------------------------------------------------------------- #
    def as_dto(self) -> dict:
        dtos = []

        dto = DatasetsDTO(
            id=self._id,
            oid=self._oid,
            name=self._name,
            description=self._description,
            datasource=self._datasource,
            mode=self._mode,
            stage=self._stage,
            task_id=self._task_id,
            parent_id=self._parent.id if self._parent else None,
            created=self._created,
            modified=self._modified,
        )
        dtos.append(dto)
        for component in self._children:
            dtos.append(component.as_dto())

        return dtos


# ------------------------------------------------------------------------------------------------ #
#                                        DATASET                                                   #
# ------------------------------------------------------------------------------------------------ #
class Dataset(DatasetComponent):
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
        parent: DatasetComponent = None,
    ) -> None:
        super().__init__(name=name, datasource=datasource, mode=mode, stage=stage, description=description, task_id=task_id, parent=parent)

        self._is_composite = False

        self._data = data

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

    def __eq__(self, other) -> bool:
        """Compares two Datasets for equality.
        Datasets are considered equal solely if their underlying data are equal.

        Args:
            other (Dataset): The Dataset object to compare.
        """

        if isinstance(other, Dataset):
            return self._data.equals(other.data)

    def __len__(self) -> int:
        return 0

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
            parent_id=self._parent.id if self._parent else None,
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
            if not isinstance(self._task_id, int):  # pragma: no cover
                msg = f"Task_id must be an integer, not {type(self._task_id)}"
                self._logger.error(msg)
                raise TypeError(msg)
