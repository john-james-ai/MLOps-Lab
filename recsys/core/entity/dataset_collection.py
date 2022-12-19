#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/entity/dataset_collection.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 07:32:54 pm                                                #
# Modified   : Monday December 19th 2022 08:07:03 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""DatasetCollection Entity Module"""
import pandas as pd

from .dataset import Dataset
from recsys.core.dal.dto import DatasetCollectionDTO
from .base import Entity, DTO


# ------------------------------------------------------------------------------------------------ #
#                                        DATASET                                                   #
# ------------------------------------------------------------------------------------------------ #
class DatasetCollection(Entity):
    """DatasetCollection encapsulates two or more Datasets.

    Args:
        name (str): collection name
        description (str): collection description
        datasource (str): Source of data for collection.
        mode (str): mode in which the collection is created
        stage (str): Stage in which the collection was created
        task_id (int): Identifier for task that created the collection
    """

    def __init__(
        self,
        name: str,
        datasource: str,
        mode: str,
        stage: str,
        task_id: int = None,
        description: str = None,
    ) -> None:
        super().__init__(name=name, description=description)

        self._datasource = datasource
        self._mode = mode
        self._stage = stage
        self._task_id = task_id

        self._datasets = {}

        # Validate entity
        self._validate()

    def __str__(self) -> str:
        return f"\n\nId: {self._id}\n\tName: {self._name}\n\tDescription: {self._description}\n\tMode: {self._mode}\n\tStage: {self._stage}\n\tTask_Id: {self._task_id}\n\tCreated: {self._created}\n\tModified: {self._modified}\n\t"

    def __repr__(self) -> str:
        return f"{self._id},{self._name},{self._description},{self._mode},{self._stage},{self._task_id},{self._created},{self._modified}"

    def __len__(self) -> int:
        """Returns the number of Datasets in the DatasetCollection."""
        return len(self._datasets)

    def __eq__(self, other) -> bool:
        """Compares two DatasetCollections for equality.
        DatasetCollections are considered equal solely if their underlying data are equal.

        Args:
            other (DatasetCollection): The DatasetCollection object to compare.
        """

        if isinstance(other, DatasetCollection):
            return (self._name == other.name
                    and self._datasource == other.datasource
                    and self._mode == other.mode
                    and self._stage == other.stage
                    and self._datasets == other.datasets
                    and self._task_id == other.task_id)
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
    def mode(self) -> str:
        return self._mode

    @property
    def stage(self) -> str:
        return self._stage

    @property
    def datasets(self) -> str:
        return self._datasets

    # ------------------------------------------------------------------------------------------------ #
    def add(self, dataset: Dataset) -> None:
        """Adds a Dataset to the collection."""
        self._datasets[dataset.id] = dataset

    def remove(self, id: int) -> None:
        """Removes a Dataset from the collection."""
        del self._datasets[id]

    def print(self) -> None:
        df = pd.DataFrame.from_dict(data=self._datasets, orient='index')
        print(df)

    # ------------------------------------------------------------------------------------------------ #
    def as_dto(self) -> DatasetCollectionDTO:
        return DatasetCollectionDTO(
            id=self._id,
            name=self._name,
            description=self._description,
            datasource=self._datasource,
            mode=self._mode,
            stage=self._stage,
            task_id=self._task_id,
            created=self._created,
            modified=self._modified,
        )

    # ------------------------------------------------------------------------------------------------ #
    def _from_dto(self, dto: DTO) -> None:
        super().__init__(name=dto.name, description=dto.description)
        self._id = dto.id
        self._name = dto.name
        self._description = dto.description
        self._datasource = dto.datasource
        self._mode = dto.mode
        self._stage = dto.stage
        self._task_id = dto.task_id
        self._created = dto.created
        self._modified = dto.modified

        self._datasets = []
        self._validate()

    # ------------------------------------------------------------------------------------------------ #
    def _validate(self) -> None:
        super()._validate()
        if self._task_id is not None:
            if not isinstance(self._task_id, int):
                msg = f"task_id must be an integer, not {type(self._task_id)}"
                self._logger.error(msg)
                raise TypeError(msg)
