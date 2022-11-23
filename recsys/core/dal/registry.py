#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /registry.py                                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday November 22nd 2022 08:04:53 pm                                              #
# Modified   : Wednesday November 23rd 2022 08:24:52 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod
from typing import Any
import logging
import pandas as pd

from recsys.core import DATASET_FEATURES
from .dataset import Dataset
from .database import Database
from .sequel import (
    CreateDatasetRegistryTable,
    DropDatasetRegistryTable,
    DatasetExists,
    SelectDataset,
    CountDatasets,
    SelectAllDatasets,
    InsertDataset,
    DeleteDataset,
)

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class Registry(ABC):
    @abstractmethod
    def reset(self) -> None:
        """Drops the registry table and re-creatas it."""

    @abstractmethod
    def add(self, *args, **kwargs) -> None:
        """Adds a dataset to the registry. If a duplicate is found, the version is bumped"""

    @abstractmethod
    def get(self, id: int) -> Any:
        """Retrieves dataset metadata from the registry, given an id

        Args:
            id (int): The id for the Dataset to retrieve.
        """

    @abstractmethod
    def get_all(self) -> Any:
        """Returns a Dataframe representation of the registry."""

    @abstractmethod
    def exists(self, *args, **kwargs) -> bool:
        """Returns true if a dataset with the same name, env, stage and version exists in the registry."""

    @abstractmethod
    def remove(self, id: int) -> None:
        """Deletes a Dataset from the registry, given an id.

        Args:
            id (int): The id for the Dataset to remove.
        """


# ------------------------------------------------------------------------------------------------ #
class DatasetRegistry(Registry):
    def __init__(self, database: Database) -> None:
        self._database = database
        self._create_registry()

    def __len__(self) -> int:
        """Returns the number of items in the registry."""
        query = CountDatasets()
        with self._database as db:
            return db.count(sql=query.sql, args=query.args)

    def reset(self) -> None:
        """Drops the registry table and re-creatas it."""
        self._drop_registry()
        self._create_registry()

    def add(self, dataset: Dataset) -> Dataset:
        """Adds a dataset to the registry. If a duplicate is found, the version is bumped.

        Args:
            dataset (Dataset): The Dataset to add to the registry.
        """
        while self.exists(dataset):
            dataset.version = dataset.version + 1

        dad = dataset.as_dict()
        registry = {k: dad[k] for k in DATASET_FEATURES}
        insert = InsertDataset(**registry)
        with self._database as db:
            dataset.id = db.insert(insert.sql, insert.args)
        return dataset

    def get(self, id: int) -> dict:
        """Retrieves dataset metadata from the registry, given an id

        Args:
            id (int): The id for the Dataset to retrieve.
        """
        result = None
        select = SelectDataset(id=id)
        with self._database as db:
            result = db.select(select.sql, select.args)
        try:
            result = result[0]
            return self._results_to_dict(result)
        except IndexError as e:
            msg = f"Dataset id: {id} not found.\n{e}"
            logger.error(msg)
            raise FileNotFoundError(msg)

    def get_all(self) -> pd.DataFrame:
        """Returns a Dataframe representation of the registry."""
        datasets = []
        select = SelectAllDatasets()
        with self._database as db:
            results = db.select(sql=select.sql, args=select.args)
            if len(results) > 0:
                for result in results:
                    datasets.append(self._results_to_dict(result))
                datasets = pd.DataFrame.from_dict(datasets)
        return datasets

    def exists(self, dataset: Dataset) -> bool:
        """Returns true if a dataset with the same name, env, stage and version exists in the registry.

        Args:
            dataset (Dataset): The Dataset to add to the registry.
        """
        exists = DatasetExists(
            name=dataset.name, env=dataset.env, stage=dataset.stage, version=dataset.version
        )
        with self._database as db:
            return db.exists(sql=exists.sql, args=exists.args)

    def remove(self, id: int) -> None:
        """Deletes a Dataset from the registry, given an id.

        Args:
            id (int): The id for the Dataset to remove.
        """
        remove = DeleteDataset(id=id)
        with self._database as db:
            db.delete(sql=remove.sql, args=remove.args)

    def _create_registry(self) -> None:
        create = CreateDatasetRegistryTable()
        with self._database as db:
            logger.debug("\n\nCreating Registry Table")
            db.create(create.sql, create.args)

    def _drop_registry(self) -> None:
        drop = DropDatasetRegistryTable()
        with self._database as db:
            db.drop(drop.sql, drop.args)

    def _results_to_dict(self, result: tuple) -> dict:
        try:
            result_dict = {}
            result_dict["id"] = result[0]
            result_dict["name"] = result[1]
            result_dict["description"] = result[2]
            result_dict["env"] = result[3]
            result_dict["stage"] = result[4]
            result_dict["version"] = result[5]
            result_dict["cost"] = result[6]
            result_dict["nrows"] = result[7]
            result_dict["ncols"] = result[8]
            result_dict["null_counts"] = result[9]
            result_dict["memory_size"] = result[10]
            result_dict["filepath"] = result[11]
            result_dict["creator"] = result[12]
            result_dict["created"] = result[13]
            return result_dict
        except IndexError as e:
            msg = f"Index error in _results_to_dict method.\n{e}"
            logger.error(msg)
            raise IndexError(msg)
