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
# Modified   : Friday November 25th 2022 05:13:37 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod
from typing import Any
import logging
import pandas as pd

from recsys.config.base import DATASET_FEATURES, DB_TABLES
from .dataset import Dataset
from .database import Database
from .sequel import (
    CreateDatasetRegistryTable,
    DropDatasetRegistryTable,
    TableExists,
    DatasetExists,
    VersionExists,
    SelectDataset,
    CountDatasets,
    SelectAllDatasets,
    InsertDataset,
    DeleteDataset,
    FindDatasetByName,
    FindDatasetByNameStage,
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
        """Returns true if a dataset with the same name, stage and version exists in the registry."""

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
        self._create_registry_if_not_exists()

    def __len__(self) -> int:
        """Returns the number of items in the registry."""
        query = CountDatasets()
        with self._database as db:
            return db.count(sql=query.sql, args=query.args)

    def reset(self) -> None:
        """Drops the registry table and re-creatas it."""
        self._drop_registry()
        self._create_registry_if_not_exists()

    def add(self, dataset: Dataset) -> Dataset:
        """Adds a dataset to the registry. If a duplicate is found, the version is bumped.

        Args:
            dataset (Dataset): The Dataset to add to the registry.
        """
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
        if len(result) > 0:
            result = result[0]
            return self._results_to_dict(result)
        else:
            msg = f"Dataset id: {id} not found."
            logger.error(msg)
            raise FileNotFoundError

    def get_all(self) -> pd.DataFrame:
        """Returns a Dataframe representation of the registry."""
        select = SelectAllDatasets()
        with self._database as db:
            results = db.select(sql=select.sql, args=select.args)
            return self._results_to_df(results)

    def exists(self, id: int) -> bool:
        """Returns true if a dataset with id exists

        Args:
            id (int): Dataset id
        """
        exists = DatasetExists(id=id)
        with self._database as db:
            return db.exists(sql=exists.sql, args=exists.args)

    def version_exists(self, dataset: Dataset) -> bool:
        """Returns True if a Dataset or Datasets match the above criteria.

        Args:
            dataset (Dataset): Required. Dataset object
        """
        exists = VersionExists(name=dataset.name, stage=dataset.stage, version=dataset.version)
        with self._database as db:
            return db.exists(sql=exists.sql, args=exists.args)

    def find_dataset(self, name: str, stage: str = None) -> pd.DataFrame:
        """Finds a Dataset or Datasets that match the search criteria.

        Args:
            name (str): Required name of Dataset.
            stage (str): Optional, one of 'raw', 'interim', or 'cooked'.
        """

        if not stage:
            find = FindDatasetByName(name=name)
        else:
            find = FindDatasetByNameStage(name=name, stage=stage)
        with self._database as db:
            results = db.select(find.sql, find.args)
            return self._results_to_df(results)

    def remove(self, id: int) -> None:
        """Deletes a Dataset from the registry, given an id.

        Args:
            id (int): The id for the Dataset to remove.
        """
        remove = DeleteDataset(id=id)
        with self._database as db:
            db.delete(sql=remove.sql, args=remove.args)

    def _table_exists(self) -> bool:
        tablename = DB_TABLES[self.__class__.__name__]
        exists = TableExists(table=tablename)
        with self._database as db:
            return db.exists(exists.sql, exists.args)

    def _create_registry_if_not_exists(self) -> None:
        create = CreateDatasetRegistryTable()
        with self._database as db:
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
            result_dict["stage"] = result[3]
            result_dict["version"] = result[4]
            result_dict["cost"] = result[5]
            result_dict["nrows"] = result[6]
            result_dict["ncols"] = result[7]
            result_dict["null_counts"] = result[8]
            result_dict["memory_size"] = result[9]
            result_dict["filepath"] = result[10]
            result_dict["creator"] = result[11]
            result_dict["created"] = result[12]
            return result_dict
        except IndexError as e:
            msg = f"Index error in _results_to_dict method.\n{e}"
            logger.error(msg)
            raise IndexError(msg)

    def _results_to_df(self, results: tuple) -> pd.DataFrame:
        datasets = []
        if len(results) > 0:
            for result in results:
                datasets.append(self._results_to_dict(result))
            datasets = pd.DataFrame.from_dict(datasets)
        return datasets
