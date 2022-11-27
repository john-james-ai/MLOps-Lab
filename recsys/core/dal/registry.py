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
# Modified   : Sunday November 27th 2022 04:38:53 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod
from typing import Any, Union, Tuple, List, Dict
import logging
from collections import OrderedDict
import pandas as pd

from recsys.config.data import DB_TABLES
from .dataset import Dataset
from .database import Database
from .sequel import (
    CreateDatasetRegistryTable,
    DropDatasetRegistryTable,
    SelectArchivedDatasets,
    ArchiveDataset,
    RestoreDataset,
    TableExists,
    DatasetExists,
    VersionExists,
    SelectDataset,
    CountDatasets,
    SelectCurrentDatasets,
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
        assert self._table_exists()

    def add(self, dataset: Dataset) -> Dataset:
        """Adds a dataset to the registry. If a duplicate is found, the version is bumped.

        Args:
            dataset (Dataset): The Dataset to add to the registry.
        """
        dsad = dataset.as_dict()
        insert = InsertDataset(**dsad)
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
            _, result = self._row_to_dict(result[0])
            return result
        else:
            msg = f"Dataset id: {id} not found."
            logger.error(msg)
            raise FileNotFoundError

    def get_all(self, as_dict: bool = False) -> Union[pd.DataFrame, dict]:
        """Returns a Dataframe representation of the registry."""
        select = SelectCurrentDatasets()
        with self._database as db:
            results = db.select(sql=select.sql, args=select.args)
            if as_dict:
                result = self._results_to_dict(results)
            else:
                result = self._results_to_df(results)

        return result

    def get_archive(self) -> dict:
        select = SelectArchivedDatasets()
        with self._database as db:
            results = db.select(sql=select.sql, args=select.args)
            return self._results_to_dict(results)

    def archive(self, id: int) -> None:
        archive = ArchiveDataset(id)
        with self._database as db:
            db.update(sql=archive.sql, args=archive.args)

    def restore(self, id: int) -> None:
        restore = RestoreDataset(id)
        with self._database as db:
            db.update(sql=restore.sql, args=restore.args)

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
            stage (str): Optional, one of 'input', 'interim', or 'final'.
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

    def _create_registry_if_not_exists(self) -> None:
        create = CreateDatasetRegistryTable()
        with self._database as db:
            db.create(create.sql, create.args)

    def _drop_registry(self) -> None:
        drop = DropDatasetRegistryTable()
        with self._database as db:
            db.drop(drop.sql, drop.args)

    def _table_exists(self) -> bool:
        tablename = DB_TABLES[self.__class__.__name__]
        exists = TableExists(table=tablename)
        with self._database as db:
            return db.exists(exists.sql, exists.args)

    def _results_to_dict(self, results: List) -> Dict:
        results_dict = OrderedDict()
        for row in results:
            id, result = self._row_to_dict(row)
            results_dict[str(id)] = result
        return results_dict

    def _row_to_dict(self, row: Tuple) -> Dict:
        try:
            row_dict = {}
            row_dict["id"] = row[0]
            row_dict["name"] = row[1]
            row_dict["description"] = row[2]
            row_dict["stage"] = row[3]
            row_dict["version"] = row[4]
            row_dict["cost"] = row[5]
            row_dict["nrows"] = row[6]
            row_dict["ncols"] = row[7]
            row_dict["null_counts"] = row[8]
            row_dict["memory_size_mb"] = row[9]
            row_dict["filepath"] = row[10]
            row_dict["archived"] = row[11]
            row_dict["creator"] = row[12]
            row_dict["created"] = row[13]
            return row_dict["id"], row_dict
        except IndexError as e:  # pragma: no cover
            msg = f"Index error in _results_to_dict method.\n{e}"
            logger.error(msg)
            raise IndexError(msg)

    def _results_to_df(self, results: list) -> pd.DataFrame:
        results = self._results_to_dict(results)
        datasets = pd.DataFrame.from_dict(results)
        return datasets
