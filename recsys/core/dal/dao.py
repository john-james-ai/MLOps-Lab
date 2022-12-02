#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /dao.py                                                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday December 1st 2022 05:48:13 am                                              #
# Modified   : Thursday December 1st 2022 07:24:55 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Data Access Object Module."""
from dataclasses import dataclass
from typing import Union, Tuple, List, Dict
import logging
from collections import OrderedDict
import pandas as pd

from .base import DAO, DTO
from .sql import (
    CountDatasets,
    InsertDataset,
    SelectDataset,
    ListDatasets,
    ArchiveDataset,
    RestoreDataset,
    DatasetExists,
    DatasetIdExists,
    FindDataset,
    DeleteDataset,
    ListEnvDatasets,
    ListSourceDatasets,
)
from ..data.database import Database

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@dataclass
class DatasetDTO(DTO):
    id: int
    task_id: int
    step_id: int
    source: str
    env: str
    name: str
    description: str
    step_input: bool
    step_output: bool
    stage: str
    version: int
    cost: int
    nrows: int
    ncols: int
    null_counts: int
    memory_size_mb: int
    filepath: str
    archived: bool
    creator: str
    created: str


# ------------------------------------------------------------------------------------------------ #
class DatasetDAO(DAO):
    def __init__(self, database: Database) -> None:
        self._database = database

    def __len__(self) -> int:
        """Returns the number of items in the registry."""
        query = CountDatasets()
        with self._database as db:
            return db.count(sql=query.sql, args=query.args)

    def add(self, dto: DatasetDTO) -> DatasetDTO:
        """Adds a dataset to the database. If a duplicate is found, the version is bumped.

        Args:
            dto (DatasetDTO): A Dataset Data Transfer Object
        """
        dsad = dto.as_dict()
        insert = InsertDataset(**dsad)
        with self._database as db:
            dto.id = db.insert(insert.sql, insert.args)
        return dto

    def get(self, id: int) -> DatasetDTO:
        """Retrieves dto metadata from the registry, given an id

        Args:
            id (int): The id for the Dataset to retrieve.
        """
        result = None
        select = SelectDataset(id=id)
        with self._database as db:
            result = db.select(select.sql, select.args)
        if len(result) > 0:
            _, result = self._row_to_dto(result[0])
            return result
        else:
            msg = f"Dataset id: {id} not found."
            logger.error(msg)
            raise FileNotFoundError

    def get_all(self, as_dict: bool = False) -> Union[pd.DataFrame, dict]:
        """Returns a Dataframe representation of the registry."""
        select = ListDatasets()
        with self._database as db:
            results = db.select(sql=select.sql, args=select.args)
            if as_dict:
                result = self._results_to_dict(results)
            else:
                result = self._results_to_df(results)

        return result

    def archive(self, dto: DatasetDTO) -> None:
        archive = ArchiveDataset(id)
        with self._database as db:
            db.update(sql=archive.sql, args=archive.args)

    def restore(self, id: int) -> None:
        restore = RestoreDataset(id)
        with self._database as db:
            db.update(sql=restore.sql, args=restore.args)

    def exists_id(self, id: int) -> bool:
        """Returns true if a dto with id exists

        Args:
            id (int): Dataset id
        """
        exists = DatasetIdExists(id=id)
        with self._database as db:
            return db.exists(sql=exists.sql, args=exists.args)

    def exists(self, dto: DatasetDTO) -> bool:
        """Returns True if a Dataset or Datasets match the above criteria.

        Args:
            dto (DatasetDTO): Required. Dataset object
        """
        exists = DatasetExists(
            source=dto.source, env=dto.env, name=dto.name, stage=dto.stage, version=dto.version
        )
        with self._database as db:
            return db.exists(sql=exists.sql, args=exists.args)

    def find_dataset(self, source: str, env: str, name: str, stage: str) -> pd.DataFrame:
        """Finds a Dataset or Datasets that match the search criteria.

        Args:
            name (str): Required name of Dataset.
            stage (str): Optional, one of 'input', 'interim', or 'final'.
        """

        find = FindDataset(source=source, env=env, name=name, stage=stage)
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

    def list_source_datasets(self, source: str) -> pd.DataFrame:
        cmd = ListSourceDatasets(source=source)
        with self._database as db:
            results = db.select(cmd.sql, cmd.args)
            return self._results_to_df(results)

    def list_env_datasets(self, env: str) -> pd.DataFrame:
        cmd = ListEnvDatasets(env=env)
        with self._database as db:
            results = db.select(cmd.sql, cmd.args)
            return self._results_to_df(results)

    def _results_to_dict(self, results: List) -> Dict:
        results_dict = OrderedDict()
        for row in results:
            id, result = self._row_to_dict(row)
            results_dict[str(id)] = result
        return results_dict

    def _results_to_df(self, results: list) -> pd.DataFrame:
        results = self._results_to_dict(results)
        df = pd.DataFrame.from_dict(results).T
        return df

    def _row_to_dto(self, row: Tuple) -> Dict:
        try:
            return DatasetDTO(
                id=row[0],
                source=row[1],
                env=row[2],
                stage=row[3],
                name=row[4],
                description=row[5],
                version=row[6],
                cost=row[7],
                nrows=row[8],
                ncols=row[9],
                null_counts=row[10],
                memory_size_mb=row[11],
                filepath=row[12],
                archived=row[13],
                creator=row[14],
                created=row[15],
            )
        except IndexError as e:  # pragma: no cover
            msg = f"Index error in_row_to_dto method.\n{e}"
            logger.error(msg)
            raise IndexError(msg)
