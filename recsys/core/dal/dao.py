#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/dal/dao.py                                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 06:27:36 am                                                #
# Modified   : Sunday December 4th 2022 07:11:21 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Data Layer Services associated with Database construction."""
from typing import Dict, Tuple

from recsys.core.data.database import Database
from .base import DAO
from .dto import DatasetDTO
from .sql import (
    InsertDataset,
    SelectDataset,
    SelectAllDatasets,
    UpdateDataset,
    DeleteDataset,
    DatasetExists,
)


# ------------------------------------------------------------------------------------------------ #
#                                 DATASET DATA ACCESS OBJECT                                       #
# ------------------------------------------------------------------------------------------------ #
class DatasetDAO(DAO):
    """Data Access Object for Datasets"""

    def __init__(self, database: Database) -> None:
        self._database = database
        super().__init__()

    def __len__(self) -> int:
        result = self.get_all()
        length = 0 if result is None else len(result)
        return length

    def add(self, dto: DatasetDTO) -> DatasetDTO:
        """Adds an entity to the database.

        Args:
            dto (DatasetDTO): Dataset Data Transfer Object

        Returns: DatasetDTO with id set.
        """
        cmd = InsertDataset(dto)
        with self._database as db:
            dto.id = db.insert(cmd.sql, cmd.args)
            self._logger.debug(f"Inserted {dto.name} with id={dto.id}")
            return dto

    def get(self, id: int) -> DatasetDTO:
        """Retrieves an entity from the database, based upon id
        Args:
            id (int): The id for the entity.

        Returns a Data Transfer Object (DTO)
        """
        cmd = SelectDataset(id)
        with self._database as db:
            row = db.select(cmd.sql, cmd.args)
            if len(row) == 0:
                msg = f"Dataset {id} does not exist."
                self._logger.info(msg)
                return None
            else:
                return self._row_to_dto(row[0])

    def get_all(self) -> Dict[int, DatasetDTO]:
        """Returns a dictionary of Data Transfer Objects."""
        cmd = SelectAllDatasets()
        with self._database as db:
            results = db.select(cmd.sql, cmd.args)
            if len(results) == 0:
                msg = "There are no Datasets in the database."
                self._logger.info(msg)
                return None
            else:
                return self._results_to_dict(results)

    def update(self, dto: DatasetDTO) -> None:
        """Updates an existing entity.

        Args:
            dto (DTO): Data Transfer Object
        """
        cmd = UpdateDataset(dto)
        with self._database as db:
            db.update(cmd.sql, cmd.args)

    def exists(self, id: int) -> bool:
        """Returns True if the entity with id exists in the database.

        Args:
            id (int): id for the entity
        """
        cmd = DatasetExists(id)
        with self._database as db:
            return db.exists(cmd.sql, cmd.args)

    def delete(self, id: int) -> None:
        """Deletes a Dataset from the registry, given an id.
        Args:
            id (int): The id for the entity to delete.
        """
        cmd = DeleteDataset(id)
        with self._database as db:
            db.delete(cmd.sql, cmd.args)

    def _row_to_dto(self, row: Tuple) -> DatasetDTO:
        try:
            return DatasetDTO(
                id=row[0],
                name=row[1],
                description=row[2],
                source=row[3],
                env=row[4],
                stage=row[5],
                version=row[6],
                cost=row[7],
                nrows=row[8],
                ncols=row[9],
                null_counts=row[10],
                memory_size_mb=row[11],
                filepath=row[12],
                task_id=row[13],
                created=row[14],
                modified=row[15],
            )

        except IndexError as e:  # pragma: no cover
            msg = f"Index error in_row_to_dto method.\n{e}"
            self._logger.error(msg)
            raise IndexError(msg)
