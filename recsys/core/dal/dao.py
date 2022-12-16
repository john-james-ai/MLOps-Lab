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
# Modified   : Thursday December 15th 2022 03:18:29 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Data Layer Services associated with Database construction."""
from abc import abstractmethod
from collections import OrderedDict
from typing import Dict, Tuple, List

from recsys.core.database.base import Database
from .dto import DTO, DatasetDTO, ProfileDTO, TaskDTO, JobDTO
from .base import DML
from recsys.core import Service


# ------------------------------------------------------------------------------------------------ #
#                                    BASE DATA ACCESS OBJECT                                       #
# ------------------------------------------------------------------------------------------------ #


class DAO(Service):
    """Data Access Object"""

    def __init__(self, database: Database, dml: DML) -> None:
        self._database = database
        self._dml = dml
        super().__init__()

    def __len__(self) -> int:
        result = self.read_all()
        length = 0 if result is None else len(result)
        return length

    def create(self, dto: DTO) -> DTO:
        """Adds an entity to the database.

        Args:
            dto (DTO): Data Transfer Object

        Returns: DTO with id set.
        """
        cmd = self._dml.insert(dto)
        self._logger.debug(f"\n\n{dto}")

        with self._database as db:
            dto.id = db.insert(cmd.sql, cmd.args)
            return dto

    def read(self, id: int) -> DTO:
        """Retrieves an entity from the database, based upon id
        Args:
            id (int): The id for the entity.

        Returns a Data Transfer Object (DTO)
        """
        cmd = self._dml.select(id)
        with self._database as db:
            row = db.select(cmd.sql, cmd.args)
            if len(row) == 0:
                msg = f"{self.__class__.__name__}.{id} does not exist."
                self._logger.info(msg)
                raise FileNotFoundError(msg)
            else:
                return self._row_to_dto(row[0])

    def read_by_name(self, name: str) -> DTO:
        """Retrieves an entity from the database, based upon name
        Args:
            name (str): The name assigned to the entity.

        Returns a Data Transfer Object (DTO)
        """
        cmd = self._dml.select_by_name(name)
        with self._database as db:
            row = db.select(cmd.sql, cmd.args)
            if len(row) == 0:
                msg = f"{self.__class__.__name__}.{name} does not exist."
                self._logger.info(msg)
                raise FileNotFoundError(msg)
            else:
                return self._row_to_dto(row[0])

    def read_all(self) -> Dict[int, DTO]:
        """Returns a dictionary of Data Transfer Objects."""
        cmd = self._dml.select_all()
        with self._database as db:
            results = db.select(cmd.sql, cmd.args)
            if len(results) == 0:
                msg = f"There are no Entities in the {self.__class__.__name__} database."
                self._logger.info(msg)
                return None
            else:
                return self._results_to_dict(results)

    def update(self, dto: DTO) -> None:
        """Updates an existing entity.

        Args:
            dto (DTO): Data Transfer Object
        """
        cmd = self._dml.update(dto)
        with self._database as db:
            db.update(cmd.sql, cmd.args)

    def exists(self, id: int) -> bool:
        """Returns True if the entity with id exists in the database.

        Args:
            id (int): id for the entity
        """
        cmd = self._dml.exists(id)
        with self._database as db:
            return db.exists(cmd.sql, cmd.args)

    def delete(self, id: int) -> None:
        """Deletes a Entity from the registry, given an id.
        Args:
            id (int): The id for the entity to delete.
        """
        cmd = self._dml.delete(id)
        with self._database as db:
            db.delete(cmd.sql, cmd.args)

    def save(self) -> None:
        """Commits the changes to the database."""
        with self._database as db:
            db.save()

    def _results_to_dict(self, results: List) -> Dict:
        """Converts the results to a dictionary of DTO objects."""
        results_dict = OrderedDict()
        for row in results:
            dto = self._row_to_dto(row)
            results_dict[dto.id] = dto
        return results_dict

    @abstractmethod
    def _row_to_dto(self, row: Tuple) -> DTO:
        """Reformats a row of output to a DTO object."""


# ------------------------------------------------------------------------------------------------ #
#                                 DATASET DATA ACCESS OBJECT                                       #
# ------------------------------------------------------------------------------------------------ #


class DatasetDAO(DAO):
    def __init__(self, database: Database, dml: DML) -> None:
        super().__init__(database=database, dml=dml)

    def _row_to_dto(self, row: Tuple) -> DatasetDTO:
        try:
            return DatasetDTO(
                id=row[0],
                name=row[1],
                description=row[2],
                datasource=row[3],
                workspace=row[4],
                stage=row[5],
                filename=row[6],
                uri=row[7],
                size=row[8],
                nrows=row[9],
                ncols=row[10],
                nulls=row[11],
                pct_nulls=row[12],
                task_id=row[13],
                created=row[14],
                modified=row[15],
            )

        except IndexError as e:  # pragma: no cover
            msg = f"Index error in_row_to_dto method.\n{e}"
            self._logger.error(msg)
            raise IndexError(msg)


# ------------------------------------------------------------------------------------------------ #
#                                 PROFILE DATA ACCESS OBJECT                                       #
# ------------------------------------------------------------------------------------------------ #
class ProfileDAO(DAO):
    """Profile for Tasks"""

    def __init__(self, database: Database, dml: DML) -> None:
        super().__init__(database=database, dml=dml)

    def _row_to_dto(self, row: Tuple) -> ProfileDTO:
        try:
            return ProfileDTO(
                id=row[0],
                name=row[1],
                description=row[2],
                started=row[3],
                ended=row[4],
                duration=row[5],
                user_cpu_time=row[6],
                percent_cpu_used=row[7],
                total_physical_memory=row[8],
                physical_memory_available=row[9],
                physical_memory_used=row[10],
                percent_physical_memory_used=row[11],
                active_memory_used=row[12],
                disk_usage=row[13],
                percent_disk_usage=row[14],
                read_count=row[15],
                write_count=row[16],
                read_bytes=row[17],
                write_bytes=row[18],
                read_time=row[19],
                write_time=row[20],
                bytes_sent=row[21],
                bytes_recv=row[22],
                task_id=row[23],
                created=row[24],
                modified=row[25],
            )

        except IndexError as e:  # pragma: no cover
            msg = f"Index error in_row_to_dto method.\n{e}"
            self._logger.error(msg)
            raise IndexError(msg)


# ------------------------------------------------------------------------------------------------ #
#                                TASK DATA ACCESS OBJECT                                           #
# ------------------------------------------------------------------------------------------------ #
class TaskDAO(DAO):
    """Task Data Access Object"""

    def __init__(self, database: Database, dml: DML) -> None:
        super().__init__(database=database, dml=dml)

    def _row_to_dto(self, row: Tuple) -> TaskDTO:
        try:
            return TaskDTO(
                id=row[0],
                name=row[1],
                description=row[2],
                workspace=row[3],
                operator=row[4],
                module=row[5],
                job_id=row[6],
                profile_id=row[7],
                created=row[8],
                modified=row[9],
            )

        except IndexError as e:  # pragma: no cover
            msg = f"Index error in_row_to_dto method.\n{e}"
            self._logger.error(msg)
            raise IndexError(msg)


# ------------------------------------------------------------------------------------------------ #
#                                 JOB DATA ACCESS OBJECT                                           #
# ------------------------------------------------------------------------------------------------ #
class JobDAO(DAO):
    """Job Data Access Object"""

    def __init__(self, database: Database, dml: DML) -> None:
        super().__init__(database=database, dml=dml)

    def _row_to_dto(self, row: Tuple) -> JobDTO:
        try:
            return JobDTO(
                id=row[0],
                name=row[1],
                description=row[2],
                workspace=row[3],
                started=row[4],
                ended=row[5],
                duration=row[6],
                tasks_completed=row[7],
                created=row[8],
                modified=row[9],
            )

        except IndexError as e:  # pragma: no cover
            msg = f"Index error in_row_to_dto method.\n{e}"
            self._logger.error(msg)
            raise IndexError(msg)
