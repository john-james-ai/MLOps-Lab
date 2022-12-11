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
# Modified   : Saturday December 10th 2022 07:34:09 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Data Layer Services associated with Database construction."""
from abc import abstractmethod
from collections import OrderedDict
from typing import Dict, Tuple, List

from recsys.core.data.database import Database
from .dto import DTO, FilesetDTO, DatasetDTO, DataSourceDTO, ProfileDTO, TaskResourceDTO, TaskDTO, JobDTO
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
        if self._duplicate_name(dto):
            msg = f"Item named {dto.name} already exists."
            self._logger.error(msg)
            raise ValueError(msg)

        cmd = self._dml.insert(dto)

        with self._database as db:
            dto.id = db.insert(cmd.sql, cmd.args)
            self._logger.debug(f"Inserted {dto.name} with id={dto.id}")
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

    def read_all_names(self) -> list:
        """Returns a list of names for entities in the Database."""
        cmd = self._dml.select_all_names()
        with self._database as db:
            names = db.select(cmd.sql, cmd.args)
            return [name for t in names for name in t]

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

    def _duplicate_name(self, dto: DTO) -> bool:
        return dto.name in self.read_all_names()

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
                id=int(row[0]),
                name=row[1],
                description=row[2],
                datasource=row[3],
                workspace=row[4],
                stage=row[5],
                filepath=row[6],
                task_id=int(row[7]),
                created=row[8],
                modified=row[9],
            )

        except IndexError as e:  # pragma: no cover
            msg = f"Index error in_row_to_dto method.\n{e}"
            self._logger.error(msg)
            raise IndexError(msg)


# ------------------------------------------------------------------------------------------------ #
#                                 FILESET DATA ACCESS OBJECT                                       #
# ------------------------------------------------------------------------------------------------ #
class FilesetDAO(DAO):
    """Data Access Object for Filesets"""

    def __init__(self, database: Database, dml: DML) -> None:
        super().__init__(database=database, dml=dml)

    def _row_to_dto(self, row: Tuple) -> FilesetDTO:
        try:
            return FilesetDTO(
                id=int(row[0]),
                name=row[1],
                description=row[2],
                datasource=row[3],
                workspace=row[4],
                stage=row[5],
                uri=row[6],
                task_id=int(row[7]),
                created=row[8],
                modified=row[9],
            )

        except IndexError as e:  # pragma: no cover
            msg = f"Index error in_row_to_dto method.\n{e}"
            self._logger.error(msg)
            raise IndexError(msg)


# ------------------------------------------------------------------------------------------------ #
#                                 PROFILE DATA ACCESS OBJECT                                       #
# ------------------------------------------------------------------------------------------------ #
class ProfileDAO(DAO):
    """Data Access Object for Filesets"""

    def __init__(self, database: Database, dml: DML) -> None:
        super().__init__(database=database, dml=dml)

    def _row_to_dto(self, row: Tuple) -> ProfileDTO:
        try:
            return ProfileDTO(
                id=int(row[0]),
                name=row[1],
                description=row[2],
                started=row[3],
                ended=row[4],
                duration=int(row[5]),
                user_cpu_time=int(row[6]),
                percent_cpu_used=float(row[7]),
                total_physical_memory=int(row[8]),
                physical_memory_available=int(row[9]),
                physical_memory_used=int(row[10]),
                percent_physical_memory_used=float(row[11]),
                active_memory_used=int(row[12]),
                disk_usage=int(row[13]),
                percent_disk_usage=float(row[14]),
                read_count=int(row[15]),
                write_count=int(row[16]),
                read_bytes=int(row[17]),
                write_bytes=int(row[18]),
                read_time=int(row[19]),
                write_time=int(row[20]),
                bytes_sent=int(row[21]),
                bytes_recv=int(row[22]),
                created=row[23],
                modified=row[24],
            )

        except IndexError as e:  # pragma: no cover
            msg = f"Index error in_row_to_dto method.\n{e}"
            self._logger.error(msg)
            raise IndexError(msg)


# ------------------------------------------------------------------------------------------------ #
#                                  DATASOURCE DATA ACCESS OBJECT                                   #
# ------------------------------------------------------------------------------------------------ #
class DataSourceDAO(DAO):
    """Data Access Object for DataSources"""

    def __init__(self, database: Database, dml: DML) -> None:
        super().__init__(database=database, dml=dml)

    def _row_to_dto(self, row: Tuple) -> DataSourceDTO:
        try:
            return DataSourceDTO(
                id=int(row[0]),
                name=row[1],
                publisher=row[2],
                description=row[3],
                website=row[4],
                created=row[5],
                modified=row[6],
            )

        except IndexError as e:  # pragma: no cover
            msg = f"Index error in_row_to_dto method.\n{e}"
            self._logger.error(msg)
            raise IndexError(msg)


# ------------------------------------------------------------------------------------------------ #
#                              TASK_RESOURCE DATA ACCESS OBJECT                                    #
# ------------------------------------------------------------------------------------------------ #
class TaskResourceDAO(DAO):
    """TaskResource Data Access Object"""

    def __init__(self, database: Database, dml: DML) -> None:
        super().__init__(database=database, dml=dml)

    def _row_to_dto(self, row: Tuple) -> TaskResourceDTO:
        try:
            return TaskResourceDTO(
                id=int(row[0]),
                name=row[1],
                description=row[2],
                task_id=int(row[3]),
                resource_kind=row[4],
                resource_id=int(row[5]),
                resource_context=row[6],
                created=row[7],
                modified=row[8],
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
                id=int(row[0]),
                name=row[1],
                description=row[2],
                workspace=row[3],
                operator=row[4],
                module=row[5],
                job_id=int(row[6]),
                profile_id=int(row[7]),
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
                id=int(row[0]),
                name=row[1],
                description=row[2],
                pipeline=row[3],
                workspace=row[4],
                profile_id=int(row[5]),
                created=row[6],
                modified=row[7],
            )

        except IndexError as e:  # pragma: no cover
            msg = f"Index error in_row_to_dto method.\n{e}"
            self._logger.error(msg)
            raise IndexError(msg)
