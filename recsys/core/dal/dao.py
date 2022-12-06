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
# Modified   : Tuesday December 6th 2022 05:52:20 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Data Layer Services associated with Database construction."""
from abc import abstractmethod
from collections import OrderedDict
from typing import Dict, Tuple, List

from recsys.core.data.database import Database
from .dto import DTO, FilesetDTO, DatasetDTO, JobDTO, OperatorDTO, DataSourceDTO, TaskDTO
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
        result = self.get_all()
        length = 0 if result is None else len(result)
        return length

    def add(self, dto: DTO) -> DTO:
        """Adds an entity to the database.

        Args:
            dto (DatasetDTO): Dataset Data Transfer Object

        Returns: DatasetDTO with id set.
        """
        cmd = self._dml.insert(dto)
        with self._database as db:
            dto.id = db.insert(cmd.sql, cmd.args)
            self._logger.debug(f"Inserted {dto.name} with id={dto.id}")
            return dto

    def get(self, id: int) -> DTO:
        """Retrieves an entity from the database, based upon id
        Args:
            id (int): The id for the entity.

        Returns a Data Transfer Object (DTO)
        """
        cmd = self._dml.select(id)
        with self._database as db:
            row = db.select(cmd.sql, cmd.args)
            if len(row) == 0:
                msg = f"Dataset {id} does not exist."
                self._logger.info(msg)
                return None
            else:
                return self._row_to_dto(row[0])

    def get_all(self) -> Dict[int, DTO]:
        """Returns a dictionary of Data Transfer Objects."""
        cmd = self._dml.select_all()
        with self._database as db:
            results = db.select(cmd.sql, cmd.args)
            if len(results) == 0:
                msg = "There are no Datasets in the database."
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
        """Deletes a Dataset from the registry, given an id.
        Args:
            id (int): The id for the entity to delete.
        """
        cmd = self._dml.delete(id)
        with self._database as db:
            db.delete(cmd.sql, cmd.args)

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
                source=row[3],
                workspace=row[4],
                stage=row[5],
                version=row[6],
                cost=row[7],
                nrows=row[8],
                ncols=row[9],
                null_counts=row[10],
                memory_size_mb=row[11],
                filename=row[12],
                filepath=row[13],
                task_id=row[14],
                creator=row[15],
                created=row[16],
                modified=row[17],
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
                id=row[0],
                name=row[1],
                description=row[2],
                source=row[3],
                filepath=row[4],
                filesize=row[5],
                task_id=row[6],
                created=row[7],
                modified=row[8],
            )

        except IndexError as e:  # pragma: no cover
            msg = f"Index error in_row_to_dto method.\n{e}"
            self._logger.error(msg)
            raise IndexError(msg)


# ------------------------------------------------------------------------------------------------ #
#                                  JOB DATA ACCESS OBJECT                                          #
# ------------------------------------------------------------------------------------------------ #
class JobDAO(DAO):
    """Data Access Object for Jobs"""

    def __init__(self, database: Database, dml: DML) -> None:
        super().__init__(database=database, dml=dml)

    def _row_to_dto(self, row: Tuple) -> JobDTO:
        try:
            return JobDTO(
                id=row[0],
                name=row[1],
                description=row[2],
                source=row[3],
                workspace=row[4],
                start=row[5],
                end=row[6],
                duration=row[7],
                cpu_user_time=row[8],
                cpu_percent=row[9],
                physical_memory_total=row[10],
                physical_memory_available=row[11],
                physical_memory_used=row[12],
                physical_memory_used_pct=row[13],
                RAM_used=row[14],
                RAM_used_pct=row[15],
                disk_usage=row[16],
                disk_usage_pct=row[17],
                disk_read_count=row[18],
                disk_write_count=row[19],
                disk_read_bytes=row[20],
                disk_write_bytes=row[21],
                disk_read_time=row[22],
                disk_write_time=row[23],
                network_bytes_sent=row[24],
                network_bytes_recv=row[25],
            )

        except IndexError as e:  # pragma: no cover
            msg = f"Index error in_row_to_dto method.\n{e}"
            self._logger.error(msg)
            raise IndexError(msg)


# ------------------------------------------------------------------------------------------------ #
#                                  OPERATOR DATA ACCESS OBJECT                                     #
# ------------------------------------------------------------------------------------------------ #
class OperatorDAO(DAO):
    """Data Access Object for Operators"""

    def __init__(self, database: Database, dml: DML) -> None:
        super().__init__(database=database, dml=dml)

    def _row_to_dto(self, row: Tuple) -> OperatorDTO:
        try:
            return OperatorDTO(
                id=row[0],
                name=row[1],
                description=row[2],
                module=row[3],
                classname=row[4],
                filepath=row[5],
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
                id=row[0],
                kind=row[1],
                name=row[2],
                description=row[3],
                website=row[4],
                link=row[5],
                filepath=row[6],
            )

        except IndexError as e:  # pragma: no cover
            msg = f"Index error in_row_to_dto method.\n{e}"
            self._logger.error(msg)
            raise IndexError(msg)


# ------------------------------------------------------------------------------------------------ #
#                                     TASK DATA ACCESS OBJECT                                      #
# ------------------------------------------------------------------------------------------------ #
class TaskDAO(DAO):
    """Data Access Object for Tasks"""

    def __init__(self, database: Database, dml: DML) -> None:
        super().__init__(database=database, dml=dml)

    def _row_to_dto(self, row: Tuple) -> TaskDTO:
        try:
            return TaskDTO(
                id=row[0],
                job_id=row[1],
                name=row[2],
                description=row[3],
                operator=row[4],
                module=row[5],
                input_kind=row[6],
                input_id=row[7],
                output_kind=row[8],
                output_id=row[9],
                start=row[10],
                end=row[11],
                duration=row[12],
                cpu_user_time=row[13],
                cpu_percent=row[14],
                physical_memory_total=row[15],
                physical_memory_available=row[16],
                physical_memory_used=row[17],
                physical_memory_used_pct=row[18],
                RAM_used=row[19],
                RAM_used_pct=row[20],
                disk_usage=row[21],
                disk_usage_pct=row[22],
                disk_read_count=row[23],
                disk_write_count=row[24],
                disk_read_bytes=row[25],
                disk_write_bytes=row[26],
                disk_read_time=row[27],
                disk_write_time=row[28],
                network_bytes_sent=row[29],
                network_bytes_recv=row[30],
            )

        except IndexError as e:  # pragma: no cover
            msg = f"Index error in_row_to_dto method.\n{e}"
            self._logger.error(msg)
            raise IndexError(msg)
