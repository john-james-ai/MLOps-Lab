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
# Modified   : Friday December 9th 2022 09:14:13 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Data Layer Services associated with Database construction."""
from abc import abstractmethod
from collections import OrderedDict
from typing import Dict, Tuple, List

from recsys.core.data.database import Database
from .dto import (
    DTO, FilesetDTO, DatasetDTO, JobDTO, DataSourceDTO, TaskDTO,
    TimeDTO, CPUDTO, MemoryDTO, DiskDTO, NetworkDTO, ProfileDTO)
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
            dto (DTO): Data Transfer Object

        Returns: DTO with id set.
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
                msg = f"{self.__class__.__name__}.{id} does not exist."
                self._logger.info(msg)
                raise FileNotFoundError(msg)
            else:
                return self._row_to_dto(row[0])

    def get_all(self) -> Dict[int, DTO]:
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
                id=int(row[0]),
                name=row[1],
                description=row[2],
                source=row[3],
                workspace=row[4],
                stage=row[5],
                version=int(row[6]),
                cost=row[7],
                nrows=row[8],
                ncols=row[9],
                null_counts=row[10],
                memory_size_mb=row[11],
                filename=row[12],
                filepath=row[13],
                task_id=int(row[14]),
                created=row[15],
                modified=row[16],
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
                source=row[3],
                filepath=row[4],
                filesize=row[5],
                task_id=int(row[6]),
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
            time = TimeDTO(
                start=row[5],
                end=row[6],
                duration=row[7],
            )

            cpu = CPUDTO(
                user_time=row[8],
                percent_cpu_used=row[9],
            )

            memory = MemoryDTO(
                total_physical_memory=row[10],
                physical_memory_available=row[11],
                physical_memory_used=row[12],
                percent_physical_memory_used=row[13],
                active_memory_used=row[14],
            )

            disk = DiskDTO(
                disk_usage=row[15],
                percent_disk_usage=row[16],
                read_count=row[17],
                write_count=row[18],
                read_bytes=row[19],
                write_bytes=row[20],
                read_time=row[21],
                write_time=row[22],
            )

            network = NetworkDTO(
                bytes_sent=row[23],
                bytes_recv=row[24],
            )

            profile = ProfileDTO(
                time=time,
                cpu=cpu,
                memory=memory,
                disk=disk,
                network=network
            )
            return JobDTO(
                id=int(row[0]),
                name=row[1],
                description=row[2],
                source=row[3],
                workspace=row[4],
                profile=profile,
                created=row[25],
                modified=row[26],
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
                url=row[5],
                created=row[6],
                modified=row[7],
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
            time = TimeDTO(
                start=row[11],
                end=row[12],
                duration=row[13],
            )

            cpu = CPUDTO(
                user_time=row[14],
                percent_cpu_used=row[15],
            )

            memory = MemoryDTO(
                total_physical_memory=row[16],
                physical_memory_available=row[17],
                physical_memory_used=row[18],
                percent_physical_memory_used=row[19],
                active_memory_used=row[20],
            )

            disk = DiskDTO(
                disk_usage=row[21],
                percent_disk_usage=row[22],
                read_count=row[23],
                write_count=row[24],
                read_bytes=row[25],
                write_bytes=row[26],
                read_time=row[27],
                write_time=row[28],
            )

            network = NetworkDTO(
                bytes_sent=row[29],
                bytes_recv=row[30],
            )

            profile = ProfileDTO(
                time=time,
                cpu=cpu,
                memory=memory,
                disk=disk,
                network=network
            )
            return TaskDTO(
                id=int(row[0]),
                job_id=int(row[1]),
                name=row[2],
                description=row[3],
                workspace=row[4],
                operator=row[5],
                module=row[6],
                input_kind=row[7],
                input_id=int(row[8]),
                output_kind=row[9],
                output_id=int(row[10]),
                profile=profile,
                created=row[25],
                modified=row[26],
            )

        except IndexError as e:  # pragma: no cover
            msg = f"Index error in_row_to_dto method.\n{e}"
            self._logger.error(msg)
            raise IndexError(msg)
