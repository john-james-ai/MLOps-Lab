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
# Modified   : Wednesday December 28th 2022 03:04:28 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Data Layer Services associated with Database construction."""
from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import Dict, Tuple, List
import logging

from recsys.core.database.relational import RDB
from recsys.core.database.object import ODB
from .dto import DTO, DataFrameDTO, DatasetDTO, ProfileDTO, TaskDTO, JobDTO, FileDTO
from .base import DML
from recsys.core.entity.base import Entity


# ------------------------------------------------------------------------------------------------ #
#                                    BASE DATA ACCESS OBJECT                                       #
# ------------------------------------------------------------------------------------------------ #


class DAO(ABC):
    """Data Access Object"""

    def __init__(self, rdb: RDB, odb: ODB, dml: DML) -> None:
        self._rdb = rdb
        self._odb = odb
        self._dml = dml
        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )

    def __len__(self) -> int:
        result = self.read_all()
        length = 0 if result is None else len(result)
        return length

    def begin(self) -> None:
        """Begins a transaction."""
        cmd = self._dml.begin()
        self._rdb.begin(cmd.sql, cmd.args)

    def create(self, entity: Entity, persist: bool = True) -> Entity:
        """Adds an entity to the database.

        Args:
            entity (Entity): Entity

        Returns: entity with id set.
        """
        dto = entity.as_dto()
        cmd = self._dml.insert(dto)
        entity.id = self._rdb.insert(cmd.sql, cmd.args)
        if persist:
            self._odb.create(entity)
        return entity

    def read(self, id: int) -> Entity:
        """Retrieves an entity from the database, based upon id
        Args:
            id (int): The id for the entity.

        Returns a Entity
        """
        cmd = self._dml.select(id)
        row = self._rdb.select(cmd.sql, cmd.args)
        if len(row) > 0:
            dto = self._row_to_dto(row[0])
            return self._odb.read(dto.oid)
        else:
            msg = f"{self.__class__.__name__}.{id} does not exist."
            self._logger.info(msg)
            raise FileNotFoundError(msg)

    def read_by_name_mode(self, name: str, mode: str) -> DTO:
        """Retrieves an entity from the database, based upon name
        Args:
            name (str): The name assigned to the entity.

        Returns a Data Transfer Object (DTO)
        """
        cmd = self._dml.select_by_name_mode(name, mode)

        row = self._rdb.select(cmd.sql, cmd.args)
        if len(row) > 0:
            dto = self._row_to_dto(row[0])
            return self._odb.read(dto.oid)
        else:
            msg = f"{self.__class__.__name__}.{name} does not exist."
            self._logger.info(msg)
            raise FileNotFoundError(msg)

    def read_all(self) -> Dict[int, Entity]:
        """Returns a dictionary of Entities."""
        entities = {}
        cmd = self._dml.select_all()
        rows = self._rdb.select(cmd.sql, cmd.args)
        if len(rows) > 0:
            rows = self._rows_to_dict(rows)
            for __, dto in rows.items():
                entities[dto.oid] = self._odb.read(dto.oid)
        else:
            msg = f"There are no Entities in the {self.__class__.__name__} database."
            self._logger.info(msg)
        return entities

    def update(self, entity: Entity, persist: bool = True) -> None:
        """Updates an existing entity.

        Args:
            dto (DTO): Data Transfer Object
        """
        dto = entity.as_dto()
        cmd = self._dml.update(dto)
        self._rdb.update(cmd.sql, cmd.args)
        if persist:
            self._odb.update(entity)

    def exists(self, id: int) -> bool:
        """Returns True if the entity with id exists in the database.

        Args:
            id (int): id for the entity
        """
        cmd = self._dml.exists(id)
        oid = self._get_oid(id)
        return (self._rdb.exists(cmd.sql, cmd.args) and self._odb.exists(oid))

    def delete(self, id: int) -> None:
        """Deletes a Entity from the registry, given an id.
        Args:
            id (int): The id for the entity to delete.
        """
        cmd = self._dml.delete(id)
        oid = self._get_oid(id)
        self._rdb.delete(cmd.sql, cmd.args)
        try:
            self._odb.delete(oid)
        except FileNotFoundError:
            msg = f"Object id {oid} does not exist."
            self._logger.info(msg)

    def save(self) -> None:
        """Commits the changes to the database."""
        self._rdb.save()
        self._odb.save()

    def _rows_to_dict(self, results: List) -> Dict:
        """Converts the results to a dictionary of DTO objects."""
        results_dict = OrderedDict()
        for row in results:
            dto = self._row_to_dto(row)
            results_dict[dto.id] = dto
        return results_dict

    @abstractmethod
    def _get_oid(self, id) -> str:
        """Returns the object id for the given id and entity."""

    @abstractmethod
    def _row_to_dto(self, row: Tuple) -> DTO:
        """Converts a row from the Database into a Data Transfer Object."""


# ------------------------------------------------------------------------------------------------ #
#                                 DATAFRAME DATA ACCESS OBJECT                                     #
# ------------------------------------------------------------------------------------------------ #
class DataFrameDAO(DAO):
    def __init__(self, rdb: RDB, odb: ODB, dml: DML) -> None:
        super().__init__(rdb=rdb, odb=odb, dml=dml)

    def _get_oid(self, id) -> str:
        """Returns the object id for the given id and entity."""
        return f"dataframe_{id}"

    def _row_to_dto(self, row: Tuple) -> DataFrameDTO:
        try:
            return DataFrameDTO(
                id=row[0],
                oid=row[1],
                name=row[2],
                description=row[3],
                datasource=row[4],
                mode=row[5],
                stage=row[6],
                size=row[7],
                nrows=row[8],
                ncols=row[9],
                nulls=row[10],
                pct_nulls=row[11],
                parent_id=row[12],
                created=row[13],
                modified=row[14],
            )

        except IndexError as e:  # pragma: no cover
            msg = f"Index error in_row_to_dto method.\n{e}"
            self._logger.error(msg)
            raise IndexError(msg)


# ------------------------------------------------------------------------------------------------ #
#                                 DATASET DATA ACCESS OBJECT                                       #
# ------------------------------------------------------------------------------------------------ #
class DatasetDAO(DAO):
    def __init__(self, rdb: RDB, odb: ODB, dml: DML) -> None:
        super().__init__(rdb=rdb, odb=odb, dml=dml)

    def _get_oid(self, id) -> str:
        """Returns the object id for the given id and entity."""
        return f"dataset_{id}"

    def _row_to_dto(self, row: Tuple) -> DataFrameDTO:
        try:
            return DatasetDTO(
                id=row[0],
                oid=row[1],
                name=row[2],
                description=row[3],
                datasource=row[4],
                mode=row[5],
                stage=row[6],
                task_id=row[7],
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
    """Profile for Tasks"""

    def __init__(self, rdb: RDB, odb: ODB, dml: DML) -> None:
        super().__init__(rdb=rdb, odb=odb, dml=dml)

    def _get_oid(self, id) -> str:
        """Returns the object id for the given id and entity."""
        return f"profile_{id}"

    def _row_to_dto(self, row: Tuple) -> ProfileDTO:
        try:
            return ProfileDTO(
                id=row[0],
                oid=row[1],
                name=row[2],
                description=row[3],
                start=row[4],
                end=row[5],
                duration=row[6],
                user_cpu_time=row[7],
                percent_cpu_used=row[8],
                total_physical_memory=row[9],
                physical_memory_available=row[10],
                physical_memory_used=row[11],
                percent_physical_memory_used=row[12],
                active_memory_used=row[13],
                disk_usage=row[14],
                percent_disk_usage=row[15],
                read_count=row[16],
                write_count=row[17],
                read_bytes=row[18],
                write_bytes=row[19],
                read_time=row[20],
                write_time=row[21],
                bytes_sent=row[22],
                bytes_recv=row[23],
                task_id=row[24],
                created=row[25],
                modified=row[26],
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

    def __init__(self, rdb: RDB, odb: ODB, dml: DML) -> None:
        super().__init__(rdb=rdb, odb=odb, dml=dml)

    def _get_oid(self, id) -> str:
        """Returns the object id for the given id and entity."""
        return f"task_{id}"

    def _row_to_dto(self, row: Tuple) -> TaskDTO:
        try:
            return TaskDTO(
                id=row[0],
                oid=row[1],
                name=row[2],
                description=row[3],
                mode=row[4],
                stage=row[5],
                job_id=row[6],
                started=row[7],
                ended=row[8],
                duration=row[9],
                state=row[10],
                created=row[11],
                modified=row[12],
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

    def __init__(self, rdb: RDB, odb: ODB, dml: DML) -> None:
        super().__init__(rdb=rdb, odb=odb, dml=dml)

    def _get_oid(self, id) -> str:
        """Returns the object id for the given id and entity."""
        return f"job_{id}"

    def _row_to_dto(self, row: Tuple) -> JobDTO:
        try:
            return JobDTO(
                id=row[0],
                oid=row[1],
                name=row[2],
                description=row[3],
                mode=row[4],
                started=row[5],
                ended=row[6],
                duration=row[7],
                state=row[8],
                created=row[9],
                modified=row[10],
            )

        except IndexError as e:  # pragma: no cover
            msg = f"Index error in_row_to_dto method.\n{e}"
            self._logger.error(msg)
            raise IndexError(msg)


# ------------------------------------------------------------------------------------------------ #
#                                 FILE DATA ACCESS OBJECT                                          #
# ------------------------------------------------------------------------------------------------ #
class FileDAO(DAO):
    """File Data Access Object"""

    def __init__(self, rdb: RDB, odb: ODB, dml: DML) -> None:
        super().__init__(rdb=rdb, odb=odb, dml=dml)

    def _get_oid(self, id) -> str:
        """Returns the object id for the given id and entity."""
        return f"file_{id}"

    def _row_to_dto(self, row: Tuple) -> FileDTO:
        try:
            return FileDTO(
                id=row[0],
                oid=row[1],
                name=row[2],
                description=row[3],
                datasource=row[4],
                mode=row[5],
                stage=row[6],
                uri=row[7],
                task_id=row[8],
                created=row[9],
                modified=row[10],
            )

        except IndexError as e:  # pragma: no cover
            msg = f"Index error in_row_to_dto method.\n{e}"
            self._logger.error(msg)
            raise IndexError(msg)
