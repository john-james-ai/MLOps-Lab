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
# Modified   : Wednesday January 4th 2023 01:42:00 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Data Layer Services associated with Database construction."""
from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import Dict, Tuple, List
import logging
import mysql.connector

from recsys.core.database.relational import Database
from .dto import DTO, DataFrameDTO, DatasetDTO, ProfileDTO, TaskDTO, JobDTO, FileDTO, DataSourceDTO, DataSourceURLDTO
from .base import DML
from recsys.core.entity.base import Entity


# ------------------------------------------------------------------------------------------------ #
#                                    BASE DATA ACCESS OBJECT                                       #
# ------------------------------------------------------------------------------------------------ #
class DAO(ABC):
    """Data Access Object

    Provides access to the underlying relational database

    Args:
        database (Database): Relational database object.
        dml (DML): The Data Manipulation Language for the database.
    """

    def __init__(self, dml: DML) -> None:
        self._dml = dml
        self._entity = dml.entity
        self._database = None
        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )

    @property
    def database(self) -> None:
        """Returns the attending Database object."""
        return self._database

    @database.setter
    def database(self, database: Database) -> None:
        """Sets the database on the DAO object."""
        self._database = database

    def create(self, dto: DTO) -> Entity:
        """Adds an entity in the form of a data transfer object to the database.

        Args:
            dto (DTO): An entity data transfer object.

        Returns: a dto with the assigned id
        """
        cmd = self._dml.insert(dto)
        dto.id = self._database.insert(cmd.sql, cmd.args)
        msg = f"Inserted {self._entity.__name__}.{dto.id} - {dto.name} into the database."
        self._logger.debug(msg)
        return dto

    def read(self, id: int) -> Entity:
        """Obtains an entity DTO with the designated id.

        Args:
            id (int): The id for the entity.

        Returns a DTO
        """
        result = None
        cmd = self._dml.select(id)
        row = self._database.select(cmd.sql, cmd.args)
        if row is not None:
            result = self._row_to_dto(row)
        return result

    def read_by_name_mode(self, name: str, mode: str) -> DTO:
        """Obtains an entity DTO with the designated name and mode.
        Args:
            name (str): The name assigned to the entity.
            mode (str): Mode, i.e. 'dev', 'prod', or 'test'

        Returns a Data Transfer Object (DTO)
        """
        result = None
        cmd = self._dml.select_by_name_mode(name, mode)
        row = self._database.select(cmd.sql, cmd.args)
        if row is not None:
            result = self._row_to_dto(row)
        return result

    def read_all(self) -> Dict[int, DTO]:
        """Returns a dictionary of all entity data transfere objects of the in the database."""
        result = {}
        cmd = self._dml.select_all()
        rows = self._database.selectall(cmd.sql, cmd.args)
        if rows is not None:
            result = self._rows_to_dict(rows)
        return result

    def read_by_parent_id(self, parent_id: int) -> Dict[int, DTO]:
        """Returns a dictionary of entity data transfer objects with the designated parent id.

        Args:
            parent_id (int): Id for the parent object.
        """
        result = {}
        cmd = self._dml.select_by_parent_id(parent_id)
        rows = self._database.selectall(cmd.sql, cmd.args)
        if rows is not None:
            result = self._rows_to_dict(rows)
        return result

    def update(self, dto: DTO) -> int:
        """Performs an upsert of an existing entity DTO

        Args:
            dto (DTO): Data Transfer Object

        Returns number of rows effected.
        """
        if self.exists(dto.id):
            cmd = self._dml.update(dto)
            rows_affected = self._database.update(cmd.sql, cmd.args)
            msg = f"Updated {self._entity.__name__}.{dto.id} in the database."
            self._logger.debug(msg)
            return rows_affected
        else:
            msg = f"Unable to update {self._entity.__name__}.{dto.id}. Not found in the database. Try insert instead."
            self._logger.error(msg)
            raise mysql.connector.ProgrammingError(msg)

    def exists(self, id: int) -> bool:
        """Returns True if the entity with id exists in the database.

        Args:
            id (int): id for the entity
        """
        cmd = self._dml.exists(id)
        return self._database.exists(cmd.sql, cmd.args)

    def delete(self, id: int, persist=True) -> None:
        """Deletes a Entity from the registry, given an id.
        Args:
            id (int): The id for the entity to delete.

        """
        if self.exists(id):
            cmd = self._dml.delete(id)
            self._database.delete(cmd.sql, cmd.args)
            msg = f"Deleted {self._entity.__name__}.{id} from the database."
            self._logger.debug(msg)
        else:
            msg = f"Unable to delete {self._entity.__name__}.{id}. Not found in the database."
            self._logger.error(msg)
            raise mysql.connector.ProgrammingError(msg)

    def _rows_to_dict(self, results: List) -> Dict:
        """Converts the results to a dictionary of DTO objects."""
        results_dict = OrderedDict()
        for row in results:
            dto = self._row_to_dto(row)
            results_dict[dto.id] = dto
        return results_dict

    @abstractmethod
    def _row_to_dto(self, row: Tuple) -> DTO:
        """Converts a row from the Database into a Data Transfer Object."""


# ------------------------------------------------------------------------------------------------ #
#                                 DATAFRAME DATA ACCESS OBJECT                                     #
# ------------------------------------------------------------------------------------------------ #
class DataFrameDAO(DAO):
    def __init__(self, dml: DML) -> None:
        super().__init__(dml=dml)

    def _row_to_dto(self, row: Tuple) -> DataFrameDTO:
        try:
            return DataFrameDTO(
                id=row[0],
                name=row[1],
                description=row[2],
                datasource_id=row[3],
                mode=row[4],
                stage=row[5],
                size=row[6],
                nrows=row[7],
                ncols=row[8],
                nulls=row[9],
                pct_nulls=row[10],
                parent_id=row[11],
                created=row[12],
                modified=row[13],
            )
        except TypeError:
            msg = "No data matched the query."
            self._logger.info(msg)
            raise FileNotFoundError(msg)

        except IndexError as e:  # pragma: no cover
            msg = f"Index error in_row_to_dto method.\n{e}"
            self._logger.error(msg)
            raise IndexError(msg)


# ------------------------------------------------------------------------------------------------ #
#                                 DATASET DATA ACCESS OBJECT                                       #
# ------------------------------------------------------------------------------------------------ #
class DatasetDAO(DAO):
    def __init__(self, dml: DML) -> None:
        super().__init__(dml=dml)

    def _row_to_dto(self, row: Tuple) -> DataFrameDTO:
        try:
            return DatasetDTO(
                id=row[0],
                name=row[1],
                description=row[2],
                datasource_id=row[3],
                mode=row[4],
                stage=row[5],
                task_id=row[6],
                created=row[7],
                modified=row[8],
            )
        except TypeError:
            msg = "No data matched the query."
            self._logger.info(msg)
            raise FileNotFoundError(msg)

        except IndexError as e:  # pragma: no cover
            msg = f"Index error in_row_to_dto method.\n{e}"
            self._logger.error(msg)
            raise IndexError(msg)


# ------------------------------------------------------------------------------------------------ #
#                                 PROFILE DATA ACCESS OBJECT                                       #
# ------------------------------------------------------------------------------------------------ #
class ProfileDAO(DAO):
    """Profile for Tasks"""

    def __init__(self, dml: DML) -> None:
        super().__init__(dml=dml)

    def _row_to_dto(self, row: Tuple) -> ProfileDTO:
        try:
            return ProfileDTO(
                id=row[0],
                name=row[1],
                description=row[2],
                start=row[3],
                end=row[4],
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
        except TypeError:
            msg = "No data matched the query."
            self._logger.info(msg)
            raise FileNotFoundError(msg)

        except IndexError as e:  # pragma: no cover
            msg = f"Index error in_row_to_dto method.\n{e}"
            self._logger.error(msg)
            raise IndexError(msg)


# ------------------------------------------------------------------------------------------------ #
#                                TASK DATA ACCESS OBJECT                                           #
# ------------------------------------------------------------------------------------------------ #
class TaskDAO(DAO):
    """Task Data Access Object"""

    def __init__(self, dml: DML) -> None:
        super().__init__(dml=dml)

    def _row_to_dto(self, row: Tuple) -> TaskDTO:
        try:
            return TaskDTO(
                id=row[0],
                name=row[1],
                description=row[2],
                mode=row[3],
                state=row[4],
                job_id=row[5],
                created=row[6],
                modified=row[7],
            )
        except TypeError:
            msg = "No data matched the query."
            self._logger.info(msg)
            raise FileNotFoundError(msg)

        except IndexError as e:  # pragma: no cover
            msg = f"Index error in_row_to_dto method.\n{e}"
            self._logger.error(msg)
            raise IndexError(msg)


# ------------------------------------------------------------------------------------------------ #
#                                 JOB DATA ACCESS OBJECT                                           #
# ------------------------------------------------------------------------------------------------ #
class JobDAO(DAO):
    """Job Data Access Object"""

    def __init__(self, dml: DML) -> None:
        super().__init__(dml=dml)

    def _row_to_dto(self, row: Tuple) -> JobDTO:
        try:
            return JobDTO(
                id=row[0],
                name=row[1],
                description=row[2],
                mode=row[3],
                state=row[4],
                created=row[5],
                modified=row[6],
            )
        except TypeError:
            msg = "No data matched the query."
            self._logger.info(msg)
            raise FileNotFoundError(msg)

        except IndexError as e:  # pragma: no cover
            msg = f"Index error in_row_to_dto method.\n{e}"
            self._logger.error(msg)
            raise IndexError(msg)


# ------------------------------------------------------------------------------------------------ #
#                                 FILE DATA ACCESS OBJECT                                          #
# ------------------------------------------------------------------------------------------------ #
class FileDAO(DAO):
    """File Data Access Object"""

    def __init__(self, dml: DML) -> None:
        super().__init__(dml=dml)

    def _row_to_dto(self, row: Tuple) -> FileDTO:
        try:
            return FileDTO(
                id=row[0],
                name=row[1],
                description=row[2],
                datasource_id=row[3],
                mode=row[4],
                stage=row[5],
                uri=row[6],
                size=row[7],
                task_id=row[8],
                created=row[9],
                modified=row[10],
            )
        except TypeError:
            msg = "No data matched the query."
            self._logger.info(msg)
            raise FileNotFoundError(msg)

        except IndexError as e:  # pragma: no cover
            msg = f"Index error in_row_to_dto method.\n{e}"
            self._logger.error(msg)
            raise IndexError(msg)


# ------------------------------------------------------------------------------------------------ #
#                                 DATA SOURCE ACCESS OBJECT                                        #
# ------------------------------------------------------------------------------------------------ #
class DataSourceDAO(DAO):
    """File Data Access Object"""

    def __init__(self, dml: DML) -> None:
        super().__init__(dml=dml)

    def _row_to_dto(self, row: Tuple) -> FileDTO:
        try:
            return DataSourceDTO(
                id=row[0],
                name=row[1],
                description=row[2],
                website=row[3],
                mode=row[4],
                created=row[5],
                modified=row[6],
            )
        except TypeError:
            msg = "No data matched the query."
            self._logger.info(msg)
            raise FileNotFoundError(msg)

        except IndexError as e:  # pragma: no cover
            msg = f"Index error in_row_to_dto method.\n{e}"
            self._logger.error(msg)
            raise IndexError(msg)


# ------------------------------------------------------------------------------------------------ #
#                               DATA SOURCE URL ACCESS OBJECT                                      #
# ------------------------------------------------------------------------------------------------ #
class DataSourceURLDAO(DAO):
    """File Data Access Object"""

    def __init__(self, dml: DML) -> None:
        super().__init__(dml=dml)

    def _row_to_dto(self, row: Tuple) -> FileDTO:
        try:
            return DataSourceURLDTO(
                id=row[0],
                name=row[1],
                description=row[2],
                url=row[3],
                mode=row[4],
                datasource_id=row[5],
                created=row[6],
                modified=row[7],
            )
        except TypeError:
            msg = "No data matched the query."
            self._logger.info(msg)
            raise FileNotFoundError(msg)

        except IndexError as e:  # pragma: no cover
            msg = f"Index error in_row_to_dto method.\n{e}"
            self._logger.error(msg)
            raise IndexError(msg)
