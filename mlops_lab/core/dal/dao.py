#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /mlops_lab/core/dal/dao.py                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 06:27:36 am                                                #
# Modified   : Tuesday January 24th 2023 08:13:42 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Data Layer Services associated with Database construction."""
from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import Dict, Tuple, List
import logging

from mlops_lab.core.database.relational import Database
from mlops_lab.core.dal.dto import (
    DTO,
    DataFrameDTO,
    DatasetDTO,
    ProfileDTO,
    TaskDTO,
    DAGDTO,
    FileDTO,
    DataSourceDTO,
    DataSourceURLDTO,
    EventDTO,
)
from mlops_lab.core.dal.sql.base import DML
from mlops_lab.core.entity.base import Entity


# ------------------------------------------------------------------------------------------------ #
#                                    BASE DATA ACCESS OBJECT                                       #
# ------------------------------------------------------------------------------------------------ #
class DAO(ABC):
    """Data Access Object

    Provides access to the underlying relational database

    Args:
        database (Database): Relational database object.
        dml (DML, database: Database): The Data Manipulation Language for the database.
    """

    def __init__(self, dml: DML, database: Database) -> None:
        self._dml = dml
        self._entity = dml.entity
        self._database = database
        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )
        msg = f"Instantiated {self.__class__.__name__} at {id(self)} with database at {id(self._database)}."
        self._logger.debug(msg)

    def create(self, dto: DTO) -> Entity:
        """Adds an entity in the form of a data transfer object to the database.

        Args:
            dto (DTO): An entity data transfer object.

        Returns: a dto with the assigned id
        """
        cmd = self._dml.insert(dto)
        dto.id = self._database.insert(cmd.sql, cmd.args)
        msg = f"{self.__class__.__name__} inserted {self._entity.__name__}.{dto.id} - {dto.name} into database at {id(self._database)}."
        self._logger.debug(msg)
        return dto

    def read(self, id: int) -> Entity:
        """Obtains an entity DTO with the designated id.

        Args:
            id (int): The id for the entity.

        Returns a DTO
        """
        result = []
        cmd = self._dml.select(id)
        row = self._database.select(cmd.sql, cmd.args)
        if row is not None:
            result = self._row_to_dto(row)
        return result

    def read_by_name(self, name: str) -> DTO:
        """Obtains an entity DTO with the designated name.
        Args:
            name (str): The name assigned to the entity.
        Returns a Data Transfer Object (DTO)
        """
        result = []
        cmd = self._dml.select_by_name(name)
        row = self._database.select(cmd.sql, cmd.args)
        if row is not None:
            result = self._row_to_dto(row)
        return result

    def read_all(self) -> Dict[int, DTO]:
        """Returns a dictionary of all entity data transfere objects of the in the database."""
        result = {}
        cmd = self._dml.select_all()
        rows = self._database.select_all(cmd.sql, cmd.args)
        if rows is not None:
            result = self._rows_to_dict(rows)
        return result

    def read_by_parent_oid(self, parent_oid: str) -> Dict[int, DTO]:
        """Returns a dictionary of entity data transfer objects with the designated parent id.

        Args:
            parent_oid (int): Id for the parent object.
        """
        result = {}
        try:
            cmd = self._dml.select_by_parent_oid(parent_oid)
        except AttributeError:
            msg = "Read by parent is not supported for this entity."
            self._logger.error(msg)
            raise NotImplementedError
        rows = self._database.select_all(cmd.sql, cmd.args)
        if rows is not None:
            result = self._rows_to_dict(rows)
        return result

    def update(self, dto: DTO) -> int:
        """Performs an update to an existing entity DTO

        Args:
            dto (DTO): Data Transfer Object

        Returns number of rows effected.
        """
        rows_affected = None
        if self.exists(dto.id):
            cmd = self._dml.update(dto)
            rows_affected = self._database.update(cmd.sql, cmd.args)

        else:
            msg = f"{self.__class__.__name__} was unable to update {self._entity.__name__}.{dto.id}. Not found in {self._database.database}. Try insert instead."
            self._logger.error(msg)
            raise FileNotFoundError(msg)
        return rows_affected

    def exists(self, id: int) -> bool:
        """Returns True if the entity with id exists in the database.

        Args:
            id (int): id for the entity
        """
        cmd = self._dml.exists(id)
        result = self._database.exists(cmd.sql, cmd.args)
        return result

    def delete(self, id: int, persist=True) -> None:
        """Deletes a Entity from the registry, given an id.
        Args:
            id (int): The id for the entity to delete.

        """
        if self.exists(id):
            cmd = self._dml.delete(id)
            self._database.delete(cmd.sql, cmd.args)
        else:
            msg = f"{self.__class__.__name__}  was unable to delete {self._entity.__name__}.{id}. Not found in {self._database.database}."
            self._logger.error(msg)
            raise FileNotFoundError(msg)

    def load(self, filepath: str) -> None:
        """Loads data from a csv file into the associated table.

        Args:
            filepath (str): Path to the CSV file containing data to import
        """
        cmd = self._dml.load(filepath)
        self._database.load(cmd.sql, cmd.args)

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
    def __init__(self, dml: DML, database: Database) -> None:
        super().__init__(dml=dml, database=database)

    def _row_to_dto(self, row: Tuple) -> DataFrameDTO:
        try:
            return DataFrameDTO(
                id=row[0],
                oid=row[1],
                name=row[2],
                description=row[3],
                stage=row[4],
                size=row[5],
                nrows=row[6],
                ncols=row[7],
                nulls=row[8],
                pct_nulls=row[9],
                dataset_oid=row[10],
                created=row[11],
                modified=row[12],
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
    def __init__(self, dml: DML, database: Database) -> None:
        super().__init__(dml=dml, database=database)

    def _row_to_dto(self, row: Tuple) -> DataFrameDTO:
        try:
            return DatasetDTO(
                id=row[0],
                oid=row[1],
                name=row[2],
                description=row[3],
                datasource_oid=row[4],
                stage=row[5],
                task_oid=row[6],
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

    def __init__(self, dml: DML, database: Database) -> None:
        super().__init__(dml=dml, database=database)

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
                task_oid=row[24],
                created=row[25],
                modified=row[26],
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

    def __init__(self, dml: DML, database: Database) -> None:
        super().__init__(dml=dml, database=database)

    def _row_to_dto(self, row: Tuple) -> TaskDTO:
        try:
            return TaskDTO(
                id=row[0],
                oid=row[1],
                name=row[2],
                description=row[3],
                state=row[4],
                dag_oid=row[5],
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
class DAGDAO(DAO):
    """DAG Data Access Object"""

    def __init__(self, dml: DML, database: Database) -> None:
        super().__init__(dml=dml, database=database)

    def _row_to_dto(self, row: Tuple) -> DAGDTO:
        try:
            return DAGDTO(
                id=row[0],
                oid=row[1],
                name=row[2],
                description=row[3],
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

    def __init__(self, dml: DML, database: Database) -> None:
        super().__init__(dml=dml, database=database)

    def _row_to_dto(self, row: Tuple) -> FileDTO:
        try:
            return FileDTO(
                id=row[0],
                oid=row[1],
                name=row[2],
                description=row[3],
                datasource_oid=row[4],
                stage=row[5],
                uri=row[6],
                size=row[7],
                task_oid=row[8],
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

    def __init__(self, dml: DML, database: Database) -> None:
        super().__init__(dml=dml, database=database)

    def _row_to_dto(self, row: Tuple) -> FileDTO:
        try:
            return DataSourceDTO(
                id=row[0],
                oid=row[1],
                name=row[2],
                description=row[3],
                website=row[4],
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

    def __init__(self, dml: DML, database: Database) -> None:
        super().__init__(dml=dml, database=database)

    def _row_to_dto(self, row: Tuple) -> FileDTO:
        try:
            return DataSourceURLDTO(
                id=row[0],
                oid=row[1],
                name=row[2],
                description=row[3],
                url=row[4],
                datasource_oid=row[5],
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
#                                   EVENT ACCESS OBJECT                                            #
# ------------------------------------------------------------------------------------------------ #
class EventDAO(DAO):
    """File Data Access Object"""

    def __init__(self, dml: DML, database: Database) -> None:
        super().__init__(dml=dml, database=database)

    def _row_to_dto(self, row: Tuple) -> EventDTO:
        try:
            return EventDTO(
                id=row[0],
                oid=row[1],
                name=row[2],
                description=row[3],
                process_type=row[4],
                process_oid=row[5],
                parent_oid=row[6],
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
