#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /profile.py                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday December 1st 2022 09:33:01 am                                              #
# Modified   : Thursday December 1st 2022 10:17:18 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Profile Data Access Layer """
from datetime import datetime
from dataclasses import dataclass
from typing import Union, Tuple, List, Dict
import logging
from collections import OrderedDict
import pandas as pd

from .base import DAO, DTO, Sequel
from ..data.database import Database

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #

# ------------------------------------------------------------------------------------------------ #
#                                   DATASET DML                                                    #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class ProfileIdExists(Sequel):
    id: int
    sql: str = """SELECT COUNT(*) FROM profile WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class ProfileExists(Sequel):
    task_id: int
    step_id: int
    env: str
    sql: str = """SELECT COUNT(*) FROM profile WHERE task_id = ? AND step_id = ? AND env = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.task_id, self.step_id, self.env)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class FindProfile(Sequel):
    task_id: int
    step_id: int
    env: str
    args: tuple = ()
    sql: str = """SELECT * FROM profile WHERE task_id = ? AND step_id = ? AND env = ?;"""

    def __post_init__(self) -> None:
        self.args = (
            self.task_id,
            self.step_id,
            self.env,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectProfile(Sequel):
    id: int
    sql: str = """SELECT * FROM profile WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class ListProfiles(Sequel):
    sql: str = """SELECT * FROM profile;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class ListEnvProfiles(Sequel):
    env: str
    args: tuple = ()
    sql: str = """SELECT * FROM profile WHERE env = ?;"""

    def __post_init__(self) -> None:
        self.args = (self.env,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class InsertProfile(Sequel):
    task_id: int
    step_id: int
    env: str
    started: datetime
    stopped: datetime
    duration: int
    total_memory: int
    used_memory: int
    free_memory: int
    cpu_usage: float
    ram_usage: float
    bytes_sent: int
    bytes_received: int
    upload_speed: float
    download_speed: float

    sql: str = """INSERT INTO profile (task_id, step_id, env, started, stopped, duration, total_memory, used_memory, free_memory, cpu_usage, ram_usage, bytes_sent, bytes_received, upload_speed, download_speed) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.task_id,
            self.step_id,
            self.env,
            self.started,
            self.stopped,
            self.duration,
            self.total_memory,
            self.used_memory,
            self.free_memory,
            self.cpu_usage,
            self.ram_usage,
            self.bytes_sent,
            self.bytes_received,
            self.upload_speed,
            self.download_speed,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class CountProfiles(Sequel):
    sql: str = """SELECT COUNT(*) FROM profile;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DeleteProfile(Sequel):
    id: int
    sql: str = """DELETE FROM profile WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
#                                          PROFILE DTO                                             #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class ProfileDTO(DTO):
    id: int
    task_id: int
    step_id: int
    env: str
    started: datetime
    stopped: datetime
    duration: int
    total_memory: int
    used_memory: int
    free_memory: int
    cpu_usage: float
    ram_usage: float
    bytes_sent: int
    bytes_received: int
    upload_speed: float
    download_speed: float


# ------------------------------------------------------------------------------------------------ #
class ProfileDAO(DAO):
    def __init__(self, database: Database) -> None:
        self._database = database

    def __len__(self) -> int:
        """Returns the number of items in the registry."""
        query = CountProfiles()
        with self._database as db:
            return db.count(sql=query.sql, args=query.args)

    def add(self, dto: ProfileDTO) -> ProfileDTO:
        """Adds a profile to the database. If a duplicate is found, the version is bumped.

        Args:
            dto (ProfileDTO): A Profile Data Transfer Object
        """
        dsad = dto.as_dict()
        insert = InsertProfile(**dsad)
        with self._database as db:
            dto.id = db.insert(insert.sql, insert.args)
        return dto

    def get(self, id: int) -> ProfileDTO:
        """Retrieves dto metadata from the registry, given an id

        Args:
            id (int): The id for the Profile to retrieve.
        """
        result = None
        select = SelectProfile(id=id)
        with self._database as db:
            result = db.select(select.sql, select.args)
        if len(result) > 0:
            _, result = self._row_to_dto(result[0])
            return result
        else:
            msg = f"Profile id: {id} not found."
            logger.error(msg)
            raise FileNotFoundError

    def get_all(self, as_dict: bool = False) -> Union[pd.DataFrame, dict]:
        """Returns a Dataframe representation of the registry."""
        select = ListProfiles()
        with self._database as db:
            results = db.select(sql=select.sql, args=select.args)
            if as_dict:
                result = self._results_to_dict(results)
            else:
                result = self._results_to_df(results)

        return result

    def exists_id(self, id: int) -> bool:
        """Returns true if a dto with id exists

        Args:
            id (int): Profile id
        """
        exists = ProfileIdExists(id=id)
        with self._database as db:
            return db.exists(sql=exists.sql, args=exists.args)

    def exists(self, dto: ProfileDTO) -> bool:
        """Returns True if a Profile or Profiles match the above criteria.

        Args:
            dto (ProfileDTO): Required. Profile object
        """
        exists = ProfileExists(task_id=dto.task_id, step_id=dto.step_id, env=dto.env)
        with self._database as db:
            return db.exists(sql=exists.sql, args=exists.args)

    def find_profile(self, task_id: int, step_id: int, env: str) -> ProfileDTO:
        """Finds a Profile or Profiles that match the search criteria.

        Args:
            name (str): Required name of Profile.
            stage (str): Optional, one of 'input', 'interim', or 'final'.
        """

        find = FindProfile(task_id=task_id, step_id=step_id, env=env)
        with self._database as db:
            results = db.select(find.sql, find.args)
            return self._row_to_dto(results)

    def remove(self, id: int) -> None:
        """Deletes a Profile from the registry, given an id.

        Args:
            id (int): The id for the Profile to remove.
        """
        remove = DeleteProfile(id=id)
        with self._database as db:
            db.delete(sql=remove.sql, args=remove.args)

    def list_env_profiles(self, env: str) -> pd.DataFrame:
        cmd = ListEnvProfiles(env=env)
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
            return ProfileDTO(
                id=row[0],
                task_id=row[1],
                step_id=row[2],
                env=row[3],
                started=row[4],
                stopped=row[5],
                duration=row[6],
                total_memory=row[7],
                used_memory=row[8],
                free_memory=row[9],
                cpu_usage=row[10],
                ram_usage=row[11],
                bytes_sent=row[12],
                bytes_received=row[13],
                upload_speed=row[14],
                download_speed=row[15],
            )
        except IndexError as e:  # pragma: no cover
            msg = f"Index error in_row_to_dto method.\n{e}"
            logger.error(msg)
            raise IndexError(msg)
