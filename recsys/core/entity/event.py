#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Eventname   : /recsys/core/entity/event.py                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 07:32:54 pm                                                #
# Modified   : Friday January 13th 2023 02:58:31 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Event Entity Module"""
from recsys.core.entity.base import Entity
from recsys.core.dal.dto import EventDTO


# ------------------------------------------------------------------------------------------------ #
#                                         FILE                                                     #
# ------------------------------------------------------------------------------------------------ #
class Event(Entity):
    """Event encapsulates a single event in the orchestrator.

    Args:
        job_id (int): Identifier for the job
        task_id (int): Identifier for the task.
        event (str): The description of the event

    """

    def __init__(
        self,
        name: str,
        job_oid: str,
        task_oid: str,
        description: str = None,
    ) -> None:
        super().__init__(name=name, description=description)
        self._job_oid = job_oid
        self._task_oid = task_oid
        self._validate()

    def __str__(self) -> str:
        return f"Event Id:{self._id}\n\tOid: {self._oid}\n\tName: {self._name}\n\tDescription: {self._description}\n\tJob_Oid: {self._job_oid}\n\tTask_Oid: {self._task_oid}\n\tCreated: {self._created}\n\tModified: {self._modified}\n\t"

    def __repr__(self) -> str:
        return f"{self._id},{self._oid},{self._name},{self._description},{self._job_oid},{self._task_oid},{self._created},{self._modified}"

    def __eq__(self, other) -> bool:
        """Compares two Event for equality.
        Event are considered equal solely if their underlying data are equal.

        Args:
            other (Event): The Event object to compare.
        """

        if isinstance(other, Event):
            return (
                self._name == other.name
                and self._description == other.description
                and self._job_oid == other.job_oid
                and self._task_oid == other.task_oid
            )
        else:
            return False

    # -------------------------------------------------------------------------------------------- #
    @property
    def job_oid(self) -> str:
        """Returns job_oid"""
        return self._job_oid

    # -------------------------------------------------------------------------------------------- #
    @property
    def task_oid(self) -> str:
        """Data processing stage in which the Event Component is created."""
        return self._task_oid

    # ------------------------------------------------------------------------------------------------ #
    def as_dto(self) -> EventDTO:
        return EventDTO(
            id=self._id,
            oid=self._oid,
            name=self._name,
            description=self._description,
            job_oid=self._job_oid,
            task_oid=self._task_oid,
            created=self._created,
            modified=self._modified,
        )
