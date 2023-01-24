#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/workflow/event.py                                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday January 20th 2023 10:23:12 pm                                                #
# Modified   : Sunday January 22nd 2023 04:32:30 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Event Module"""

from recsys.core.dal.dto import EventDTO


# ------------------------------------------------------------------------------------------------ #
class Event:
    """Event encapsulates a single execution of a process (dag or task).

    Args:
        id (int): Database assigned unique identifier for an event
        name (str): Name for the event or process
        description (str): The description for event or process
        process_oid (int): The id for the dag or task for which the event is published. Required.
        parent_oid (int): The id for the parent dag from which a task has spawned. None allowed.
        state (str): One of the valid states in which a process may assume, defined in the application init module. Required.

    """

    def __init__(
        self,
        process_type: type,
        process_oid: int,
        state: str,
        id: int = None,
        name: str = None,
        description: str = None,
        parent_oid: str = None,
    ) -> None:
        self._id = id
        self._name = name
        self._description = description
        self._process_type = process_type
        self._process_oid = process_oid
        self._parent_oid = parent_oid
        self._state = state

    def __str__(self) -> str:
        return f"Event Id:{self._id}\n\tProcess Type: {self._process_type}\n\tProcess Id: {self._process_oid}\n\tName: {self._name}\n\tDescription: {self._description}\n\tState: {self._state}\n\tParent Id: {self._parent_oid}"

    def __repr__(self) -> str:
        return f"{self._id},{self._process_type}, ,{self._process_oid}, {self._name},{self._description}, {self._state}, {self._parent_oid}"

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
                and self._process_oid == other.process_oid
                and self._process_type == other.process_type
                and self._parent_oid == other.parent_oid
            )
        else:
            return False

    # -------------------------------------------------------------------------------------------- #
    @property
    def name(self) -> str:
        return self._name

    # -------------------------------------------------------------------------------------------- #
    @property
    def description(self) -> str:
        return self._description

    # -------------------------------------------------------------------------------------------- #
    @property
    def process_type(self) -> str:
        return self._process_type

    # -------------------------------------------------------------------------------------------- #
    @property
    def process_oid(self) -> str:
        return self._process_oid

    # -------------------------------------------------------------------------------------------- #
    @property
    def parent_oid(self) -> str:
        return self._parent_oid

    # -------------------------------------------------------------------------------------------- #
    def as_dto(self) -> EventDTO:
        return EventDTO(
            id=self._id,
            name=self._name,
            description=self._description,
            process_type=self._process_type,
            process_oid=self._process_oid,
            parent_oid=self._parent_oid,
            created=self._created,
            modified=self._modified,
        )
