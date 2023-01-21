#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/workflow/callback.py                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday January 20th 2023 08:48:19 pm                                                #
# Modified   : Saturday January 21st 2023 04:45:23 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Event Callback Module"""
from abc import abstractmethod


from recsys.core.workflow.base import Callback, Process
from recsys.core.workflow.event import Event


# ------------------------------------------------------------------------------------------------ #
#                                       JOB CALLBACK                                               #
# ------------------------------------------------------------------------------------------------ #
class JobCallback(Callback):
    """Job Callback is used by job objects at creation, startup, failure and completion."""

    def __init__(self) -> None:
        super().__init__()

    def on_create(self, process: Process) -> None:
        """Called at process (job, task) creation

        May be overriden if this has utility.

        Args:
            process (Process): Process object representation of the process being created.

        """
        event = Event(
            name="created_" + process.name,
            description="Created " + process.description,
            process_type=process.__class__.__name__,
            process_oid=process.oid,
        )
        self._events.job().add(entity=process)
        self._events.event().add(event)

    @abstractmethod
    def on_start(self, process: Process) -> None:
        """Called when a process (job, task) begins execution.

        Args:
            process (Process): Process object representation of the process which has started.

        """
        event = Event(
            name="started_" + process.name,
            description="Started " + process.description,
            process_type=process.__class__.__name__,
            process_oid=process.oid,
        )
        self._events.job().update(entity=process)
        self._events.event().add(event)

    @abstractmethod
    def on_fail(self, process: Process) -> None:
        """Called at process (job, task) ends either successfully or otherwise.

        Args:
            process (Process): Process object representation of the process which has ended.

        """
        event = Event(
            name=process.name + "_failed.",
            description=process.description + "FAILED",
            process_type=process.__class__.__name__,
            process_oid=process.oid,
        )
        self._events.job().update(entity=process)
        self._events.event().add(event)

    @abstractmethod
    def on_end(self, process: Process) -> None:
        """Called at process (job, task) ends either successfully or otherwise.

        Args:
            process (Process): Process object representation of the process which has ended.

        """
        event = Event(
            name="ended_" + process.name,
            description="Ended " + process.description,
            process_type=process.__class__.__name__,
            process_oid=process.oid,
            parent_oid=None,
        )
        self._events.job().update(entity=process)
        self._events.event().add(event)


# ------------------------------------------------------------------------------------------------ #
#                                       TASK CALLBACK                                              #
# ------------------------------------------------------------------------------------------------ #
class TaskCallback(Callback):
    """Task Callback is used by Task objects at creation, startup, failure and completion.

    Args:
        events (DeclarativeContainer): Container of event repositories.
    """

    def __init__(self) -> None:
        super().__init__()

    def on_create(self, process: Process) -> None:
        """Called at process (job, task) creation

        May be overriden if this has utility.

        Args:
            process (Process): Process object representation of the process being created.

        """
        event = Event(
            name="created_" + process.name,
            description="Created " + process.description,
            process_type=process.__class__.__name__,
            process_oid=process.oid,
        )
        self._events.task().add(entity=process)
        self._events.event().add(event)

    @abstractmethod
    def on_start(self, process: Process) -> None:
        """Called when a process (job, task) begins execution.

        Args:
            process (Process): Process object representation of the process which has started.

        """
        event = Event(
            name="started_" + process.name,
            description="Started " + process.description,
            process_type=process.__class__.__name__,
            process_oid=process.oid,
        )
        self._events.task().update(entity=process)
        self._events.event().add(event)

    @abstractmethod
    def on_fail(self, process: Process) -> None:
        """Called at process (job, task) ends either successfully or otherwise.

        Args:
            process (Process): Process object representation of the process which has ended.

        """
        event = Event(
            name=process.name + "_failed.",
            description=process.description + "FAILED",
            process_type=process.__class__.__name__,
            process_oid=process.oid,
        )
        self._events.task().update(entity=process)
        self._events.event().add(event)

    @abstractmethod
    def on_end(self, process: Process) -> None:
        """Called at process (job, task) ends either successfully or otherwise.

        Args:
            process (Process): Process object representation of the process which has ended.

        """
        event = Event(
            name="ended_" + process.name,
            description="Ended " + process.description,
            process_type=process.__class__.__name__,
            process_oid=process.oid,
            parent_oid=process.parent.oid,
        )
        self._events.task().update(entity=process)
        self._events.event().add(event)
