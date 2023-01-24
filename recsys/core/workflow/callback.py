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
# Loaded    : Friday January 20th 2023 08:48:19 pm                                                 #
# Modified   : Sunday January 22nd 2023 04:27:25 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Callback Module"""

from recsys.core.workflow.base import Callback, Process
from recsys.core.workflow.event import Event
from recsys.core.workflow import STATES


# ------------------------------------------------------------------------------------------------ #
#                                       JOB CALLBACK                                               #
# ------------------------------------------------------------------------------------------------ #
class DAGCallback(Callback):
    """DAG Callback is used by dag objects at creation, startup, failure and completion."""

    def __init__(self) -> None:
        super().__init__()

    # -------------------------------------------------------------------------------------------- #
    def on_create(self, process: Process) -> None:
        """Called at process (dag, task) creation

        Args:
            process (Process): Process object representation of the process being created.

        """
        event = Event(
            name="created_" + process.name,
            description="Created " + process.description,
            state=STATES[0],
            process_type=process.__class__.__name__,
            process_oid=process.oid,
            parent_oid=None,
        )
        self._events.dag().add(entity=process)
        self._events.event().add(event)

    # -------------------------------------------------------------------------------------------- #
    def on_load(self, process: Process) -> None:
        """Called at process (dag, task) creation

        May be overriden if this has utility.

        Args:
            process (Process): Process object representation of the process being created.

        """
        event = Event(
            name="loaded_" + process.name,
            description="Loaded " + process.description,
            state=STATES[1],
            process_type=process.__class__.__name__,
            process_oid=process.oid,
            parent_oid=None,
        )
        self._events.dag().update(entity=process)
        self._events.event().add(event)

    # -------------------------------------------------------------------------------------------- #
    def on_start(self, process: Process) -> None:
        """Called when a process (dag, task) begins execution.

        Args:
            process (Process): Process object representation of the process which has started.

        """
        event = Event(
            name="started_" + process.name,
            description="Started " + process.description,
            state=STATES[2],
            process_type=process.__class__.__name__,
            process_oid=process.oid,
            parent_oid=None,
        )
        self._events.dag().update(entity=process)
        self._events.event().add(event)

    # -------------------------------------------------------------------------------------------- #
    def on_fail(self, process: Process) -> None:
        """Called at process (dag, task) ends either successfully or otherwise.

        Args:
            process (Process): Process object representation of the process which has ended.

        """
        event = Event(
            name=process.name + "_failed.",
            description=process.description + "FAILED",
            state=STATES[3],
            process_type=process.__class__.__name__,
            process_oid=process.oid,
            parent_oid=None,
        )
        self._events.dag().update(entity=process)
        self._events.event().add(event)

    # -------------------------------------------------------------------------------------------- #
    def on_end(self, process: Process) -> None:
        """Called at process (dag, task) ends either successfully or otherwise.

        Args:
            process (Process): Process object representation of the process which has ended.

        """
        event = Event(
            name="ended_" + process.name,
            description="Ended " + process.description,
            state=STATES[4],
            process_type=process.__class__.__name__,
            process_oid=process.oid,
            parent_oid=None,
        )
        self._events.dag().update(entity=process)
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

    # -------------------------------------------------------------------------------------------- #
    def on_create(self, process: Process) -> None:
        """Called at process (dag, task) creation

        Args:
            process (Process): Process object representation of the process being created.

        """
        event = Event(
            name="created_" + process.name,
            description="Created " + process.description,
            process_type=process.__class__.__name__,
            process_oid=process.oid,
            parent_oid=process.parent.oid,
        )
        self._events.task().add(entity=process)
        self._events.event().add(event)

    # -------------------------------------------------------------------------------------------- #
    def on_load(self, process: Process) -> None:
        """Called at process (dag, task) creation

        May be overriden if this has utility.

        Args:
            process (Process): Process object representation of the process being created.

        """
        event = Event(
            name="loaded_" + process.name,
            description="Loaded " + process.description,
            process_type=process.__class__.__name__,
            process_oid=process.oid,
            parent_oid=process.parent.oid,
        )
        self._events.task().update(entity=process)
        self._events.event().add(event)

    # -------------------------------------------------------------------------------------------- #
    def on_start(self, process: Process) -> None:
        """Called when a process (dag, task) begins execution.

        Args:
            process (Process): Process object representation of the process which has started.

        """
        event = Event(
            name="started_" + process.name,
            description="Started " + process.description,
            process_type=process.__class__.__name__,
            process_oid=process.oid,
            parent_oid=process.parent.oid,
        )
        self._events.task().update(entity=process)
        self._events.event().add(event)

    # -------------------------------------------------------------------------------------------- #
    def on_fail(self, process: Process) -> None:
        """Called at process (dag, task) ends either successfully or otherwise.

        Args:
            process (Process): Process object representation of the process which has ended.

        """
        event = Event(
            name=process.name + "_failed.",
            description=process.description + "FAILED",
            process_type=process.__class__.__name__,
            process_oid=process.oid,
            parent_oid=process.parent.oid,
        )
        self._events.task().update(entity=process)
        self._events.event().add(event)

    # -------------------------------------------------------------------------------------------- #
    def on_end(self, process: Process) -> None:
        """Called at process (dag, task) ends either successfully or otherwise.

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
