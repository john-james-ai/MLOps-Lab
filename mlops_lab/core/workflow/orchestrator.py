#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /mlops_lab/core/workflow/orchestrator.py                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday January 12th 2023 09:09:55 pm                                              #
# Modified   : Tuesday January 24th 2023 08:13:46 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Orchestrator Module"""
from abc import ABC, abstractmethod
from typing import Any
import logging
import asyncio

from dependency_injector.wiring import Provide, inject

from mlops_lab.core.repo.uow import UnitOfWork
from mlops_lab.core.repo.container import WorkContainer
from mlops_lab.core.workflow.dag import DAG


# ------------------------------------------------------------------------------------------------ #
#                                ORCHESTRATOR ABSTRACT BASE CLASS                                  #
# ------------------------------------------------------------------------------------------------ #
class Orchestrator(ABC):
    """Orchestrator abstract base class

    Args:
        uow (UnitOfWork): Unit of Work class containing all entity repos.
    """

    @inject
    def __init__(self, uow: UnitOfWork = Provide[WorkContainer.unit]) -> None:
        self._uow = uow
        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )

    @property
    def dag(self) -> DAG:
        return self._dag

    @dag.setter
    def dag(self, dag: DAG) -> None:
        self._dag = dag
        self.on_load()

    def reset(self) -> None:
        self._dag = None

    @abstractmethod
    def run(self) -> Any:
        """Runs the dag."""

    def on_load(self) -> None:
        self._dag.on_load()
        msg = f"DAG {self._dag.name} has been loaded into the orchestrator."
        self._logger.info(msg)

    def on_start(self) -> None:
        self._dag.on_start()
        msg = f"DAG {self._dag.name} has started."
        self._logger.info(msg)

    def on_end(self) -> None:
        self._dag.on_end()
        msg = f"DAG {self._dag.name} has ended."
        self._logger.info(msg)

    def on_fail(self) -> None:
        self._dag.on_fail()
        msg = f"DAG {self._dag.name} failed."
        self._logger.info(msg)


# ------------------------------------------------------------------------------------------------ #
#                                SYNCHRONOUS ORCHESTRATOR CLASS                                    #
# ------------------------------------------------------------------------------------------------ #
class SyncOrchestrator(Orchestrator):
    """Executes a DAG in task sequence order."""

    def __init__(self, uow: UnitOfWork = Provide[WorkContainer.unit]) -> None:
        super().__init__(uow=uow)

    def run(self) -> Any:
        self.on_start()
        data = None

        with self._uow as uow:
            task = next(self._dag)
            try:
                task.on_start()
                data = task.run(uow=uow, data=data)
                task.on_end()
            except Exception:  # pragma: no cover
                task.on_fail()
                self.on_fail()
                raise

        self.on_end()
        return data


# ------------------------------------------------------------------------------------------------ #
#                                ASYNCHRONOUS ORCHESTRATOR CLASS                                   #
# ------------------------------------------------------------------------------------------------ #
class AsyncOrchestrator(Orchestrator):
    """Executes a DAG asyncronously."""

    def __init__(self, uow: UnitOfWork = Provide[WorkContainer.unit]) -> None:
        super().__init__(uow=uow)

    def run(self) -> Any:
        self.on_start()
        data = None

        async def run_dag():
            with self._uow as uow:
                for task in asyncio.as_completed(next(self._dag)):
                    try:
                        task.on_start()
                        await task.run(uow=uow, data=data)
                        task.on_end()
                    except Exception:
                        task.on_fail()
                        self.on_fail()
                        raise

        asyncio.run(run_dag())

        self.on_end()
        return data


# ------------------------------------------------------------------------------------------------ #
#                                PARALLEL ORCHESTRATOR CLASS                                       #
# ------------------------------------------------------------------------------------------------ #
class ParallelOrchestrator(Orchestrator):
    """Executes DAG tasks in parallel."""
