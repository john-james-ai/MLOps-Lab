#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /mlops_lab/core/factory/dag.py                                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday January 14th 2023 07:43:33 pm                                              #
# Modified   : Tuesday January 24th 2023 08:13:41 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
from mlops_lab.core.factory.base import Factory
from mlops_lab.core.workflow.dag import DAG, Task


# ------------------------------------------------------------------------------------------------ #
#                                      JOB FACTORY                                                 #
# ------------------------------------------------------------------------------------------------ #
class DAGFactory(Factory):
    def __init__(self) -> None:
        super().__init__()
        self._instance = None

    def __call__(self, config: dict) -> DAG:
        if not self._instance:
            self._instance = self._build_entity(config)
        return self._instance

    def _build_entity(self, config: dict) -> DAG:
        dag = DAG(**config)
        return dag


# ------------------------------------------------------------------------------------------------ #
#                                   TASK FACTORY                                                   #
# ------------------------------------------------------------------------------------------------ #
class TaskFactory(Factory):
    def __init__(self) -> None:
        super().__init__()
        self._instance = None

    def __call__(self, config: dict) -> Task:
        if not self._instance:
            self._instance = self._build_entity(config)
        return self._instance

    def _build_entity(self, config: dict) -> Task:
        return Task(**config)
