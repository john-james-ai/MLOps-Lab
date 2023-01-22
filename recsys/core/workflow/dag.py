#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/workflow/dag.py                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday January 21st 2023 05:09:50 am                                              #
# Modified   : Saturday January 21st 2023 07:50:18 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/workflow/process.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 07:32:54 pm                                                #
# Modified   : Saturday January 21st 2023 08:50:57 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Process Module"""
import pandas as pd
from datetime import datetime
from collections import OrderedDict


from dependency_injector.wiring import Provide, inject

from recsys.core.workflow.base import Process
from recsys.core.workflow.callback import Callback
from recsys.core.workflow.container import CallbackContainer
from recsys.core.workflow.operator.base import Operator
from recsys.core.dal.dao import DTO, DAGDTO, TaskDTO
from recsys import STATES


# ------------------------------------------------------------------------------------------------ #
#                                          JOB                                                     #
# ------------------------------------------------------------------------------------------------ #
class DAG(Process):
    """Directed Acyclic Graph of Tasks to be executed within a sync, async, or parallel orchestration context.

    Args:
        name (str): DAG name
        description (str): DAG Description
    """

    @inject
    def __init__(
        self,
        name: str,
        description: str = None,
        callback: Callback = Provide[CallbackContainer.dag],
    ) -> None:
        super().__init__(name=name, description=description)
        self._callback = callback()

        self._tasks = OrderedDict()
        self._task_no = 0
        self._state = STATES[0]
        self._is_composite = True

    # -------------------------------------------------------------------------------------------- #
    def __str__(self) -> str:
        return f"DAG Id: {self._id}\n\tName: {self._name}\n\tDescription: {self._description}\n\tState: {self._state}\n\tCreated: {self._created}\n\tModified: {self._modified}"

    # -------------------------------------------------------------------------------------------- #
    def __repr__(self) -> str:
        return f"{self._id}, {self._name}, {self._description}, {self._state}, {self._created}, {self._modified}"

    # -------------------------------------------------------------------------------------------- #
    def __eq__(self, other: Process) -> bool:
        if self.__class__.__name__ == other.__class__.__name__:
            return (
                self.is_composite == other.is_composite
                and self.name == other.name
                and self.description == other.description
                and self.state == other.state
            )
        else:
            return False

    # -------------------------------------------------------------------------------------------- #
    def __len__(self) -> int:
        return len(self._tasks)

    # -------------------------------------------------------------------------------------------- #
    def __aiter__(self):
        return self

    # -------------------------------------------------------------------------------------------- #
    async def __anext__(self):
        n_tasks = len(self._tasks)
        if self._task_no >= n_tasks - 1:
            raise StopIteration
        task = self._tasks.popitem()
        self._task_no += 1
        return task

    # -------------------------------------------------------------------------------------------- #
    @property
    def is_composite(self) -> str:
        return self._is_composite

    # -------------------------------------------------------------------------------------------- #
    @property
    def tasks(self) -> dict:
        return self._tasks

    # -------------------------------------------------------------------------------------------- #
    def add_task(self, task: Process) -> None:
        task.dag = self
        self._tasks[task.name] = task
        self._modified = datetime.now()
        self._logger.debug(f"just added task {task.name} to {self._name}")

    # -------------------------------------------------------------------------------------------- #
    def get_task(self, name: str = None) -> None:
        try:
            return self._tasks[name]
        except KeyError:
            msg = f"DAG {self._name} has no task with name = {name}."
            self._logger.error(msg)
            raise FileNotFoundError(msg)

    # -------------------------------------------------------------------------------------------- #
    def show_tasks(self) -> pd.DataFrame:
        d = {}
        for name, task in self._tasks.items():
            d[name] = task.as_dto().as_dict()
        df = pd.DataFrame.from_dict(data=d, orient="index")
        return df

    # -------------------------------------------------------------------------------------------- #
    def update_task(self, task: Process) -> None:
        if task.name in self._tasks.keys():
            task.dag = self
            self._tasks[task.name] = task
            self._modified = datetime.now()
            self._logger.debug(f"just updated task {task.name} in {self._name}")
        else:
            msg = f"Task {task.name} does not exist in dag {self._name}. Did you mean add_task?"
            self._logger.error(msg)
            raise KeyError(msg)

    # -------------------------------------------------------------------------------------------- #
    def remove_task(self, name: str) -> None:
        try:
            del self._tasks[name]
            self._modified = datetime.now()
        except KeyError:
            msg = f"Unable to delete task. Task {name} does not exist in dag {self._name}."
            self._logger.error(msg)
            raise KeyError(msg)

    # -------------------------------------------------------------------------------------------- #
    def as_dto(self) -> DTO:

        dto = DAGDTO(
            id=self._id,
            oid=self._oid,
            name=self._name,
            description=self._description,
            state=self._state,
            created=self._created,
            modified=self._modified,
        )
        return dto


# ------------------------------------------------------------------------------------------------ #
#                                          TASK                                                    #
# ------------------------------------------------------------------------------------------------ #
class Task(Process):
    """Task is a pipeline step or operation in execution.

    Args:
        name (str): Short, yet descriptive lowercase name for Task object.
        description (str): Describes the Task object. Default's to dag's description if None.
        operator (Operator): An instance of an operator object.
        dag (DAG): The dag DAG instance.

    """

    def __init__(
        self,
        name: str,
        operator: Operator = None,
        description: str = None,
        callback: Callback = Provide[CallbackContainer.task],
    ) -> None:
        super().__init__(name=name, description=description)

        self._callback = callback()
        self._operator = operator
        self._dag = None
        self._is_composite = False
        self._state = STATES[0]

    def __str__(self) -> str:
        return f"Task Id: {self._id}\n\tName: {self._name}\n\tDescription: {self._description}\n\tState: {self._state}\n\tCreated: {self._created}\n\tModified: {self._modified}"

    def __repr__(self) -> str:
        return f"{self._id}, {self._name}, {self._description}, {self._state}, {self._created}, {self._modified}"

    def __eq__(self, other) -> bool:
        """Compares two DAG for equality.
        DAG are considered equal solely if their underlying data are equal.

        Args:
            other (Task): The Task object to compare.
        """

        if isinstance(other, Task):
            return (
                self._name == other.name
                and self._operator == other._operator
                and self._description == other.description
                and self._dag == other.dag
            )
        else:
            return False

    def __len__(self) -> int:
        return 1

    # -------------------------------------------------------------------------------------------- #
    @property
    def is_composite(self) -> str:
        return self._is_composite

    # -------------------------------------------------------------------------------------------- #
    @property
    def dag(self) -> DAG:
        return self._dag

    # -------------------------------------------------------------------------------------------- #
    @dag.setter
    def dag(self, dag: DAG) -> None:
        self._dag = dag

    # -------------------------------------------------------------------------------------------- #
    @property
    def operator(self) -> Operator:
        return self._operator

    # -------------------------------------------------------------------------------------------- #
    @operator.setter
    def operator(self, operator: Operator) -> None:
        self._operator = operator

    # -------------------------------------------------------------------------------------------- #
    def as_dto(self) -> TaskDTO:
        return TaskDTO(
            id=self._id,
            oid=self._oid,
            name=self._name,
            description=self._description,
            state=self._state,
            dag_oid=self._dag.oid,
            created=self._created,
            modified=self._modified,
        )
