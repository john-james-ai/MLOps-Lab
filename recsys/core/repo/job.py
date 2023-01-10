#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/repo/job.py                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday December 31st 2022 11:14:54 pm                                             #
# Modified   : Tuesday January 10th 2023 01:58:21 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Job Repository"""

from recsys.core.entity.base import Entity
from recsys.core.entity.job import Job, Task
from .base import RepoABC
from .context import Context


# ------------------------------------------------------------------------------------------------ #
#                                           REPOSITORY                                             #
# ------------------------------------------------------------------------------------------------ #
class JobRepo(RepoABC):
    """Job aggregate repository. """

    def __init__(self, context: Context) -> None:
        super().__init__()
        self._context = context
        self._job_dao = self._context.get_dao(Job)
        self._task_dao = self._context.get_dao(Task)

    def __len__(self) -> int:
        return len(self.get_all())

    def add(self, entity: Entity) -> Entity:
        """Adds an entity to the repository and returns the Entity with the id added."""
        dto = self._job_dao.create(entity.as_dto())
        entity.id = dto.id

        for name, task in entity.tasks.items():
            task.parent = entity
            dto = self._task_dao.create(dto=task.as_dto())
            task.id = dto.id
            entity.update_task(task)

        self._oao.create(entity)
        return entity

    def get(self, id: str) -> Entity:
        "Returns an entity with the designated id"
        return self._job_dao.read(id)

    def get_by_name_mode(self, name: str, mode: str = None) -> Entity:
        mode = mode or self._get_mode()
        return self._job_dao.read_by_name_mode(name, mode)

    def get_all(self) -> dict:
        return self._job_dao.read_all()

    def update(self, entity: Entity) -> None:
        """Updates an entity in the database."""
        self._job_dao.update(entity=entity, persist=True)
        self._job_dao.save()
        for _, task in entity.tasks.items():
            self._task_dao.update(entity=task, persist=False)
        self._task_dao.save()

    def remove(self, id: str) -> None:
        """Removes an entity (and its children) from repository."""
        entity = self.get(id)
        for _, task in entity.tasks.items():
            self._task_dao.delete(task.id, persist=False)
        self._task_dao.save()
        self._job_dao.delete(id)
        self._job_dao.save()

    def exists(self, id: str) -> bool:
        """Returns True if entity with id exists in the repository."""
        return self._job_dao.exists(id)

    def print(self) -> None:
        """Prints the repository contents as a DataFrame."""
        entities = self.get_all()
        for name, entity in entities.items():
            print("\n\n")
            print(entity)
            print(120 * "=")
            print(50 * " ", "Tasks")
            print(120 * "_")
            tasks = self._task_dao.read_by_parent_id(parent=entity)
            print(tasks)
