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
# Modified   : Tuesday January 10th 2023 06:50:41 pm                                               #
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

    def __init__(self, context: Context, *args, **kwargs) -> None:
        super().__init__()
        self._context = context
        self._job_dao = self._context.get_dao(Job)
        self._task_dao = self._context.get_dao(Task)
        self._oao = self._context.get_oao()

    def __len__(self) -> int:
        return len(self._job_dao.read_all())

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
        result = []
        dto = self._job_dao.read(id)
        if dto:
            result = self._oao.read(dto.oid)
        return result

    def get_by_name_mode(self, name: str, mode: str = None) -> Entity:
        result = []
        mode = mode or self._get_mode()
        dto = self._job_dao.read_by_name_mode(name, mode)
        if dto:
            result = self._oao.read(dto.oid)
        return result

    def update(self, entity: Entity) -> None:
        """Updates an entity in the database."""
        for task in entity.tasks.values():
            self._task_dao.update(task.as_dto())

        self._job_dao.update(dto=entity.as_dto())   # Update job metadata
        self._oao.update(entity)  # Persist job in object storage

    def remove(self, id: str) -> None:
        """Removes an entity (and its children) from repository."""
        dto = self._job_dao.read(id)
        job = self._oao.read(dto.oid)
        for task in job.tasks.values():
            self._task_dao.delete(task.id)

        self._job_dao.delete(id)   # Delete job metadata
        self._oao.delete(job.oid)  # Delete job from object storage

    def exists(self, id: str) -> bool:
        """Returns True if entity with id exists in the repository."""
        return self._job_dao.exists(id)

    def print(self) -> None:
        """Prints the repository contents as a DataFrame."""
        dtos = self._job_dao.read_all()
        for dto in dtos.values():
            job = self._oao.read(dto.oid)
            print("\n\n")
            print(job)
            print(120 * "=")
            print(50 * " ", "Tasks")
            print(120 * "_")
            dfs = job.get_tasks()
            print(dfs)
