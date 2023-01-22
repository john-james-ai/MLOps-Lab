#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/repo/dag.py                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday December 31st 2022 11:14:54 pm                                             #
# Modified   : Saturday January 21st 2023 07:56:36 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""DAG Repository"""

from recsys.core.entity.base import Entity
from .base import RepoABC
from .context import Context


# ------------------------------------------------------------------------------------------------ #
#                                           REPOSITORY                                             #
# ------------------------------------------------------------------------------------------------ #
class DAGRepo(RepoABC):
    """DAG aggregate repository."""

    def __init__(self, context: Context, *args, **kwargs) -> None:
        super().__init__()
        self._context = context
        self._dag_dao = self._context.get_dao("dag")
        self._task_dao = self._context.get_dao("task")
        self._oao = self._context.get_oao()

    def __len__(self) -> int:
        return len(self._dag_dao.read_all())

    def add(self, entity: Entity) -> Entity:
        """Adds an entity to the repository and returns the Entity with the id added."""
        dto = self._dag_dao.create(entity.as_dto())
        entity.id = dto.id

        for task in entity.tasks.values():
            task.dag = entity
            dto = self._task_dao.create(dto=task.as_dto())
            task.id = dto.id
            entity.update_task(task)

        self._oao.create(entity)
        return entity

    def get(self, id: str) -> Entity:
        "Returns an entity with the designated id"
        result = []
        dto = self._dag_dao.read(id)
        if dto:
            result = self._oao.read(dto.oid)
        return result

    def get_by_name(self, name: str) -> Entity:
        result = []
        dto = self._dag_dao.read_by_name(name)
        if dto:
            result = self._oao.read(dto.oid)
        return result

    def get_all(self) -> dict:
        entities = {}
        dtos = self._dag_dao.read_all()
        for dto in dtos.values():
            entity = self._oao.read(oid=dto.oid)
            if hasattr(entity, "id"):
                entities[entity.id] = entity
        return entities

    def update(self, entity: Entity) -> None:
        """Updates an entity in the database."""
        for task in entity.tasks.values():
            self._task_dao.update(task.as_dto())

        self._dag_dao.update(dto=entity.as_dto())  # Update dag metadata
        self._oao.update(entity)  # Persist dag in object storage

    def remove(self, id: str) -> None:
        """Removes an entity (and its children) from repository."""
        dto = self._dag_dao.read(id)
        dag = self._oao.read(dto.oid)
        for task in dag.tasks.values():
            self._task_dao.delete(task.id)

        self._dag_dao.delete(id)  # Delete dag metadata
        self._oao.delete(dag.oid)  # Delete dag from object storage

    def exists(self, id: str) -> bool:
        """Returns True if entity with id exists in the repository."""
        return self._dag_dao.exists(id)

    def print(self) -> None:
        """Prints the repository contents as a DataFrame."""
        dtos = self._dag_dao.read_all()
        for dto in dtos.values():
            dag = self._oao.read(dto.oid)
            print("\n\n")
            print(dag)
            print(120 * "=")
            print(50 * " ", "Tasks")
            print(120 * "_")
            dfs = dag.get_tasks()
            print(dfs)
