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
# Created    : Friday December 16th 2022 12:28:07 am                                               #
# Modified   : Sunday December 25th 2022 11:13:24 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Job Repository Module."""
import pandas as pd

from recsys.core.entity.base import Entity
from .base import Repo, Context
from recsys.core.workflow.job import Job


# ------------------------------------------------------------------------------------------------ #
#                                    DATASET REPOSITORY                                            #
# ------------------------------------------------------------------------------------------------ #
class JobRepo(Repo):
    """Repository base class"""

    def __init__(self, context: Context) -> None:
        super().__init__(context=context)

    def __len__(self) -> int:
        return len(self._context.job)

    def add(self, entity: Entity) -> Entity:
        """Adds an entity to the repository and returns the Entity with the id added."""
        if isinstance(entity, Job):
            entity = self._context.job.create(entity)
        else:
            entity = self._context.task.create(entity)

        if entity.is_composite:
            if len(entity.components) > 0:
                for component in entity.components:
                    self.add(component)
        return entity

    def get(self, id: str) -> Entity:
        "Returns an entity with the designated id"
        return self._context.job.read(id)

    def get_by_name(self, name: str) -> Entity:
        return self._context.job.read_by_name(name)

    def get_all(self) -> dict:
        return self._context.job.read_all()

    def update(self, entity: Entity) -> None:
        """Updates an entity in the database."""
        if isinstance(entity, Job):
            self._context.job.update(entity)
        else:
            self._context.task.update(entity)

        if entity.is_composite:
            if len(entity.components) > 0:
                for component in entity.components:
                    self.update(component)

    def remove(self, id: str) -> None:
        """Removes an entity (and its children) from repository."""
        entity = self.get(id)
        if entity.is_composite:
            if len(entity.components) > 0:
                for component in entity.components:
                    self.remove(component.id)
            else:
                self._context.job.delete(id)
        else:
            self._context.task.delete(id)

    def exists(self, id: str) -> bool:
        """Returns True if entity with id exists in the repository."""
        return self._context.job.exists(id)

    def print(self) -> None:
        """Prints the repository contents as a DataFrame."""
        jobs = self._context.job.read_all()
        for id, job in jobs.items():
            print("\n")
            df = pd.DataFrame.from_dict(job, orient="columns")
            print(df)
            if job.is_composite:
                if len(job.components) > 0:
                    for component in job.components:
                        d = component.as_dict()
                        task = pd.Dataframe.from_dict(d, orient='columns')
                        print(task)
