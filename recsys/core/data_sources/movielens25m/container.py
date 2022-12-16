#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/data/movielens25m/container.py                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 12th 2022 12:32:54 am                                               #
# Modified   : Friday December 16th 2022 12:26:01 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Workflow Container Module"""
from dependency_injector import containers, providers  # pragma: no cover

from recsys.core.workflow.operator import (
    DownloadExtractor,
    NullOperator,
    Sampler,
)


# ------------------------------------------------------------------------------------------------ #
#                                    EXTRACT CONTAINER                                             #
# ------------------------------------------------------------------------------------------------ #
class ExtractContainer(containers.DeclarativeContainer):

    config = providers.Configuration()

    operator = providers.Factory(
        DownloadExtractor,
        url=config.operator.url,
        destination=config.operator.destination,
    )

    task = providers.Factory(
        name=config.task.name,
        description=config.task.description,
        operator=operator,
        input=config.input,
        output=config.output,
        force=config.force,
    )


# ------------------------------------------------------------------------------------------------ #
#                                   TRANSFORM CONTAINER                                            #
# ------------------------------------------------------------------------------------------------ #
class TransformContainer(containers.DeclarativeContainer):

    config = providers.Configuration()

    operator = providers.Factory(
        NullOperator
    )

    task = providers.Factory(
        name=config.task.name,
        description=config.task.description,
        operator=operator,
        input=config.task.input,
        output=config.task.output,
        force=config.task.force,
    )


# ------------------------------------------------------------------------------------------------ #
#                                       LOAD CONTAINER                                             #
# ------------------------------------------------------------------------------------------------ #
class LoadContainer(containers.DeclarativeContainer):

    config = providers.Configuration()

    operator = providers.Factory(
        Sampler,
        cluster=config.operator.cluster,
        cluster_by=config.operator.cluster_by,
        replace=config.operator.replace,
        shuffle=config.operator.shuffle,
        frac=config.operator.frac.prod,
        random_state=config.operator.random_state
    )

    task = providers.Factory(
        name=config.name,
        description=config.description,
        operator=operator,
        input=config.input,
        output=config.output,
    )


class MovieLens(containers.DeclarativeContainer):

    config = providers.Configuration(yaml_files=[".config.yml"])

    extract = providers.Container(ExtractContainer, config=config.job.tasks.extract)

    transform = providers.Container(TransformContainer, config=config.job.tasks.transform)

    load_prod = providers.Container(LoadContainer, config=config.job.tasks.load_prod)

    load_dev = providers.Container(LoadContainer, config=config.job.tasks.load_dev)

    load_test = providers.Container(LoadContainer, config=config.job.tasks.load_test)
