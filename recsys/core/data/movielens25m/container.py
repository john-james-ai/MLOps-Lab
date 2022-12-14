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
# Modified   : Tuesday December 13th 2022 04:04:21 am                                              #
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
        input=config.task.input,
        output=config.task.output,
        force=config.task.force,
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

    # -------------------------------------------------------------------------------------------- #
    #                                 PRODUCTION WORKSPACE                                         #
    # -------------------------------------------------------------------------------------------- #

    operator_load_production = providers.Factory(
        Sampler,
        cluster=config.operator.cluster,
        cluster_by=config.operator.cluster_by,
        replace=config.operator.replace,
        shuffle=config.operator.shuffle,
        frac=config.operator.frac.prod,
        random_state=config.operator.random_state
    )

    task_load_production = providers.Factory(
        name=config.task.prod.name,
        description=config.task.prod.description,
        operator=operator_load_production,
        input=config.task.prod.input,
        output=config.task.prod.output,
    )

    # -------------------------------------------------------------------------------------------- #
    #                                 DEVELOPMENT WORKSPACE                                        #
    # -------------------------------------------------------------------------------------------- #
    operator_load_development = providers.Factory(
        Sampler,
        cluster=config.operator.cluster,
        cluster_by=config.operator.cluster_by,
        replace=config.operator.replace,
        shuffle=config.operator.shuffle,
        frac=config.operator.frac.dev,
        random_state=config.operator.random_state,
    )

    task_load_development = providers.Factory(
        name=config.task.dev.name,
        description=config.task.dev.description,
        operator=operator_load_development,
        input=config.task.dev.input,
        output=config.task.dev.output,
    )

    # -------------------------------------------------------------------------------------------- #
    #                                    TEST WORKSPACE                                            #
    # -------------------------------------------------------------------------------------------- #

    operator_load_test = providers.Factory(
        Sampler,
        cluster=config.operator.cluster,
        cluster_by=config.operator.cluster_by,
        replace=config.operator.replace,
        shuffle=config.operator.shuffle,
        frac=config.operator.frac.test,
        random_state=config.operator.random_state,
    )

    task_load_test = providers.Factory(
        name=config.task.test.name,
        description=config.task.test.description,
        operator=operator_load_test,
        input=config.task.test.input,
        output=config.task.test.output,
    )


class MovieLens(containers.DeclarativeContainer):

    config = providers.Configuration(yaml_files=[".config.yml"])

    extract = providers.Container(ExtractContainer, config=config.pipeline.dependencies.extract)

    transform = providers.Container(TransformContainer, config=config.pipeline.dependencies.transform)

    load_prod = providers.Container(LoadContainer, config=config.pipeline.dependencies.load)

    load_dev = providers.Container(LoadContainer, config=config.pipeline.dependencies.load)

    load_test = providers.Container(LoadContainer, config=config.pipeline.dependencies.load)
