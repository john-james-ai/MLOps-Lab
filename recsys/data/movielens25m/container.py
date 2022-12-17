#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/data/movielens25m/container.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 12th 2022 12:32:54 am                                               #
# Modified   : Friday December 16th 2022 08:41:17 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Workflow Container Module"""
from dependency_injector import containers, providers  # pragma: no cover
from recsys.core.entity.dataset import Dataset

from recsys.core.entity.job import Job
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

    genome_scores = providers.Factory(
        Dataset,
        name=config.output[0].name,
        description=config.output[0].description,
        datasource=config.output[0].datasource,
        workspace=config.output[0].workspace,
        stage=config.output[0].stage,
        filename=config.output[0].filename,
    )
    genome_tags = providers.Factory(
        Dataset,
        name=config.output[1].name,
        description=config.output[1].description,
        datasource=config.output[1].datasource,
        workspace=config.output[1].workspace,
        stage=config.output[1].stage,
        filename=config.output[1].filename,
    )

    links = providers.Factory(
        Dataset,
        name=config.output[2].name,
        description=config.output[2].description,
        datasource=config.output[2].datasource,
        workspace=config.output[2].workspace,
        stage=config.output[2].stage,
        filename=config.output[2].filename,
    )

    movies = providers.Factory(
        Dataset,
        name=config.output[3].name,
        description=config.output[3].description,
        datasource=config.output[3].datasource,
        workspace=config.output[3].workspace,
        stage=config.output[3].stage,
        filename=config.output[3].filename,
    )

    ratings = providers.Factory(
        Dataset,
        name=config.output[4].name,
        description=config.output[4].description,
        datasource=config.output[4].datasource,
        workspace=config.output[4].workspace,
        stage=config.output[4].stage,
        filename=config.output[4].filename,
    )

    tags = providers.Factory(
        Dataset,
        name=config.output[5].name,
        description=config.output[5].description,
        datasource=config.output[5].datasource,
        workspace=config.output[5].workspace,
        stage=config.output[5].stage,
        filename=config.output[5].filename,
    )

    task = providers.Factory(
        name=config.task.name,
        description=config.task.description,
        operator=operator,
        input=None,
        output=[genome_scores, genome_tags, links, movies, ratings, tags],
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

    ratings = providers.Factory(
        Dataset,
        name=config.output[0].name,
        description=config.output[0].description,
        datasource=config.output[0].datasource,
        workspace=config.output[0].workspace,
        stage=config.output[0].stage,
        filename=config.output[0].filename,
    )

    task = providers.Factory(
        name=config.task.name,
        description=config.task.description,
        operator=operator,
        input=config.task.input,
        output=ratings,
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
    ratings = providers.Factory(
        Dataset,
        name=config.output[0].name,
        description=config.output[0].description,
        datasource=config.output[0].datasource,
        workspace=config.output[0].workspace,
        stage=config.output[0].stage,
        filename=config.output[0].filename,
    )

    task = providers.Factory(
        name=config.task.name,
        description=config.task.description,
        operator=operator,
        input=config.task.input,
        output=ratings,
        force=config.task.force,
    )


# ------------------------------------------------------------------------------------------------ #
#                                      JOB CONTAINER                                               #
# ------------------------------------------------------------------------------------------------ #
class JobContainer(containers.DeclarativeContainer):

    config = providers.Configuration()

    job = providers.Factory(
        Job,
        name=config.name,
        description=config.description,
        workspace=config.workspace,
    )


class MovieLens25M(containers.DeclarativeContainer):

    config = providers.Configuration(yaml_files=[".config.yml"])

    job = providers.Container(JobContainer, config=config.job)

    extract = providers.Container(ExtractContainer, config=config.job.tasks.extract)

    transform = providers.Container(TransformContainer, config=config.job.tasks.transform)

    load_prod = providers.Container(LoadContainer, config=config.job.tasks.load_prod)

    load_dev = providers.Container(LoadContainer, config=config.job.tasks.load_dev)

    load_test = providers.Container(LoadContainer, config=config.job.tasks.load_test)
