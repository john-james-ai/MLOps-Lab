#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/workflow/container.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 12th 2022 12:32:54 am                                               #
# Modified   : Monday December 12th 2022 02:50:51 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Workflow Container Module"""
from dependency_injector import containers, providers  # pragma: no cover

from recsys.core.workflow.operator import DownloadExtractor, NullOperator, Sampler, TrainTestSplit, DataCenterizer, MeanAggregator
# ------------------------------------------------------------------------------------------------ #


class DataStagingContainer(containers.DeclarativeContainer):

    config = providers.Configuration()

    downloader = providers.Factory(
        DownloadExtractor,
        url=config.download.params.operator.url,
        destination=config.download.params.operator.destination,
    )

    pickler = providers.Factory(
        NullOperator,
    )


class ProdDataContainer(containers.DeclarativeContainer):

    config = providers.Configuration()

    gen_dataset = providers.Factory(
        Sampler,
        cluster=config.gen_dataset.params.operator.cluster,
        cluster_by=config.gen_dataset.params.operator.cluster_by,
        frac=config.gen_dataset.params.operator.frac,
        replace=config.gen_dataset.params.operator.replace,
        shuffle=config.gen_dataset.params.operator.shuffle,
        random_state=config.gen_dataset.params.operator.random_state
    )

    train_test_split = providers.Factory(
        TrainTestSplit,
        train_size=config.train_test_split.params.operator.train_size
    )

    center = providers.Factory(
        DataCenterizer,
        var=config.center_train_data.params.operator.var,
        group_var=config.center_train_data.params.operator.group_var,
        out_var=config.center_train_data.params.operator.out_var,
    )

    aggregate = providers.Factory(
        MeanAggregator,
        var=config.aggregate_train.params.operator.var,
        group_var=config.aggregate_train.params.operator.group_var,
        out_var=config.aggregate_train.params.operator.out_var,
    )


class DevDataContainer(containers.DeclarativeContainer):

    config = providers.Configuration()

    gen_dataset = providers.Factory(
        Sampler,
        cluster=config.gen_dataset.params.operator.cluster,
        cluster_by=config.gen_dataset.params.operator.cluster_by,
        frac=config.gen_dataset.params.operator.frac,
        replace=config.gen_dataset.params.operator.replace,
        shuffle=config.gen_dataset.params.operator.shuffle,
        random_state=config.gen_dataset.params.operator.random_state
    )


class TestingDataContainer(containers.DeclarativeContainer):

    config = providers.Configuration()

    gen_dataset = providers.Factory(
        Sampler,
        cluster=config.gen_dataset.params.operator.cluster,
        cluster_by=config.gen_dataset.params.operator.cluster_by,
        frac=config.gen_dataset.params.operator.frac,
        replace=config.gen_dataset.params.operator.replace,
        shuffle=config.gen_dataset.params.operator.shuffle,
        random_state=config.gen_dataset.params.operator.random_state
    )


class MovieLens(containers.DeclarativeContainer):

    config = providers.Configuration(yaml_files=["recsys/core/workflow/config/01_movielens25m_datasets.yml"])

    staging = providers.Container(DataStagingContainer, config=config.staging.tasks)

    prod = providers.Container(ProdDataContainer, config=config.prod.tasks)

    dev = providers.Container(DevDataContainer, config=config.dev.tasks)

    test = providers.Container(TestingDataContainer, config=config.test.tasks)
