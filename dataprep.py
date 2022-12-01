#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /dataprep.py                                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday November 29th 2022 08:46:21 pm                                              #
# Modified   : Wednesday November 30th 2022 10:24:18 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from recsys.methods.neighborhood.workflow.pipeline import DatasetPipelineBuilder
from recsys.core.workflow.pipeline import PipelineDirector
from recsys.core.services.io import IOService
from recsys.core.services.container import container

CF_CONFIG_FILEPATH = "recsys/config/cf.yml"
# ------------------------------------------------------------------------------------------------ #


def get_config():
    return IOService.read(CF_CONFIG_FILEPATH)


def create_builder():
    return DatasetPipelineBuilder()


def create_director(config: dict, builder: DatasetPipelineBuilder, io: IOService):
    return PipelineDirector(config=config, builder=builder, io=IOService)


def build_pipeline(director: PipelineDirector):
    director.build_pipeline()
    return director.builder.pipeline


def main():
    config = get_config()
    repo = container.repo
    repo().reset()

    builder = create_builder()
    director = create_director(config=config, builder=builder, io=IOService)
    pipeline = build_pipeline(director)
    pipeline.run()

    repo().print_registry()


if __name__ == "__main__":
    main()
