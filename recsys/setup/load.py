#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/setup/load.py                                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 30th 2022 07:04:08 pm                                               #
# Modified   : Friday December 30th 2022 07:18:07 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #

from recsys.core.workflow.builder import PipelineBuilder, Director
from recsys.core.workflow.pipeline import Pipeline
from recsys.core.dal.uow import UnitOfWork
from recsys.core.services.io import IOService


# ------------------------------------------------------------------------------------------------ #
def load_sources():
    config_filepath = "recsys/setup/config.yml"
    director = Director(pipeline=Pipeline(), uow=UnitOfWork(), io=IOService)
    director.builder = PipelineBuilder()
    pipeline = director.build_pipeline(config_filepath)
    pipeline.run()
