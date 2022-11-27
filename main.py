#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /main.py                                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday November 25th 2022 07:22:27 pm                                               #
# Modified   : Saturday November 26th 2022 04:50:06 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import logging
from logging import config

from recsys.config.log import log_config
from recsys.core.services.io import IOService
from recsys.core.workflow.pipeline import PipelineDirector
from recsys.core.data.etl import ETLPipelineBuilder
from recsys.config.data import ETL_CONFIG_FILE

# ------------------------------------------------------------------------------------------------ #
config.dictConfig(log_config)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


def get_config(config_filepath, io: IOService):
    return io.read(config_filepath)


def get_data(config: dict):
    builder = ETLPipelineBuilder()
    director = PipelineDirector(config=config, builder=builder)
    director.build_pipeline()
    pipeline = builder.pipeline
    pipeline.run()
    assert os.path.exists(
        config["steps"][3]["params"]["outfilepath"]
    ), f"Output file {config['steps'][3]['params']['outfilepath']} was not found."


def main():
    cfg = get_config(config_filepath=ETL_CONFIG_FILE, io=IOService)
    get_data(config=cfg)


# ------------------------------------------------------------------------------------------------ #
if __name__ == "__main__":
    main()
