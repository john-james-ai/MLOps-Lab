#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /test_operators.py                                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday November 26th 2022 12:17:19 am                                             #
# Modified   : Saturday November 26th 2022 05:53:47 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import inspect
from datetime import datetime
import pytest
import logging
from logging import config

from recsys.config.log import test_log_config
from recsys.core.services.io import IOService
from recsys.core.workflow.operators import KaggleDownloader, DeZipper, Pickler, Copier, Sampler
from recsys.core.workflow.pipeline import Context, PipelineDirector
from recsys.core.data.etl import ETLPipelineBuilder

# ------------------------------------------------------------------------------------------------ #
config.dictConfig(test_log_config)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.ops
class TestOperators:  # pragma: no cover
    # ================================================================================================ #
    def test_setup(self, etl_config, caplog):
        # Enter setup activities here
        pass

    # ================================================================================================ #
    def test_downloader(self, etl_config, caplog):
        start = datetime.now()
        logger.info(
            "\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%H:%M:%S"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        config = etl_config["steps"][1]["params"]
        dl = KaggleDownloader(
            kaggle_filepath=config["kaggle_filepath"],
            destination=config["destination"],
            force=True,
        )
        dl.execute()
        assert os.path.exists(
            os.path.join(config["destination"], os.path.basename(config["kaggle_filepath"]))
            + ".zip"
        )
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%H:%M:%S"),
                end.strftime("%m/%d/%Y"),
            )
        )

    # ============================================================================================ #
    def test_dezip(self, etl_config, caplog):
        start = datetime.now()
        logger.info(
            "\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%H:%M:%S"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        config = etl_config["steps"][2]["params"]
        dz = DeZipper(
            zipfilepath=config["zipfilepath"],
            destination=config["destination"],
            members=config["members"],
            force=True,
        )
        dz.execute()
        assert os.path.exists(os.path.join(config["destination"], config["members"][0]))
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%H:%M:%S"),
                end.strftime("%m/%d/%Y"),
            )
        )

    # ============================================================================================ #
    def test_pickler(self, etl_config, caplog):
        start = datetime.now()
        logger.info(
            "\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%H:%M:%S"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        context = Context(name="test_pickler", description="testing pickler", io=IOService)

        config = etl_config["steps"][3]["params"]
        p = Pickler(
            infilepath=config["infilepath"],
            outfilepath=config["outfilepath"],
            infile_format=config["infile_format"],
            index_col=config["index_col"],
            encoding=config["encoding"],
            low_memory=config["low_memory"],
            usecols=config["usecols"],
            force=True,
        )
        p.execute(context=context)
        assert os.path.exists(config["outfilepath"])
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%H:%M:%S"),
                end.strftime("%m/%d/%Y"),
            )
        )

    # ============================================================================================ #
    def test_copy(self, etl_config, caplog):
        start = datetime.now()
        logger.info(
            "\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%H:%M:%S"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        context = Context(name="test_copier", description="testing copier", io=IOService)

        config = etl_config["steps"][4]["params"]

        c = Copier(
            infilepath=config["infilepath"],
            outfilepath=config["outfilepath"],
            force=True,
        )
        c.execute(context=context)
        assert os.path.exists(config["outfilepath"])
        assert os.path.getsize(config["infilepath"]) == os.path.getsize(config["outfilepath"])

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%H:%M:%S"),
                end.strftime("%m/%d/%Y"),
            )
        )

    # ============================================================================================ #
    def test_sampler(self, etl_config, caplog):
        start = datetime.now()
        logger.info(
            "\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%H:%M:%S"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        context = Context(name="test_sampler", description="testing samplier", io=IOService)

        config = etl_config["steps"][5]["params"]

        s = Sampler(
            infilepath=config["infilepath"],
            outfilepath=config["outfilepath"],
            clustered=config["clustered"],
            clustered_by=config["clustered_by"],
            frac=config["frac"],
            random_state=config["random_state"],
            force=True,
        )
        s.execute(context=context)
        assert os.path.exists(config["outfilepath"])
        assert os.path.getsize(config["infilepath"]) > os.path.getsize(config["outfilepath"])
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%H:%M:%S"),
                end.strftime("%m/%d/%Y"),
            )
        )

    # ================================================================================================ #
    def test_teardown(self, etl_config, caplog):
        # Enter teardown activities here
        pass


@pytest.mark.pipe
class TestPipeline:
    # ================================================================================================ #
    def test_setup(self, etl_config, caplog):
        start = datetime.now()
        logger.info(
            "\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%H:%M:%S"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        pass
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%H:%M:%S"),
                end.strftime("%m/%d/%Y"),
            )
        )

    # ============================================================================================ #
    def test_director(self, etl_config, caplog):
        start = datetime.now()
        logger.info(
            "\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%H:%M:%S"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        outfile = etl_config["steps"][6]["params"]["outfilepath"]
        d = PipelineDirector(config=etl_config, builder=ETLPipelineBuilder())
        d.build_pipeline()
        pipeline = d.builder.pipeline
        pipeline.run()
        assert os.path.exists(outfile)
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%H:%M:%S"),
                end.strftime("%m/%d/%Y"),
            )
        )

    # ================================================================================================ #
    def test_teardown(self, etl_config, caplog):
        start = datetime.now()
        logger.info(
            "\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%H:%M:%S"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        pass
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%H:%M:%S"),
                end.strftime("%m/%d/%Y"),
            )
        )
