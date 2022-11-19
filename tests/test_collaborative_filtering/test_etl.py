#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /test_etl.py                                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday November 18th 2022 09:10:34 am                                               #
# Modified   : Saturday November 19th 2022 04:28:27 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import inspect
from datetime import datetime
import pytest
import logging

# import shutil

from recsys.core.workflow.operators import KaggleDownloader, DeZipper, Pickler
from recsys.core.services.io import IOService
from recsys.core.workflow.pipeline import Context

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    datefmt="%m/%d/%Y %H:%M",
    force=True,
)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
CONFIG_FILEPATH = "tests/config/etl.yaml"
CONFIG = IOService.read(CONFIG_FILEPATH)
TEST_FILEPATH = "tests/data/etl"


@pytest.mark.etl
class TestOperators:
    # ================================================================================================ #
    def test_setup(self, caplog):
        # Enter setup activities here
        # shutil.rmtree(TEST_FILEPATH, ignore_errors=True)
        pass

    # ================================================================================================ #
    def test_downloader(self, caplog):
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
        config = CONFIG["steps"][1]["params"]
        dl = KaggleDownloader(
            kaggle_filepath=config["kaggle_filepath"],
            destination=config["destination"],
            force=False,
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
    def test_dezip(self, caplog):
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
        config = CONFIG["steps"][2]["params"]
        dz = DeZipper(
            zipfilepath=config["zipfilepath"],
            destination=config["destination"],
            members=config["members"],
            force=False,
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
    def test_pickler(self, caplog):
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

        config = CONFIG["steps"][3]["params"]
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

    # ================================================================================================ #
    def test_teardown(self, caplog):
        # Enter teardown activities here
        # shutil.rmtree(TEST_FILEPATH, ignore_errors=True)
        pass


@pytest.mark.etl
class TestPipeline:
    # ================================================================================================ #
    def test_setup(self, caplog):
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
        # shutil.rmtree(TEST_FILEPATH, ignore_errors=True)

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
    def test_director(self, caplog):
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
        outfile = CONFIG["steps"][3]["params"]["outfilepath"]
        d = PipelineDirector(config_filepath=CONFIG_FILEPATH, builder=PipelineBuilder())
        d.build_etl_pipeline()
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
    def test_teardown(self, caplog):
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
        # shutil.rmtree(TEST_FILEPATH, ignore_errors=True)
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
