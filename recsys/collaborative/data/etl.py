#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /etl.py                                                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 17th 2022 06:37:20 am                                             #
# Modified   : Friday November 18th 2022 12:47:45 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import logging
import os
import shlex
import subprocess
import importlib
from datetime import datetime
from zipfile import ZipFile
from tqdm import tqdm

import wandb

from recsys.core.base.workflow import Pipeline, Operator, PipelineBuilder, Context, PipelineDirector

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


# ------------------------------------------------------------------------------------------------ #
#                                     KAGGLE DOWNLOADER                                            #
# ------------------------------------------------------------------------------------------------ #


class KaggleDownloader(Operator):
    """Downloads Dataset from Kaggle using the Kaggle API
    Args:
        kaggle_filepath (str): The filepath for the Kaggle dataset
        destination (str): The folder to which the data will be downloaded.
    """

    def __init__(
        self,
        kaggle_filepath: str,
        destination: str,
        force: bool = False,
    ) -> None:
        name = self.__class__.__name__.lower()
        super().__init__(
            name=name,
            kaggle_filepath=kaggle_filepath,
            destination=destination,
            force=force,
        )

        self.command = (
            "kaggle datasets download" + " -d " + self.kaggle_filepath + " -p " + self.destination
        )

    def execute(self, *args, **kwargs) -> None:
        """Downloads compressed data via an API using bash"""
        os.makedirs(self.destination, exist_ok=True)
        if self._proceed():
            subprocess.run(shlex.split(self.command), check=True, text=True, shell=False)

    def _proceed(self) -> bool:
        if self.force:
            return True

        else:
            kaggle_filename = os.path.basename(self.kaggle_filepath) + ".zip"
            if os.path.exists(os.path.join(self.destination, kaggle_filename)):
                logger.info("Download skipped as {} already exists.".format(kaggle_filename))
                return False
            else:
                return True


# ------------------------------------------------------------------------------------------------ #
#                                     DEZIPPER - EXTRACT ZIPFILE                                   #
# ------------------------------------------------------------------------------------------------ #


class DeZipper(Operator):
    """Unzipps a ZipFile archive

    Args:
        zipfilepath (str): The path to the Zipfile to be extracted.
        destination (str): The directory into which the zipfiles shall be extracted
        members (list): Optional, list of members to be extracted
        force (bool): If True, unzip will overwrite existing file(s) if present.
    """

    def __init__(
        self,
        zipfilepath: str,
        destination: str,
        members: list = None,
        force: bool = False,
    ) -> None:
        name = self.__class__.__name__.lower()
        super().__init__(
            name=name,
            zipfilepath=zipfilepath,
            destination=destination,
            members=members,
            force=force,
        )

    def execute(self, *args, **kwargs) -> None:
        os.makedirs(self.destination, exist_ok=True)

        if self._proceed():
            with ZipFile(self.zipfilepath, "r") as zipobj:
                zipobj.extractall(path=self.destination, members=self.members)

    def _proceed(self) -> bool:
        if self.force:
            return True
        elif os.path.exists(os.path.join(self.destination, self.members[0])):
            zipfilename = os.path.basename(self.zipfilepath)
            logger.info("DeZip skipped as {} already exists.".format(zipfilename))
            return False
        else:
            return True


# ------------------------------------------------------------------------------------------------ #
#                                          PICKLER                                                 #
# ------------------------------------------------------------------------------------------------ #


class Pickler(Operator):
    """Converts a file to pickle format and optionally removes the original file.

    Args:
        infilepath (str): Path to file being converted
        outfilepath (str): Path to the converted file
        infile_format(str): The format of the input file
        infile_params (dict): Optional. Dictionary containing additional keyword arguments for reading the infile.
        usecols (list): List of columns to select.
        outfile_params (dict): Optional. Dictionary containing additional keyword arguments for writing the outfile.
        force (bool): If True, overwrite existing file if it exists.
        kwargs (dict): Additional keyword arguments to be passed to io object.
    """

    def __init__(
        self,
        infilepath: str,
        outfilepath: str,
        infile_format: str = "csv",
        usecols: list = [],
        index_col: bool = False,
        encoding: str = "utf-8",
        low_memory: bool = False,
        force: bool = False,
    ) -> None:
        super().__init__(
            infilepath=infilepath,
            outfilepath=outfilepath,
            infile_format=infile_format,
            usecols=usecols,
            index_col=index_col,
            encoding=encoding,
            low_memory=low_memory,
            force=force,
        )

    def execute(self, context: Context, *args, **kwargs) -> None:
        """Executes the operation

        Args:
            context (Context): Context object containing the name
                and description of Pipeline, and the io object as well.
        """
        if self._proceed():
            io = context.io
            data = io.read(
                filepath=self.infilepath,
                usecols=self.usecols,
                index_col=self.index_col,
                low_memory=self.low_memory,
                encoding=self.encoding,
            )
            os.makedirs(os.path.dirname(self.outfilepath), exist_ok=True)
            io.write(self.outfilepath, data)

    def _proceed(self) -> bool:
        if self.force:
            return True
        elif os.path.exists(self.outfilepath):
            outfilename = os.path.basename(self.outfilepath)
            logger.info("Pickler skipped as {} already exists.".format(outfilename))
            return False
        else:
            return True


# ------------------------------------------------------------------------------------------------ #


class ETLPipeline(Pipeline):
    """ETL Pipeline class

    Executes Extract-Transform-Load pipelines

    Args:
        name (str): Human readable name for the pipeline run.
        description (str): Optional.
    """

    def __init__(self, name: str, description: str = None) -> None:
        super().__init__(name=name, description=description)

    def run(self) -> None:
        """Runs the pipeline"""
        self._setup()
        for name, step in tqdm(self._steps.items()):
            started = datetime.now()

            step.execute(context=self._context)

            ended = datetime.now()
            duration = (ended - started).total_seconds()
            wandb.log(
                {
                    "pipeline": self._context.name,
                    "step": name,
                    "started": started,
                    "ended": ended,
                    "duration": duration,
                }
            )
        self._teardown()


# ------------------------------------------------------------------------------------------------ #
class ETLPipelineBuilder(PipelineBuilder):
    """Constructs an ETL processing pipeline."""

    def __init__(self) -> None:
        self.reset()
        self._config = None
        self._context = None
        self._steps = {}

    def build_config(self, config: dict) -> None:
        self._config = config

    def build_context(self) -> None:
        try:
            module = importlib.import_module(name=self._config["io_module"])
            io = getattr(module, self._config["io_service"])
            self._context = Context(
                name=self._config["name"], description=self._config["description"], io=io
            )
        except KeyError as e:
            logger.error(e)
            raise

    def build_steps(self) -> None:
        steps = self._config["steps"]
        for _, step_config in steps.items():
            try:
                module = importlib.import_module(name=step_config["module"])
                step = getattr(module, step_config["operator"])

                operator = step(**step_config["params"])

                self._steps[operator.name] = operator

            except KeyError as e:
                logging.error("Configuration File is missing operator configuration data")
                raise (e)

    def build_pipeline(self) -> None:
        logger.debug(
            f"Config name: {self._config['name']}, Config description: {self._config['description']}"
        )
        self._pipeline = ETLPipeline(
            name=self._config["name"], description=self._config["description"]
        )
        logger.debug(self._pipeline)
        self._pipeline.context = self._context
        for _, step in self._steps.items():
            self._pipeline.add_step(step)


# ------------------------------------------------------------------------------------------------ #
class ETLPipelineDirector(PipelineDirector):
    """ETL Pipelne director responsible for executing the steps of the PipelineBuilder in a sequence.

    Args:
        config_filepath (str): The path to the pipeline configuration file
        builder (ETLPipelineBuilder): The concrete builder class
    """

    def __init__(self, config_filepath: str, builder: ETLPipelineBuilder) -> None:
        super().__init__(config_filepath=config_filepath, builder=builder)

    def build_etl_pipeline(self) -> None:
        """Constructs the ETL Pipeline"""
        self._builder.build_config(config=self._config)
        self._builder.build_context()
        self._builder.build_steps()
        self._builder.build_pipeline()
