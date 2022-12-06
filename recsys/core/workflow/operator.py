#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/entity/operator.py                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 5th 2022 02:31:12 am                                                #
# Modified   : Monday December 5th 2022 05:03:20 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Operator Entity Module"""
import os
import pandas as pd
from dataclasses import dataclass
from abc import ABC, abstractmethod
from urllib.request import urlopen
from zipfile import ZipFile


from recsys.core.dal.dto import OperatorDTO
from .base import Entity


# ================================================================================================ #
#                                    PARAMETER OBJECTS                                             #
# ================================================================================================ #
#                                  TASK PARAMETER OBJECT                                           #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskPO(ABC):
    """Base class for parameters that control operator behavior"""

    force: bool = False


# ------------------------------------------------------------------------------------------------ #
#                                  INPUT PARAMETER OBJECTS                                         #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class InputPO(ABC):
    """Base class for parameters that specify operator input."""


@dataclass
class RemoteInputPO(InputPO):
    url: str


# ------------------------------------------------------------------------------------------------ #


@dataclass
class FilesetInputPO(InputPO):
    id: int


# ------------------------------------------------------------------------------------------------ #


@dataclass
class DatasetInputPO(InputPO):
    id: int


# ------------------------------------------------------------------------------------------------ #
#                                  OUTPUT PARAMETER OBJECTS                                        #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class OutputPO(ABC):
    """Base class that specifies the parameters for operator output."""


@dataclass
class FilesetOutputPO(OutputPO):
    name: str
    source: str
    filepath: str
    description: str = None


@dataclass
class DatasetOutputPO(OutputPO):
    name: str
    source: str
    workspace: str
    stage: str
    description: str = None


# ================================================================================================ #
#                                    OPERATOR BASE CLASS                                           #
# ================================================================================================ #


class Operator(Entity):
    """Operator Base Class

    Args:
        id (int): Id for the operator
        name (str): Name for the operator
        description (str): Describes the operation
        module (str): Operator's module
        classname (str): Operator's class name
        filepath (str): Operator's filepath
        task_po (TaskPO): The parameters that specify operator behavior
        input_po (InputPO): The input into the operator
        output_po (OutputPO): The specification for operator output.

    """

    def __init__(
        self,
        id: int,
        name: str,
        module: str,
        classname: str,
        filepath: str,
        task_po: TaskPO,
        input_po: InputPO,
        output_po: OutputPO,
        description: str = None,
    ) -> None:
        self._id = id
        self._name = name
        self._module = module
        self._classname = classname
        self._filepath = filepath
        self._task_po = task_po
        self._input_po = input_po
        self._output_po = output_po
        self._description = description

        super().__init__()

    def __str__(self) -> str:
        return f"\n\nOperator Id: {self._id}\n\tName: {self._name}\n\tModule: {self._module}n\tClass: {self._classname}\n\tFilepath: {self._filepath}\n\tDescription: {self._description}"

    def __repr__(self) -> str:
        return f"{self._id},{self._name},{self._module},{self._clasname},{self._filepath},{self._description}"

    @abstractmethod
    def execute(self, data: pd.DataFrame = None) -> None:
        """Executes the operation."""

    def as_dto(self) -> OperatorDTO:
        return OperatorDTO(
            id=self._id,
            name=self._name,
            description=self._description,
            module=self._module,
            classname=self._classname,
            filepath=self._filepath,
        )


# ================================================================================================ #
#                                    DOWNLOAD OPERATOR                                             #
# ================================================================================================ #


class Downloader(Operator):
    """Downloads Dataset from a website using urllib
    Args:
        url (str): The URL to the web resource
        destination (str): The file into which the data will be downloaded.
    """

    def __init__(
        self,
        id: int,
        name: str,
        module: str,
        classname: str,
        filepath: str,
        task_po: TaskPO,
        input_po: RemoteInputPO,
        output_po: FilesetOutputPO,
        description: str,
    ) -> None:

        name = name or self.__class__.__name__.lower()
        description = (
            description
            or self.__class__.__name__ + " from " + input_po.url + " to " + output_po.filepath
        )

        super().__init__(
            id=id,
            name=name,
            module=module,
            classname=classname,
            filepath=filepath,
            task_po=task_po,
            input_po=input_po,
            output_po=output_po,
            description=description,
        )

    @abstractmethod
    def execute(self, data: pd.DataFrame = None) -> None:
        """Download file."""

        if self._proceed():
            os.makedirs(os.path.dirname(self._output_po.filepath), exist_ok=True)

            zipresp = urlopen(self.url)
            # Create a new file on the hard drive
            tempzip = open("/tmp/tempfile.zip", "wb")
            # Write the contents of the downloaded file into the new file
            tempzip.write(zipresp.read())
            # Close the newly-created file
            tempzip.close()
            # Re-open the newly-created file with ZipFile()
            zf = ZipFile("/tmp/tempfile.zip")
            # Extract its contents into <extraction_path>
            # note that extractall will automatically create the path
            zf.extractall(path=self._output_po.filepath)
            # close the ZipFile instance
            zf.close()

    def _proceed(self) -> bool:
        if self.task_po.force:
            return True
        elif os.path.exists(self._output_po.filepath):
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
        id: int,
        name: str,
        module: str,
        classname: str,
        filepath: str,
        task_po: TaskPO,
        input_po: RemoteInputPO,
        output_po: FilesetOutputPO,
        description: str,
    ) -> None:
        name = name or self.__class__.__name__
        description = description or self.__str__()
        super().__init__(
            name=name,
            module=module,
            classname=classname,
            filepath=filepath,
            task_po=task_po,
            input_po=input_po,
            output_po=output_po,
            description=description,
        )

    def execute(self, data: pd.DataFrame = None) -> None:
        os.makedirs(self._output_po.filepath, exist_ok=True)

        if self._proceed():
            with ZipFile(self._input_po.filepath, "r") as zipobj:
                zipobj.extractall(path=self._output_po.filepath, members=self._output_po.members)

    def _proceed(self) -> bool:
        if self.force:
            return True
        elif os.path.exists(os.path.join(self._output_po.filepath, self._output_po.members[0])):
            zipfilename = os.path.basename(self._input_po.filepath)
            self._logger.info("DeZip skipped as {} already exists.".format(zipfilename))
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
        name: str = None,
        description: str = None,
        force: bool = False,
    ) -> None:
        name = self.__class__.__name__ if name is None else name
        description = self.__str__()

        super().__init__(
            name=name,
            description=description,
            infilepath=infilepath,
            outfilepath=outfilepath,
            infile_format=infile_format,
            usecols=usecols,
            index_col=index_col,
            encoding=encoding,
            low_memory=low_memory,
            force=force,
        )

    # @profiler
    def execute(self, data: pd.DataFrame = None, context: Context = None, *args, **kwargs) -> None:
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
#                                           COPY                                                   #
# ------------------------------------------------------------------------------------------------ #


class Copier(Operator):
    """Copies a file from source to destination
    Args:
        infilepath (str): Path to file being copied
        outfilepath (str): The destination filepath
        force (bool): If True, overwrite existing file if it exists.
    """

    def __init__(
        self,
        infilepath: str,
        outfilepath: str,
        name: str = None,
        description: str = None,
        force: bool = False,
    ) -> None:
        name = self.__class__.__name__ if name is None else name
        description = self.__str__()

        super().__init__(
            name=name,
            description=description,
            infilepath=infilepath,
            outfilepath=outfilepath,
            force=force,
        )

    # @profiler
    def execute(self, data: pd.DataFrame = None, context: Context = None, *args, **kwargs) -> None:
        """Executes the operation
        Args:
            context (Context): Context object containing the name
                and description of Pipeline, and the io object as well.
        """
        if self._proceed():
            shutil.copy(src=self.infilepath, dst=self.outfilepath)

    def _proceed(self) -> bool:
        if self.force:
            return True
        elif os.path.exists(self.outfilepath):
            outfilename = os.path.basename(self.outfilepath)
            logger.info("Copier skipped as {} already exists.".format(outfilename))
            return False
        else:
            return True


# ------------------------------------------------------------------------------------------------ #
#                                          SAMPLER                                                 #
# ------------------------------------------------------------------------------------------------ #


class Sampler(Operator):
    """Copies a file from source to destination
    Args:
        infilepath (str): Path to file being copied
        outfilepath (str): The destination filepath
        clustered (bool): Conduct clustered sampling if True. Otherwise, simple random sampling.
        clustered_by (str): The column name to cluster by.
        frac (float): The proportion of the data to return as sample.
        random_state (int): The pseudo random seed for reproducibility.
        force (bool): If True, overwrite existing file if it exists.
    """

    def __init__(
        self,
        infilepath: str,
        outfilepath: str,
        clustered: bool = False,
        clustered_by: str = None,
        frac: float = None,
        random_state: int = None,
        name: str = None,
        description: str = None,
        force: bool = False,
    ) -> None:
        name = self.__class__.__name__ if name is None else name
        description = self.__str__()

        super().__init__(
            name=name,
            description=description,
            infilepath=infilepath,
            outfilepath=outfilepath,
            clustered=clustered,
            clustered_by=clustered_by,
            frac=frac,
            random_state=random_state,
            force=force,
        )

    # @profiler
    def execute(self, data: pd.DataFrame = None, context: Context = None, *args, **kwargs) -> None:
        """Executes the operation
        Args:
            context (Context): Context object containing the name
                and description of Pipeline, and the io object as well.
        """
        if self._proceed():
            io = context.io
            data = io.read(self.infilepath)
            if self.clustered:
                data = clustered_sample(
                    df=data, by=self.clustered_by, frac=self.frac, random_state=self.random_state
                )
            else:
                data = data.sample(frac=self.frac, random_state=self.random_state)
            io.write(filepath=self.outfilepath, data=data)

    def _proceed(self) -> bool:
        if self.force:
            return True
        elif os.path.exists(self.outfilepath):
            outfilename = os.path.basename(self.outfilepath)
            logger.info("Sampler skipped as {} already exists.".format(outfilename))
            return False
        else:
            return True
