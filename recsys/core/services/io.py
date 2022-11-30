#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /io.py                                                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 10th 2022 04:03:40 pm                                             #
# Modified   : Tuesday November 29th 2022 11:55:22 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""IO Utils"""
import os
import yaml
import pickle
import logging
import pandas as pd
from typing import Any, Union, List
from abc import ABC, abstractmethod

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class IO(ABC):
    @classmethod
    def read(cls, filepath: str, *args, **kwargs) -> Any:
        data = cls._read(filepath, **kwargs)
        return data

    @classmethod
    @abstractmethod
    def _read(cls, filepath: str, **kwargs) -> Any:
        """Read data"""

    @classmethod
    def write(cls, filepath: str, data: Any, *args, **kwargs) -> None:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        cls._write(filepath, data, **kwargs)

    @classmethod
    @abstractmethod
    def _write(cls, filepath: str, data: Any, **kwargs) -> None:
        """Write data"""


# ------------------------------------------------------------------------------------------------ #
#                                        CSV IO                                                    #
# ------------------------------------------------------------------------------------------------ #


class CSVIO(IO):
    @classmethod
    def _read(
        cls,
        filepath: str,
        header: Union[int, None] = 0,
        index_col: Union[int, str] = 0,
        usecols: List[str] = None,
        low_memory: bool = False,
        encoding: str = "utf-8",
        **kwargs,
    ) -> pd.DataFrame:

        return pd.read_csv(
            filepath,
            header=header,
            index_col=index_col,
            usecols=usecols,
            low_memory=low_memory,
            encoding=encoding,
        )

    @classmethod
    def _write(
        cls,
        filepath: str,
        data: pd.DataFrame,
        sep: str = ",",
        index: bool = True,
        index_label: bool = None,
        encoding: str = "utf-8",
        **kwargs,
    ) -> None:
        data.to_csv(filepath, sep=sep, index=index, index_label=index_label, encoding=encoding)


# ------------------------------------------------------------------------------------------------ #
#                                        YAML IO                                                   #
# ------------------------------------------------------------------------------------------------ #


class YamlIO(IO):
    @classmethod
    def _read(cls, filepath: str, document: str = None, **kwargs) -> dict:

        with open(filepath, "r") as f:
            try:
                return yaml.safe_load(f)
            except yaml.YAMLError as e:  # pragma: no cover
                logger.error(e)
                raise IOError(e)
            finally:
                f.close()

    @classmethod
    def _write(cls, filepath: str, data: Any, **kwargs) -> None:
        with open(filepath, "w") as f:
            try:
                yaml.dump(data, f)
            except yaml.YAMLError as e:  # pragma: no cover
                logger.error(e)
                raise IOError(e)
            finally:
                f.close()


# ------------------------------------------------------------------------------------------------ #
#                                         PICKLE                                                   #
# ------------------------------------------------------------------------------------------------ #


class PickleIO(IO):
    @classmethod
    def _read(cls, filepath: str, **kwargs) -> Any:

        with open(filepath, "rb") as f:
            try:
                return pickle.load(f)
            except pickle.PickleError() as e:  # pragma: no cover
                logger.error(e)
                raise IOError(e)
            finally:
                f.close()

    @classmethod
    def _write(cls, filepath: str, data: Any, write_mode: str = "wb", **kwargs) -> None:
        # Note, "a+" write_mode for append. If <TypeError: write() argument must be str, not bytes>
        # use "ab+"
        with open(filepath, write_mode) as f:
            try:
                pickle.dump(data, f)
            except pickle.PickleError() as e:  # pragma: no cover
                logger.error(e)
                raise (e)
            finally:
                f.close()


# ------------------------------------------------------------------------------------------------ #
#                                       IO SERVICE                                                 #
# ------------------------------------------------------------------------------------------------ #
class IOService:

    __io = {"csv": CSVIO, "yaml": YamlIO, "yml": YamlIO, "pkl": PickleIO, "pickle": PickleIO}

    def __init__(self) -> None:
        """Here to test dependency injection with providers."""
        pass

    @property
    def file_formats(self) -> list:
        return list(IOService.__io.keys())

    @classmethod
    def read(cls, filepath: str, **kwargs) -> Any:
        file_format = cls._validate(filepath)
        io = cls._get_io(file_format)
        return io.read(filepath, **kwargs)

    @classmethod
    def write(cls, filepath: str, data: Any, **kwargs) -> None:
        file_format = cls._validate(filepath)
        io = cls._get_io(file_format)
        io.write(filepath=filepath, data=data, **kwargs)

    @classmethod
    def _get_io(cls, file_format: str) -> IO:
        return IOService.__io[file_format]

    @classmethod
    def _validate(cls, filepath) -> str:
        file_format = os.path.splitext(filepath)[1].replace(".", "")
        if file_format not in IOService.__io.keys():
            msg = f"File format {file_format} is not supported."
            logger.error(msg)
            raise TypeError(msg)

        return file_format
