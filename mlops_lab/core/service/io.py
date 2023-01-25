#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /mlops_lab/core/service/io.py                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 5th 2022 10:28:41 pm                                                #
# Modified   : Tuesday January 24th 2023 08:13:44 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""IO Utils"""
from abc import ABC, abstractmethod
import os
import logging
import yaml
import pickle
import pandas as pd
from typing import Any, Union, List

# ------------------------------------------------------------------------------------------------ #


class IO(ABC):  # pragma: no cover

    _logger = logging.getLogger(
        f"{__module__}.{__name__}",
    )

    @classmethod
    def read(cls, filepath: str, *args, **kwargs) -> Any:
        data = cls._read(filepath, **kwargs)
        return data

    @classmethod
    @abstractmethod
    def _read(cls, filepath: str, **kwargs) -> Any:
        pass

    @classmethod
    def write(cls, filepath: str, data: Any, *args, **kwargs) -> None:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        cls._write(filepath, data, **kwargs)

    @classmethod
    @abstractmethod
    def _write(cls, filepath: str, data: Any, **kwargs) -> None:
        pass


# ------------------------------------------------------------------------------------------------ #
#                                         EXCEL IO                                                 #
# ------------------------------------------------------------------------------------------------ #


class ExcelIO(IO):
    @classmethod
    def _read(
        cls,
        filepath: str,
        sheet_name: Union[str, int, list, None] = 0,
        header: Union[int, None] = 0,
        names: list = None,
        index_col: Union[int, str] = None,
        usecols: List[str] = None,
        **kwargs,
    ) -> pd.DataFrame:

        return pd.read_excel(
            filepath,
            sheet_name=sheet_name,
            header=header,
            index_col=index_col,
            usecols=usecols,
            **kwargs,
        )

    @classmethod
    def _write(
        cls,
        filepath: str,
        data: pd.DataFrame,
        sheet_name: str = "Sheet1",
        columns: Union[str, list] = None,
        header: Union[bool, list] = True,
        index: bool = False,
        **kwargs,
    ) -> None:
        data.to_excel(
            excel_writer=filepath,
            sheet_name=sheet_name,
            columns=columns,
            header=header,
            index=index,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
#                                        CSV IO                                                    #
# ------------------------------------------------------------------------------------------------ #


class CSVIO(IO):
    @classmethod
    def _read(
        cls,
        filepath: str,
        header: Union[int, None] = 0,
        index_col: Union[int, str] = None,
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
        index: bool = False,
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
    def _read(cls, filepath: str, **kwargs) -> dict:

        with open(filepath, "r") as f:
            try:
                return yaml.safe_load(f)
            except yaml.YAMLError as e:  # pragma: no cover
                cls._logger.error(e)
                raise IOError(e)
            finally:
                f.close()

    @classmethod
    def _write(cls, filepath: str, data: Any, **kwargs) -> None:
        with open(filepath, "w") as f:
            try:
                yaml.dump(data, f)
            except yaml.YAMLError as e:  # pragma: no cover
                cls._logger.error(e)
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
                cls._logger.error(e)
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
                cls._logger.error(e)
                raise (e)
            finally:
                f.close()


# ------------------------------------------------------------------------------------------------ #
#                                       IO SERVICE                                                 #
# ------------------------------------------------------------------------------------------------ #
class IOService:

    __io = {
        "csv": CSVIO,
        "yaml": YamlIO,
        "yml": YamlIO,
        "pkl": PickleIO,
        "pickle": PickleIO,
        "xlsx": ExcelIO,
        "xls": ExcelIO,
    }
    _logger = logging.getLogger(
        f"{__module__}.{__name__}",
    )

    @classmethod
    def read(cls, filepath: str, **kwargs) -> Any:
        io = cls._get_io(filepath)
        return io.read(filepath, **kwargs)

    @classmethod
    def write(cls, filepath: str, data: Any, **kwargs) -> None:
        io = cls._get_io(filepath)
        io.write(filepath=filepath, data=data, **kwargs)

    @classmethod
    def _get_io(cls, filepath: str) -> IO:
        try:
            file_format = os.path.splitext(filepath)[1].replace(".", "")
            return IOService.__io[file_format]
        except TypeError:
            if filepath is None:
                msg = "Filepath is None"
                cls._logger.error(msg)
                raise ValueError(msg)
        except KeyError:
            msg = "File type {} is not supported.".format(file_format)
            cls._logger.error(msg)
            raise ValueError(msg)
