#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /mlops_lab/core/workflow/operator/data/mover.py                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday January 14th 2023 04:03:14 am                                              #
# Modified   : Tuesday January 24th 2023 08:13:48 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Data Mover Module"""
import urllib
from zipfile import ZipFile
from typing import Any

from mlops_lab.core.workflow.operator.base import Operator
from mlops_lab.core.repo.uow import UnitOfWork


# ------------------------------------------------------------------------------------------------ #
#                                  DOWNLOAD EXTRACTOR ZIP                                          #
# ------------------------------------------------------------------------------------------------ #
class DownloadZipExtract(Operator):
    """Downloads and extracts DataSource files to a destination directory.

    Args:
        name (str): Name of the DataSource
        destination (str): A directory into which the ZipFile contents will be extracted.
    """

    def __init__(self, name: str, destination: str) -> None:
        super().__init__()
        self._name = name
        self._destination = destination

    def execute(self, uow: UnitOfWork, data: Any = None) -> None:
        """Downloads and extracts the DataSource."""
        # Obtain the datasource urls
        repo = uow.get_repo("datasource")
        datasource = repo.get_by_name(self._name)
        # Iterate through the URLs
        for url in datasource.urls.values():
            zipresp = urllib.request.urlopen(url["url"])
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
            zf.extractall(path=self._destination)
            # close the ZipFile instance
            zf.close()
