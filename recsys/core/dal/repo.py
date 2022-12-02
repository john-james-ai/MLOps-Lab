#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /repo.py                                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday November 14th 2022 01:27:04 am                                               #
# Modified   : Friday December 2nd 2022 03:36:02 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Data Repository Module"""
import logging
import os
from glob import glob
from abc import ABC, abstractmethod
from typing import Any
import shutil

from recsys.core.dal.dataset import Dataset
from recsys.core.dal.DATA import Dataset
from recsys.core.services.io import IOService

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class Repo(ABC):
    """Repository base class"""

    @abstractmethod
    def add(self, *args, **kwargs) -> None:  # pragma: no cover
        pass

    @abstractmethod
    def get(self, id: str) -> Any:  # pragma: no cover
        pass

    @abstractmethod
    def remove(self, id: str) -> None:  # pragma: no cover
        pass

    @abstractmethod
    def exists(self, id: str) -> bool:  # pragma: no cover
        pass


# ------------------------------------------------------------------------------------------------ #
class DatasetRepo(Repo):
    """Dataset Repository

    Repository of Dataset objects.

    Args:
        repo_directory (str): The base directory for the repository.
        archive_directory (str): The base directory for the archive.
        io (IOService): Service responsible for reading and writing data to, from the repository.
        file_format (str): The format in which the Dataset objects are stored.
        archive (bool): Whether to archive rather than delete objects on reset and remove.
    """

    def __init__(
        self,
        repo_directory: str,
        archive_directory: str,
        io: IOService,
        registry: Dataset,
        file_format: str,
        archive: bool = True,
    ) -> None:
        self._repo_directory = repo_directory
        self._archive_directory = archive_directory
        self._io = io
        self._registry = registry
        self._archive = archive
        self._file_format = file_format

    def __len__(self) -> int:
        return len(self._registry)

    @property
    def repo_directory(self) -> str:
        return self._repo_directory

    @property
    def archive(self) -> bool:
        return self._archive

    @archive.setter
    def archive(self, archive: bool) -> None:
        self._archive = archive

    @property
    def archive_directory(self) -> str:
        return self._archive_directory

    def reset(self, purge_archive: bool = False, silent: bool = False) -> None:
        """Resets the repository and optionally purges the archive.

        Args:
            purge_archive (bool): Indicates whether the archive should be purged.
            silent (bool): If True, confirmation prompts are suppressed.
        """
        if silent:
            if purge_archive:
                self._reset_all()
            elif self._archive:
                self._reset_and_archive()
            else:
                self._reset_repo()
        else:
            if purge_archive:
                msg = (
                    "This action will PERMANENTLY DELETE all data and archive. Are you SURE? (Y/N)"
                )
                sure = input(msg)
                if "y" in sure.lower():
                    self._reset_all()
            elif self._archive:
                self._reset_and_archive()
            else:
                msg = (
                    "Resetting the repository will PERMANENTLY DELETE all data. Are you SURE? (Y/N)"
                )
                sure = input(msg)
                if "y" in sure.lower():
                    self._reset_repo()

    def add(self, dataset: Dataset) -> None:
        """Adds a Dataset to the repo.

        Args:
            dataset (Dataset): The Dataset object
        """
        while self._registry.version_exists(dataset):
            dataset.version = dataset.version + 1

        dataset.filepath = self._get_filepath(dataset)
        dataset = self._registry.add(dataset)
        self._io.write(filepath=dataset.filepath, data=dataset)
        return dataset

    def get(self, id: int) -> Any:
        """Retrieves a Dataset object by id

        Args:
            id (int): ID of the dataset object
        """
        try:
            registration = self._registry.get(id)
            try:
                return self._io.read(registration["filepath"])
            except FileNotFoundError as e:
                msg = f"The registered Dataset {id} data not found in repository. The database may be corrupt.\n{e}"
                logger.error(msg)
                raise FileNotFoundError(msg)
        except FileNotFoundError:
            msg = f"No Dataset with id = {id} exists."
            logger.error(msg)
            raise FileNotFoundError(msg)

    def get_dataset(self, name: str, stage: str = None):
        """Returns the latest version of the Dataset that matches the criteria.

        Args:
            name (str): Name of the Dataset object.
            stage (str): Preprocessing stage. One of 'staged', 'interim' or 'final'
        """
        result = self.find_dataset(name=name, stage=stage)
        if result.shape[0] > 1:
            result = result.sort_values(by="version", ascending=False).reset_index().iloc[0]
        return self.get(int(result["id"]))

    def find_dataset(self, name: str, stage: str = None, include_archive: bool = True) -> Dataset:
        """Returns a Dataframe of Dataset objects that match the criteria

        Args:
            name (str): Required name of Dataset.
            stage (str): Optional, one of 'input', 'interim', or 'final'.
        """
        return self._registry.find_dataset(name=name, stage=stage)

    def remove(self, id: int, ignore_errors=False) -> None:
        """Removes a Dataset object from the registry and file system.

        Args:
            id (int): ID of the dataset object
            ignore_errors (bool): If True, errors will not throw an exception.

        Raises: FileNotFoundError (if ignore_errors = False)
        """
        try:
            registration = self._registry.get(id)
            if self._archive:
                self.archive_dataset(id=registration["id"])
            else:
                os.remove(registration["filepath"])
            self._registry.remove(id)
        except FileNotFoundError:
            if ignore_errors:
                pass
            else:
                msg = f"Dataset id: {id} does not exist."
                logger.error(msg)
                raise FileNotFoundError(msg)

    def exists(self, id: int) -> bool:
        """Checks whether the dataset exists.
        Args:
            dataset (Dataset): The dataset to check.
        """
        return self._registry.exists(id)

    def version_exists(self, dataset: Dataset) -> bool:
        """Checks whether the dataset exists.
        Args:
            dataset (Dataset): The dataset to check.
        """
        return self._registry.version_exists(dataset=dataset)

    def archive_repo(self) -> None:
        """movies repository data to an archive folder."""
        i = 0

        registrations = self._registry.get_all(as_dict=True)
        for id, registration in registrations.items():
            self.archive_dataset(id)
            i += 1

        logger.info(f"Archive complete, {str(i)} files archived.")

    def archive_dataset(self, id: int) -> None:
        """Moves a Dataset to archive.

        Args:
            id (int): Dataset id

        Raises FileNotFoundError if Dataset doesn't exist.
        """
        registration = self._registry.get(id)
        source = registration["filepath"]
        destination = self._get_archive_filepath(registration)
        self._move_file(source=source, destination=destination)
        self._registry.archive(id)

    def restore_repo(self) -> None:
        """Restores a repository from archive"""
        i = 0

        registrations = self._registry.get_archive()
        for id, registration in registrations.items():
            self.restore_dataset(id)
            i += 1
        logger.info(f"Restore complete, {str(i)} files restored.")

    def restore_dataset(self, id: int) -> None:
        """Restores a Dataset from archive.

        Args:
            id (int): Dataset Id
        """
        registration = self._registry.get(id)
        source = self._get_archive_filepath(registration)
        destination = registration["filepath"]  # Filepath doesn't change under archive.
        self._move_file(source=source, destination=destination)
        self._registry.restore(id)

    def print(self, id: int) -> None:
        """Prints a Dataset by id.

        Args:
            id (int): Id for a Dataset

        """
        try:
            registration = self._registry.get(id=id)
            dataset = Dataset(**registration)
            print(dataset)
        except FileNotFoundError:
            msg = f"Dataset id: {id} does not exist."
            logger.error(msg)
            raise FileNotFoundError(msg)

    def print_registry(self) -> None:
        """Prints and returns a DataFrame containing the registry."""
        registry = self._registry.get_all()
        print(registry)
        return registry

    def _get_filepath(self, dataset: Dataset) -> str:
        """Formats a filename using the Dataset name, stage, and version."""
        filename = (
            dataset.name
            + "_"
            + dataset.stage
            + "_v"
            + str(dataset.version)
            + "."
            + self._file_format
        )
        return os.path.join(self._repo_directory, dataset.stage, filename)

    def _get_archive_filepath(self, registration: str) -> str:
        """Returns the archive filepath given a registration."""
        return os.path.join(
            self._archive_directory,
            registration["stage"],
            os.path.basename(registration["filepath"]),
        )

    def _reset_all(self) -> None:
        """Resets repo and archive. All data is deleted."""
        self._registry.reset()
        self._purge_repo()
        self._purge_archive()

    def _reset_and_archive(self) -> None:
        """Resets the repository, archiving all data."""
        self._registry.reset()
        self.archive_repo()
        self._purge_repo()

    def _reset_repo(self) -> None:
        """Resets repo and deletes data. Archive is untouched."""
        self._registry.reset()
        self._purge_repo()

    def _purge_repo(self) -> None:
        """Deletes the data in the repository."""
        pattern = self._repo_directory + "/**/*." + self._file_format
        filelist = glob(pattern)
        self._purge(filelist=filelist)

    def _purge_archive(self) -> None:
        pattern = self._archive_directory + "/**/*." + self._file_format
        filelist = glob(pattern)
        self._purge(filelist=filelist)

    def _purge(self, filelist: list) -> None:
        """Purges the designated folder(s) and contents.

        Args:
            filelist (list): List of files to remove.
        """
        for filepath in filelist:
            try:
                os.remove(filepath)
            except OSError as e:  # pragma: no cover
                msg = f"Error while deleting {filepath}\n{e}"
                logger.error(msg)
                raise OSError(msg)

    def _move_file(self, source, destination) -> None:
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        shutil.move(src=source, dst=destination)
