#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /tests/test_core/test_entity/test_fileset.py                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday December 7th 2022 10:37:56 am                                             #
# Modified   : Saturday December 10th 2022 04:23:28 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
from datetime import datetime
import pytest
import logging

from recsys.core.entity.fileset import Fileset
from recsys.core.dal.dto import FilesetDTO

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.fse
class TestFilesetEntity:  # pragma: no cover
    # ============================================================================================ #
    def test_instantiation_and_update(self, caplog):
        start = datetime.now()
        logger.info(
            "\n\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        fs = Fileset(
            name="fileset_test",
            description="Fileset Test",
            datasource="spotify",
            workspace="test",
            stage="raw",
            filepath="tests/file/fileset_test.pkl",
            task_id=122,
        )
        modified = fs.modified

        assert fs.id is None
        assert fs.name == "fileset_test"
        assert fs.description == "Fileset Test"
        assert fs.datasource == "spotify"
        assert fs.workspace == "test"
        assert fs.stage == "raw"
        assert fs.filepath == "tests/file/fileset_test.pkl"
        assert fs.task_id == 122
        assert isinstance(fs.created, datetime)
        assert fs.modified is None

        fs.id = 3
        assert fs.id == 3
        assert fs.name == "fileset_test"
        assert fs.description == "Fileset Test"
        assert fs.datasource == "spotify"
        assert fs.workspace == "test"
        assert fs.stage == "raw"
        assert fs.filepath == "tests/file/fileset_test.pkl"
        assert fs.task_id == 122
        assert isinstance(fs.created, datetime)
        assert isinstance(fs.modified, datetime)
        assert fs.modified != modified
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )

    # ============================================================================================ #
    def test_validation(self, caplog):
        start = datetime.now()
        logger.info(
            "\n\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        with pytest.raises(TypeError):
            _ = Fileset()  # Name required

        with pytest.raises(TypeError):
            _ = Fileset(name="test")  # Source missing

        with pytest.raises(TypeError):
            _ = Fileset(name="test", datasource="spotify")  # Filepath missing

        with pytest.raises(TypeError):
            _ = Fileset(name="test", datasource="spotify", filepath="/test/filepath")  # Task_id missing

        with pytest.raises(ValueError):
            _ = Fileset(
                name="test", datasource="asa", filepath="/test/filepath", task_id=22
            )  # Invalid datasource

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )

    # ============================================================================================ #
    def test_dto(self, caplog):
        start = datetime.now()
        logger.info(
            "\n\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        fs = Fileset(
            name="fileset_test",
            description="Fileset Test",
            datasource="spotify",
            workspace="test",
            stage="raw",
            filepath="tests/file/fileset_test.pkl",
            task_id=122,
        )
        # AS DTO
        dto = fs.as_dto()
        assert isinstance(dto, FilesetDTO)
        assert fs.id == dto.id
        assert fs.name == dto.name
        assert fs.description == dto.description
        assert fs.datasource == dto.datasource
        assert fs.workspace == "test"
        assert fs.stage == "raw"
        assert fs.filepath == dto.filepath
        assert fs.task_id == dto.task_id
        assert fs.created == dto.created
        assert fs.modified == dto.modified

        # FROM DTO
        fs = Fileset.from_dto(dto)
        assert isinstance(fs, Fileset)
        assert fs.id == dto.id
        assert fs.name == dto.name
        assert fs.description == dto.description
        assert fs.datasource == dto.datasource
        assert fs.workspace == dto.workspace
        assert fs.stage == dto.stage
        assert fs.filepath == dto.filepath
        assert fs.task_id == dto.task_id
        assert fs.created == dto.created
        assert fs.modified == dto.modified

        # Validate validation on from dto
        dto.datasource = "dsdsds"
        with pytest.raises(ValueError):
            _ = Fileset.from_dto(dto)

        dto.datasource = "spotify"
        dto.name = None
        with pytest.raises(TypeError):
            _ = Fileset.from_dto(dto)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )

    # ============================================================================================ #
    def test_filepath(self, caplog):
        start = datetime.now()
        logger.info(
            "\n\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        fs = Fileset(
            name="fileset_test",
            description="Fileset Test",
            datasource="spotify",
            workspace="prod",
            stage="interim",
            filepath="data/movielens25m/raw/ratings.csv",
            task_id=122,
        )
        assert fs.id is None
        assert fs.name == "fileset_test"
        assert fs.description == "Fileset Test"
        assert fs.datasource == "spotify"
        assert fs.workspace == "prod"
        assert fs.stage == "interim"
        assert fs.filepath == "data/movielens25m/raw/ratings.csv"
        assert fs.task_id == 122
        assert isinstance(fs.created, datetime)
        assert fs.modified is None
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )

    # ============================================================================================ #
    def test_id_assignment(self, caplog):
        start = datetime.now()
        logger.info(
            "\n\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        fs = Fileset(
            name="fileset_test",
            description="Fileset Test",
            datasource="spotify",
            filepath="tests/file/fileset_test.pkl",
            task_id=122,
        )
        fs.id = 501
        with pytest.raises(TypeError):
            fs.id = 73
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )

    # ============================================================================================ #
    def test_as_dict(self, caplog):
        start = datetime.now()
        logger.info(
            "\n\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        fs = Fileset(
            name="fileset_test",
            description="Fileset Test",
            datasource="spotify",
            workspace="prod",
            stage="interim",
            filepath="tests/file/fileset_test.pkl",
            task_id=122,
        )
        d = fs.as_dict()
        assert isinstance(d, dict)
        assert d["id"] is None
        assert d["name"] == "fileset_test"
        assert d["description"] == "Fileset Test"
        assert d["datasource"] == "spotify"
        assert d["workspace"] == "prod"
        assert d["stage"] == "interim"
        assert d["filepath"] == "tests/file/fileset_test.pkl"
        assert d["task_id"] == 122

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )

    # ============================================================================================ #
    def test_magic(self, caplog):
        start = datetime.now()
        logger.info(
            "\n\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        fs = Fileset(
            name="fileset_test",
            description="Fileset Test",
            datasource="spotify",
            filepath="tests/file/fileset_test.pkl",
            task_id=122,
        )
        logger.info(fs)
        logger.info(fs.__repr__)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )

    # ============================================================================================ #
    def test_equality(self, caplog):
        start = datetime.now()
        logger.info(
            "\n\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        fs1 = Fileset(
            name="fileset_test",
            description="Fileset Test",
            datasource="spotify",
            workspace="prod",
            stage="interim",
            filepath="data/movielens25m/raw/ratings.csv",
            task_id=122,
        )
        fs2 = Fileset(
            name="fileset_test",
            description="Fileset Test",
            datasource="spotify",
            workspace="prod",
            stage="interim",
            filepath="data/movielens25m/raw/ratings.csv",
            task_id=122,
        )
        fs3 = Fileset(
            name="fileset_test",
            description="Fileset Test",
            datasource="spotify",
            workspace="prod",
            stage="interim",
            filepath="data/movielens25m/raw",
            task_id=122,
        )
        assert fs1 == fs2
        assert fs1.workspace is not None
        assert not fs1 == fs3
        assert fs3.stage == "interim"
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )
