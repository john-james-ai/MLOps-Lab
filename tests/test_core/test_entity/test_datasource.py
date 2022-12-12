#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Dataname   : /tests/test_core/test_entity/test_datasource.py                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday December 7th 2022 10:37:56 am                                             #
# Modified   : Sunday December 11th 2022 03:25:47 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
from datetime import datetime
import pytest
import logging

from recsys.core.entity.datasource import DataSource
from recsys.core.entity.fileset import Fileset
from recsys.core.dal.dto import DataSourceDTO, FilesetDTO

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.source
class TestDataSourceEntity:  # pragma: no cover
    def build_datasource(self, method):
        ds = DataSource(
            name="spotify",
            description=f"Description of datasource_{method}",
            publisher="GroupLens",
            website="https://grouplens.org/datasets/movielens/"
        )

        for i in range(1, 6):
            fs = Fileset(
                name=f"datasource_{method}_fileset_{i}",
                description=f"Description of datasource_{method}_fileset_{i}",
                datasource="movielens25m",
                uri="www.movielens.com",
                workspace="remote",
                stage="ext",
                task_id=0,
            )
            ds.add_fileset(fs)

        return ds

    # ============================================================================================ #
    def test_datasource_build(self, caplog):
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
        ds = self.build_datasource(method=inspect.stack()[0][3])

        assert ds.name == "spotify"
        assert ds.description == f"Description of datasource_{inspect.stack()[0][3]}"
        assert ds.publisher == "GroupLens"
        assert ds.website == "https://grouplens.org/datasets/movielens/"
        assert isinstance(ds.filesets, list)
        assert len(ds.filesets) == 5
        assert isinstance(ds.created, datetime)

        for i, fs in enumerate(ds.filesets, start=1):
            assert fs.name == f"datasource_{inspect.stack()[0][3]}_fileset_{i}"
            assert fs.description == f"Description of datasource_{inspect.stack()[0][3]}_fileset_{i}"
            assert fs.datasource == "movielens25m"
            assert fs.uri == "www.movielens.com"
            assert fs.workspace == "remote"
            assert fs.stage == "ext"
            assert fs.task_id == 0

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
    def test_instantiation_validation(self, caplog):
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
        with pytest.raises(TypeError):  # No name
            _ = DataSource()

        with pytest.raises(TypeError):  # No publisher
            _ = DataSource(name="johosephat")

        with pytest.raises(TypeError):
            _ = DataSource(name="johosephat", publisher="spotify")  # website

        with pytest.raises(ValueError):
            _ = DataSource(name="johosephat", publisher="spotify", website="https://grouplens.org/datasets/movielens/")  # Invalid publisher

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
    def test_dto_conversion(self, datasource_dtos, caplog):
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
        ds = self.build_datasource(method=inspect.stack()[0][3])

        dto = ds.as_dto()
        assert dto.name == "spotify"
        assert dto.description == f"Description of datasource_{inspect.stack()[0][3]}"
        assert dto.publisher == "GroupLens"
        assert dto.website == "https://grouplens.org/datasets/movielens/"
        assert isinstance(dto.created, datetime)
        assert isinstance(dto.filesets, list)
        assert len(dto.filesets) == 5
        assert isinstance(dto, DataSourceDTO)

        for i, dto in enumerate(dto.filesets, start=1):
            assert isinstance(dto, FilesetDTO)
            assert dto.name == f"datasource_{inspect.stack()[0][3]}_fileset_{i}"
            assert dto.description == f"Description of datasource_{inspect.stack()[0][3]}_fileset_{i}"
            assert dto.datasource == "movielens25m"
            assert dto.uri == "www.movielens.com"
            assert dto.workspace == "remote"
            assert dto.stage == "ext"
            assert dto.task_id == 0
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
    def test_dto_instantiation(self, datasource_dto, caplog):
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
        ds = DataSource.from_dto(datasource_dto)
        logger.debug(f"\n\n\tDataSource has {len(ds.filesets)} filesets.")

        assert ds.id == datasource_dto.id
        assert ds.name == datasource_dto.name
        assert ds.publisher == datasource_dto.publisher
        assert ds.description == datasource_dto.description
        assert ds.website == datasource_dto.website
        assert ds.created == datasource_dto.created
        assert ds.modified == datasource_dto.modified

        for i in range(len(ds.filesets)):
            logger.debug(f"\t\tEvaluating the {i}th Fileset.")
            assert ds.filesets[i] == Fileset.from_dto(datasource_dto.filesets[i])

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
    def test_dto_instantiation_validation(self, datasource_dtos, caplog):
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
        for i, dto in enumerate(datasource_dtos, start=1):
            if i == 1:
                dto.name = None  # Name
                with pytest.raises(TypeError):
                    DataSource.from_dto(dto)

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
    def test_as_dict(self, ratings, caplog):
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
        ds = self.build_datasource(method=inspect.stack()[0][3])
        d = ds.as_dict()
        assert isinstance(d, dict)
        for fileset in d['filesets']:
            assert isinstance(fileset, dict)

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
    def test_magic(self, datasources, caplog):
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
        ds = datasources[0]
        assert isinstance(ds.__str__(), str)
        assert isinstance(ds.__repr__(), str)

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

    def test_equality(self, datasources, caplog):
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
        ds1 = datasources[0]
        ds2 = datasources[0]
        ds3 = datasources[1]
        ds4 = {'d': "somedict"}
        assert ds1 == ds2
        assert not ds1 == ds3
        assert not ds1 == ds4

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
