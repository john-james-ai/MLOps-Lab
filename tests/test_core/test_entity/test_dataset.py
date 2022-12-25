#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Dataname   : /tests/test_core/test_entity/test_dataset.py                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday December 7th 2022 10:37:56 am                                             #
# Modified   : Sunday December 25th 2022 12:37:19 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
from datetime import datetime
import pandas as pd
import pytest
import logging

from recsys.core.entity.dataset import Dataset, Datasets
from recsys.core.dal.dto import DatasetDTO

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.dse
class TestDatasetEntity:  # pragma: no cover
    # ============================================================================================ #
    def test_instantiation_no_data(self, caplog):
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
        ds = Dataset(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            mode="test",
            task_id=22,
            stage="extract",
        )
        dss = Datasets(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            mode="test",
            task_id=22,
            stage="extract",
        )
        dss.add(ds)
        assert len(dss) == 1
        assert ds.name == inspect.stack()[0][3]
        assert ds.description == f"Description of {inspect.stack()[0][3]}"
        assert ds.datasource == "movielens25m"
        assert ds.size is None
        assert ds.nrows is None
        assert ds.ncols is None
        assert ds.nulls is None
        assert ds.pct_nulls is None
        assert ds.task_id == 22
        assert isinstance(ds.created, datetime)

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
    def test_instantiation_with_data(self, ratings, caplog):
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
        ds = Dataset(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            data=ratings,
            mode="test",
            task_id=22,
            stage="extract",
        )
        assert ds.name == inspect.stack()[0][3]
        assert ds.description == f"Description of {inspect.stack()[0][3]}"
        assert ds.datasource == "movielens25m"
        assert ds.task_id == 22
        assert ds.size is not None
        assert ds.nrows is not None
        assert ds.ncols is not None
        assert ds.nulls is not None
        assert ds.pct_nulls is not None
        assert isinstance(ds.created, datetime)
        assert isinstance(ds.data, pd.DataFrame)

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
            _ = Dataset()

        with pytest.raises(TypeError):  # No datasource
            _ = Dataset(name="johosephat", stage="extract")

        with pytest.raises(TypeError):
            _ = Dataset(name="johosephat", datasource="spotify")  # No task_id

        with pytest.raises(TypeError):
            _ = Dataset(name="johosephat", datasource="spotify", task_id=232)  # No stage

        with pytest.raises(TypeError):
            _ = Dataset(name="johosephat", datasource="spotify", task_id=232, stage="extract")  # No mode

        with pytest.raises(ValueError):
            _ = Dataset(
                name="johosephat", datasource="spotify", task_id=232, stage="rafas", mode="test",
            )  # Invalid stage

        with pytest.raises(TypeError):
            _ = Dataset(
                name="johosephat", datasource="spotify", task_id="232", stage="raw", mode="test",
            )  # Invalid task_id

        with pytest.raises(ValueError):
            _ = Dataset(
                name="johosephat", datasource="dfas", task_id=232, stage="raw", mode="test",
            )  # Invalid datasource

        with pytest.raises(ValueError):
            _ = Dataset(
                name="johosephat", datasource="spotify", task_id=232, stage="raw", mode="towt",
            )  # Invalid mode

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
        ds = Dataset(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            task_id=22,
            mode="test",
            data=ratings,
            stage="extract",
        )
        d = ds.as_dict()
        assert isinstance(d, dict)
        assert d["id"] is None
        assert d["name"] == inspect.stack()[0][3]
        assert d["description"] == f"Description of {inspect.stack()[0][3]}"
        assert d["datasource"] == "movielens25m"
        assert d["mode"] == "test"
        assert d["stage"] == "extract"
        assert d["task_id"] == 22
        assert isinstance(d["created"], datetime)
        assert d["modified"] is None

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
    def test_as_dto(self, ratings, caplog):
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
        ds = Dataset(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            mode="test",
            task_id=22,
            data=ratings,
            stage="extract",
        )
        dto = ds.as_dto()
        assert isinstance(dto, DatasetDTO)
        assert dto.id is None
        assert dto.name == inspect.stack()[0][3]
        assert dto.description == f"Description of {inspect.stack()[0][3]}"
        assert dto.datasource == "movielens25m"
        assert dto.mode == "test"
        assert dto.stage == "extract"
        assert dto.task_id == 22
        assert isinstance(dto.created, datetime)
        assert dto.modified is None

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
    def test_access_methods(self, ratings, caplog):
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
        ds = Dataset(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            task_id=22,
            mode="test",
            data=ratings,
            stage="extract",
        )
        logger.info(ds.info())
        assert isinstance(ds.head(), pd.DataFrame)
        assert isinstance(ds.tail(), pd.DataFrame)
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
    def test_access_methods_no_data(self, ratings, caplog):
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
        ds = Dataset(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            task_id=22,
            mode="test",
            stage="extract",
        )

        assert ds.info() is None
        assert ds.head() is None
        assert ds.tail() is None
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
    def test_set_once(self, ratings, caplog):
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
        ds = Dataset(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            task_id=22,
            mode='test',
            stage="interim",
        )
        ds.id = 5
        with pytest.raises(TypeError):
            ds.id = 6

        ds.data = ratings
        with pytest.raises(TypeError):
            ds.data = ratings

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
    def test_modified(self, ratings, caplog):
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
        ds = Dataset(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            task_id=22,
            mode='test',
            stage="raw",
        )
        modified = ds.modified
        ds.id = 9
        assert ds.modified != modified
        modified = ds.modified

        ds.data = ratings
        assert ds.modified != modified

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

    def test_equality(self, ratings, caplog):
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
        ds1 = Dataset(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            task_id=22,
            mode='test',
            stage="extract",
            data=ratings,
        )
        ds2 = Dataset(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            mode="dev",
            task_id=22,
            stage="extract",
        )
        assert not ds1 == ds2

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
    def test_data(self, ratings, caplog):
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
        ds1 = Dataset(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            task_id=22,
            mode="dev",
            stage="extract",
            data=ratings,
        )
        assert isinstance(ds1.data, pd.DataFrame)

        ds2 = Dataset(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            mode="prod",
            task_id=22,
            stage="extract",
        )
        with pytest.raises(TypeError):
            ds2.data = {'some': 'dictionary'}
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
    def test_task_id_assignment(self, ratings, caplog):
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
        ds1 = Dataset(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            stage="extract",
            mode="prod",
            data=ratings,
        )
        ds1.task_id = 75
        with pytest.raises(TypeError):
            ds1.task_id = 76

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
    def test_component(self, ratings, caplog):
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
        ds1 = Dataset(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            stage="extract",
            mode="prod",
            data=ratings,
        )

        ds2 = Dataset(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            stage="extract",
            mode="prod",
            data=ratings,
        )

        dss1 = Datasets(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            stage="extract",
            mode="prod",
        )

        dss2 = Datasets(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            stage="extract",
            mode="prod",
        )
        dss3 = 5

        ds1.parent = dss1
        assert isinstance(ds1.parent, Datasets)
        assert ds1.is_composite is False
        assert dss1.is_composite is True
        assert len(ds1) == 0

        with pytest.raises(TypeError):
            ds1.task_id = 'str'

        with pytest.raises(TypeError):
            ds1.parent = ds2

        assert ds1 == ds2
        assert dss1 == dss2
        assert ds1 != dss2
        assert dss2 != dss3

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
    def test_datasets(self, datasets, caplog):
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
        dss = Datasets(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            stage="extract",
            mode="prod",
        )
        for dataset in datasets:
            dss.add(dataset)
        dto = dss.as_dto()
        assert len(dss) == 5
        if isinstance(dto, dict):
            for i, (k, v) in enumerate(dto.items()):
                if isinstance(v, Dataset):
                    assert v.name == datasets[i].name
                    assert v.description == datasets[i].description
                    assert v.datasource == datasets[i].datasource
                    assert v.stage == datasets[i].stage
                    assert v.mode == datasets[i].mode

        for dataset in datasets:
            dss.remove(dataset)
        assert len(dss) == 0

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
