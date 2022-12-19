#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /tests/test_core/test_entity/test_dataset_collection.py                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 18th 2022 08:23:21 pm                                               #
# Modified   : Sunday December 18th 2022 09:18:41 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
from datetime import datetime
import pytest
import logging

from recsys.core.entity.dataset_collection import DatasetCollection
from recsys.core.dal.dto import DatasetCollectionDTO
import tests.containers    # noqa F401

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.dsc
class TestDatasetCollection:  # pragma: no cover
    # ============================================================================================ #
    def test_setup(self, container, caplog):
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
        ts = container.table.dataset_collection()
        ts.reset()
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
    def test_instantiation(self, caplog):
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
        dsc = DatasetCollection(
            name=inspect.stack()[0][3],
            description=f"Description for {inspect.stack()[0][3]}.",
            mode="prod",
            stage="extract",
            task_id=2
        )
        assert dsc.name == inspect.stack()[0][3]
        assert dsc.description == f"Description for {inspect.stack()[0][3]}."
        assert dsc.mode == "prod"
        assert dsc.stage == "extract"
        assert dsc.task_id == 2
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
    def test_add(self, datasets, caplog):
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
        dsc = DatasetCollection(
            name=inspect.stack()[0][3],
            description=f"Description for {inspect.stack()[0][3]}.",
            mode="prod",
            stage="extract",
            task_id=2
        )
        for dataset in datasets:
            dsc.add(dataset)

        assert len(dsc) == 5
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
    def test_remove(self, datasets, caplog):
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
        dsc = DatasetCollection(
            name=inspect.stack()[0][3],
            description=f"Description for {inspect.stack()[0][3]}.",
            mode="prod",
            stage="extract",
            task_id=2
        )
        for dataset in datasets:
            dsc.add(dataset)

        assert len(dsc) == 5

        # Test Remove
        dsc.remove(3)
        assert len(dsc) == 4
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
    def test_print(self, datasets, caplog):
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
        dsc = DatasetCollection(
            name=inspect.stack()[0][3],
            description=f"Description for {inspect.stack()[0][3]}.",
            mode="prod",
            stage="extract",
            task_id=2
        )
        for dataset in datasets:
            dsc.add(dataset)

        logger.info(dsc.print())
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
        dsc = DatasetCollection(
            name=inspect.stack()[0][3],
            description=f"Description for {inspect.stack()[0][3]}.",
            mode="prod",
            stage="extract",
            task_id=5
        )
        dto = dsc.as_dto()
        assert isinstance(dto, DatasetCollectionDTO)
        assert dto.name == inspect.stack()[0][3]
        assert dto.description == f"Description for {inspect.stack()[0][3]}."
        assert dto.mode == "prod"
        assert dto.stage == "extract"
        assert dto.task_id == 5

        dsc2 = DatasetCollection.from_dto(dto)
        assert not dsc == dsc2

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
        dsc = DatasetCollection(
            name=inspect.stack()[0][3],
            description=f"Description for {inspect.stack()[0][3]}.",
            mode="prod",
            stage="extract",
            task_id=5
        )
        a = dsc.__str__()
        b = dsc.__repr__()

        assert isinstance(a, str)
        assert isinstance(b, str)

        logger.info(a)
        logger.info(b)

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
        dsc1 = DatasetCollection(
            name=inspect.stack()[0][3],
            description=f"Description for {inspect.stack()[0][3]}.",
            mode="prod",
            stage="extract",
            task_id=5
        )
        dsc2 = DatasetCollection(
            name=inspect.stack()[0][3],
            description=f"Description for {inspect.stack()[0][3]}.",
            mode="prod",
            stage="extract",
            task_id=5
        )
        dsc3 = DatasetCollection(
            name=inspect.stack()[0][3],
            description=f"Description for {inspect.stack()[0][3]}.",
            mode="prod",
            stage="extract",
            task_id=2
        )
        dsc4 = 5
        assert dsc1 == dsc2
        assert not dsc1 == dsc3
        assert not dsc1 == dsc4
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
    def test_task_id(self, caplog):
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
        dsc1 = DatasetCollection(
            name=inspect.stack()[0][3],
            description=f"Description for {inspect.stack()[0][3]}.",
            mode="prod",
            stage="extract",
            task_id=None
        )
        dsc1.task_id = 56
        with pytest.raises(TypeError):  # No reassignment allowed, unless current value is None
            dsc1.task_id = 89

        with pytest.raises(TypeError):
            _ = DatasetCollection(
                name=inspect.stack()[0][3],
                description=f"Description for {inspect.stack()[0][3]}.",
                mode="prod",
                stage="extract",
                task_id='xpso'
            )

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
