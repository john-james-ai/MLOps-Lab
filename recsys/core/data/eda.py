#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /eda.py                                                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 10th 2022 06:19:23 pm                                             #
# Modified   : Sunday November 27th 2022 10:01:54 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import pandas as pd
import logging
import seaborn as sns
import matplotlib.pyplot as plt

from recsys.core.services.io import IOService
from recsys.config.base import VisualConfig

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class MovieLensRatings:
    """Encapsulates Movielens25M Dataset

    Args:
        filepath (str): The filepath to the ratings file.
        fileformat (str): Format in which the dataset is stored
        IOService (IOService): The factory object that returns the IO object.
    """

    def __init__(
        self,
        filepath: str,
        io: IOService = IOService,
    ) -> None:
        self._filepath = filepath
        self._io_factory = IOService
        self._io = io

        self._ratings = None
        # Stats
        self._n_ratings = None
        self._n_users = None
        self._n_items = None
        self._average_num_ratings_per_user = None
        self._average_num_ratings_per_item = None
        self._summary = None
        self._ratings_stats = None
        self._user_rating_counts = None
        self._user_rating_counts_stats = None
        self._item_rating_counts = None
        self._item_rating_counts_stats = None
        self.load()

    @property
    def ratings(self) -> pd.DataFrame:
        return self._ratings

    @property
    def n_ratings(self) -> int:
        if not self._n_ratings:
            self._n_ratings = self._ratings.shape[0]
        return self._n_ratings

    @property
    def n_users(self) -> int:
        if not self._n_users:
            self._n_users = self._ratings["userId"].nunique()
        return self._n_users

    @property
    def n_items(self) -> int:
        if not self._n_items:
            self._n_items = self._ratings["movieId"].nunique()
        return self._n_items

    @property
    def average_num_ratings_per_user(self) -> float:
        if not self._average_num_ratings_per_user:
            self._average_num_ratings_per_user = round(
                self._ratings.groupby(["userId"])["movieId"].count().mean(), 2
            )
        return self._average_num_ratings_per_user

    @property
    def average_num_ratings_per_item(self) -> float:
        if not self._average_num_ratings_per_item:
            self._average_num_ratings_per_item = round(
                self._ratings.groupby(["movieId"])["userId"].count().mean(), 2
            )
        return self._average_num_ratings_per_item

    @property
    def summary(self) -> pd.DataFrame:
        if not self._summary:
            d = {
                "Ratings": self.n_ratings,
                "Users": self.n_users,
                "Items": self.n_items,
                "Ave Num Ratings per User": self.average_num_ratings_per_user,
                "Ave Num Ratings per Item": self.average_num_ratings_per_item,
            }
            self._summary = pd.DataFrame(data=d, index=["Statistic"])
        return self._summary.T

    @property
    def ratings_stats(self) -> pd.DataFrame:
        if not self._ratings_stats:
            self._ratings_stats = self._ratings["rating"].describe().T
        return self._ratings_stats

    @property
    def user_rating_stats(self) -> pd.DataFrame:
        if not self._user_rating_counts:
            self._user_rating_counts = (
                self._ratings.groupby(["userId"])["rating"].count().to_frame()
            )
        return self._user_rating_counts.describe().T

    @property
    def item_rating_stats(self) -> pd.DataFrame:
        if not self._item_rating_counts:
            self._item_rating_counts = (
                self._ratings.groupby(["movieId"])["rating"].count().to_frame()
            )
        return self._item_rating_counts.describe().T

    def load(self) -> None:
        self._ratings = self._io.read(self._filepath)

    def info(self) -> None:
        self._ratings.info(show_counts=True, memory_usage=True)

    def head(self, n: int = 5) -> None:
        return self._ratings.head(n)

    def ratings_plot(
        self,
        ax: plt.axes = None,
        show: bool = True,
        title: str = "Distribution of Ratings\n(000,000's)",
    ) -> None:
        """Plots the distribution of ratings

        Args:
            ax (plt.axes): Matplotlib axes object.
            show (bool): Whether to show the plot
            title (str): Title for plot
        """
        if ax is None:
            fig, ax = plt.subplots(figsize=VisualConfig.figsize)
        ax = sns.histplot(data=self._ratings["rating"], bins=10, ax=ax, palette="Blues_r")
        ax.set_title(title)
        ax.set_xlabel("Rating")
        if show:
            plt.show()
        return ax

    def user_rating_count_plot(
        self,
        ax: plt.axes = None,
        show: bool = True,
        title: str = "Distribution of User Rating Counts",
    ) -> None:
        """Plots distribution of user rating counts

        Args:
            ax (plt.axes): Matplotlib axes object.
            show (bool): Whether to show the plot
            title (str): Title for plot
        """
        if not self._user_rating_counts:
            self._user_rating_counts = (
                self._ratings.groupby(["userId"])["rating"].count().to_frame()
            )
        if ax is None:
            fig, ax = plt.subplots(figsize=VisualConfig.figsize)
        ax = sns.histplot(
            data=self._user_rating_counts["rating"], bins=100, ax=ax, palette="Blues_r"
        )
        ax.set_title("Distribution of User Rating Counts")
        ax.set_xlabel("User Rating Counts")
        if show:
            plt.show()
        return ax

    def item_rating_count_plot(
        self,
        ax: plt.axes = None,
        show: bool = True,
        title: str = "Distribution of item Rating Counts",
    ) -> None:
        """Plots distribution of item rating counts

        Args:
            ax (plt.axes): Matplotlib axes object.
            show (bool): Whether to show the plot
            title (str): Title for plot
        """
        if not self._item_rating_counts:
            self._item_rating_counts = (
                self._ratings.groupby(["movieId"])["rating"].count().to_frame()
            )
        if ax is None:
            fig, ax = plt.subplots(figsize=VisualConfig.figsize)
        ax = sns.histplot(
            data=self._item_rating_counts["rating"], bins=100, ax=ax, palette="Blues_r"
        )
        ax.set_title("Distribution of item Rating Counts")
        ax.set_xlabel("item Rating Counts")
        if show:
            plt.show()
        return ax
