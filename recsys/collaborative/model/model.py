#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /model.py                                                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday November 13th 2022 03:38:50 am                                               #
# Modified   : Tuesday November 15th 2022 11:56:00 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import logging
import pandas as pd
import numpy as np
from tqdm import tqdm

from recsys.core.services.io import IOService

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
tqdm.pandas()  # Supports progress monitoring of pandas groupby operations using tqdm
# ------------------------------------------------------------------------------------------------ #


class CollaborativeFilterRecommender:
    """Computes scores or movie ratings for each user.

    Args:
        users_filepath (str): The path to the file containing user bias information.
        ratings_filepath (str): The path to the adjusted ratings file.
        weights_filepath (str): The path to the user-user weights file.
        predictions_filepath (str): The path to which predictions will be stored.
        n_neighbors (int): The number of neighbors to use in the scoring calculation. Default = 50.
        rank_weights_by_absolute_value (bool): If True, neighbors are ranked by the absolute
            value of the weight.
        IOService (IOService): The factory that returns the required io object.
        force (bool): If True, overwrite existing TrainTest splits

    """

    def __init__(
        self,
        users_filepath: str,
        ratings_filepath: str,
        weights_filepath: str,
        predictions_filepath: str,
        n_neighbors: int = 50,
        rank_weights_by_absolute_value: bool = False,
        IOService: IOService = IOService(),
        force: bool = False,
    ) -> None:
        name = self.__class__.__name__.lower()
        super().__init__(
            name=name,
            users_filepath=users_filepath,
            ratings_filepath=ratings_filepath,
            weights_filepath=weights_filepath,
            predictions_filepath=predictions_filepath,
            n_neighbors=n_neighbors,
            rank_weights_by_absolute_value=rank_weights_by_absolute_value,
            IOService=IOService,
            force=force,
        )
        self._io = self.IOService.create("pkl")
        self._users = None
        self._ratings = None
        self._weights = None
        self._predictions = None

    def predict(self, X: pd.DataFrame) -> None:
        """Executes the operation

        Args:
            X (pd.DataFrame): A DataFrame containing the users
                and movies (items) for which rating predictions are to be made.
        """
        self._load_data()

        # Rank and select the top N neighbors
        weights = self._rank_select_neighbors()
        # Merge user, rating, and weight data.
        X = self._merge_data(X, weights)
        # Compute prediction
        predictions = self._predict(X)
        # Store Predictions
        self._save_data(predictions)

        return predictions

    def score(self, X: pd.DataFrame, y: np.array) -> int:
        """Computes MSE of predictions

        Args:
            X (pd.DataFrame): Dataframe containing user and items to predict.
            y (np.array): The True ratings for X

        Returns: MSE score (int)
        """
        y_hat = self.predict(X)
        return np.mean(np.square(y - y_hat))

    def _load_data(self) -> None:
        """Loads user, ratings, and weights data"""

        self._users = self._io.read(self.users_filepath)
        self._ratings = self._io.read(self.ratings_filepath)
        self._weights = self._io.read(self.weights_filepath)

    def _save_data(self, predictions: pd.DataFrame) -> None:
        """Saves the predictions to file."""
        self._io.write(self.prepredictions_filepath, predictions)

    def _rank_select_neighbors(self, weights: pd.DataFrame) -> pd.DataFrame:
        """Ranks each neighbor by weight.

        Ranking is performed by weight, or by the absolute value
        of the weight, as per user parameter.

        Args:
            weights (pd.DataFrame): The user neighbor weights.
        """
        if self.rank_weights_by_absolute_value:
            weights["weight_abs"] = np.abs(weights["weight"])
            weights["rank"] = weights.groupby("user")["weight_abs"].rank(
                method="first", ascending=False
            )
            weights.drop(columns=["weight_abs"], inplace=True)
        else:
            weights["rank"] = weights.groupby("user")["weight"].rank(
                method="first", ascending=False
            )

        weights.sort_values(by=["user", "rank"], ascending=True, inplace=True, ignore_index=True)

        weights = weights.loc[weights["rank"] <= self.n_neighbors]

        return weights

    def _merge_data(self, X: pd.DataFrame, weights: pd.DataFrame) -> pd.DataFrame:
        """Merges user, rating, and weight data."""
        # Add the user's bias to the prediction dataframe.
        X = X.merge(self._users, how="left", on="userId")
        # Add neighbors and weights
        X = X.merge(weights, how="left", on="userId")
        # Merge the ratings for the neighbors into the dataframe
        X = X.merge(self._ratings, how="left", left_on="neighbor", right_on="userId")
        return X

    def _predict(self, X) -> pd.DataFrame:

        predictions = (
            X.groupby(["userId", "movieId"])
            .progress_apply(
                lambda x: (
                    x["mean_rating"]
                    + (np.dot(x["weight"], x["adj_rating"] / np.sum(np.abs(x["weight"]))))
                )
            )
            .reset_index()
        )
        return predictions
