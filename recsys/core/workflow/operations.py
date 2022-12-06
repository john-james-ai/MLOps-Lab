#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/workflow/operations.py                                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 5th 2022 10:32:14 pm                                                #
# Modified   : Monday December 5th 2022 10:32:53 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems and Deep Learning in Python                                     #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /data.py                                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems-and-Deep-Learning-in-Python    #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday November 12th 2022 07:42:45 pm                                             #
# Modified   : Saturday November 12th 2022 08:29:18 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Data Utility Module"""
import pandas as pd
from numpy.random import default_rng
import logging

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


def clustered_sample(
    df: pd.DataFrame,
    by: str,
    frac: float = None,
    n: int = None,
    replace: bool = False,
    shuffle: bool = True,
    ignore_index: bool = False,
    random_state: int = None,
):
    """Performs sampling by cluster.
    Args:
        df (pd.Dataframe): The pandas dataframe to sample
        by (str): The column by which to cluster
        frac (float): The fraction of total to sample. Cannot be used with n.
        n (int): The number of samples to return. Cannot be used with frac.
        replace (bool): Allow or disallow sampling of the same row more than once. Default is False
        ignore_index (bool): If True, the resulting index will be labeled 0, 1, …, n - 1.
        random_state (int): Seed for pseudo randomization.
    Returns: DataFrame
    """
    if frac is None and n is None:
        # Defaults to single sample cluster.
        n = 1
    elif frac and n:
        logger.error("Must provide frac or n, not both")
        raise ValueError("Must provide frac or n, not both")
    elif frac:
        if frac > 1:
            logger.error("frac must be a float in (0,1]")
            raise ValueError("frac must be a float in (0,1]")

    rng = default_rng(random_state)

    try:
        clusters = df[by].unique()
        n_clusters = len(clusters)
        size = n if n else int(n_clusters * frac)
        sample_clusters = rng.choice(a=clusters, size=size, replace=replace, shuffle=shuffle)
        sample = df.loc[df[by].isin(sample_clusters)]
        return sample
    except KeyError:
        logger.error("The dataframe has no column {}".format(by))
        raise KeyError("The dataframe has no column {}".format(by))
