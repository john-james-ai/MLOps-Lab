#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /jbook/prep_notebooks.py                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday November 27th 2022 10:50:44 pm                                               #
# Modified   : Friday December 9th 2022 06:50:29 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import nbformat as nbf
from glob import glob
import logging
import logging.config
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.WARN)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


def prepare_notebooks():
    # Collect a list of all notebooks in the designated folder
    logging.info("\tPreparing Notebook Metadata")
    notebooks = glob("./**/*.ipynb", recursive=True)

    # Userful tags
    # Two types of tags, hide and remove.
    #   Hide provides a button to reveal the cell contents
    #   Remove prevents the content from appearing in the HTML at all.
    # Hide Tags:
    #   "hide-input": Hides the cell but displays the output
    #   "hide-output": Hides the output from a cell, but provides a button to show
    #   "hide-cell": Hides both input and output
    # Remove Tags:
    #    "remove-input": Removes cell from HTML, but shows ouput. No botton available
    #    "remove-output": Removes cell output from HTML. No botton
    #    "remove-cell": Removes entire cell, input and output. No botton.
    #
    # remove-cell: remove entire cell
    #

    # Text to look for in adding tags
    text_search_dict = {
        "# Imports": "hide-cell",  # Removes the 'module not found' error from output
        "# FILEPATHS": "hide-cell",  # Removes the 'module not found' error from output
        "# GLUE": "remove-cell",  # Removes the cell (input/output) which declares glue variables
        "# HIDE-INPUT": "hide-input",  # Collapse input with toggle to display
        "# Constants": "hide-input",  # Collapses input with toggle to display
        "# HIDE-OUTPUT": "hide-output",  # Collapse output with toggle to display
        "# HIDE-CELL": "hide-cell",  # Collapse input and output with toggle to display
        "# REMOVE-INPUT": "remove-input",  # Removes input, no toggle option
        "# REMOVE-OUTPUT": "remove-output",  # Removes output, no toggle option
        "# REMOVE-CELL": "remove-cell",  # Removes input and output, no toggle option
        "# %load": "hide-cell",  # Hides cells containing source loaded via ipython magic function.
    }

    # Search through each notebook and look for the text, add a tag if necessary
    i = 0
    for ipath in notebooks:
        i += 1
        logging.info("\t\tProcessing Notebook {}".format(str(i)))
        ntbk = nbf.read(ipath, nbf.NO_CONVERT)

        for cell in ntbk.cells:
            cell_tags = cell.get("metadata", {}).get("tags", [])
            for key, val in text_search_dict.items():
                if key in cell["datasource"]:
                    if val not in cell_tags:
                        cell_tags.append(val)
            if len(cell_tags) > 0:
                cell["metadata"]["tags"] = cell_tags

        nbf.write(ntbk, ipath)

    logging.info("\tNotebook Metadata Processed")


if __name__ == "__main__":
    prepare_notebooks()
