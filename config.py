"""This file contains the following configuration variables to be used 
throughout modules in this project:
- CWD, TMPDIR:  paths to directories
- StateConfig:  class storing scrape configuration for a single state
- CONFIG:   dictionary of state_abbrevation:StateConfig_object pairs
- name2abbrev and abbrev2name:  dictionaries to look up state's abbreviation
                                given its name, and vice versa
"""

import os
import numpy as np
import yaml
import ast
import re
import pandas as pd
import logging

from modules.utils import import_yaml, to_bool, lower_and_underscore, get_valid_uri


# =============================================================================
# Directories and files
# =============================================================================
CWD = os.path.dirname(__file__)
TMPDIR = os.path.join(CWD, 'tmp')
SAVEDIR = TMPDIR # TODO: update this from external file or command line

if not os.path.isdir(TMPDIR):
    os.mkdir(TMPDIR)


# =============================================================================
# Internal variables/methods: These should not be accessed outside of config.py
# =============================================================================

# Import configuration files
_cfg = import_yaml(os.path.join(CWD, 'config.yml'))
_df_ids = pd.read_csv(os.path.join(CWD, _cfg['files']['state_ids']))
_df_cfg = pd.read_csv(os.path.join(CWD, _cfg['files']['scrape_cfg']))


def _create_config_dict():
    """Create configuration dictionary from imported information
    on each state's WARN databases, including URLs, field names, 
    and how to process the data.
    
    Returns:
    - configs:  dictionary of StateConfig objects, one for each row in 
                the scrape_cfg file
    """

    # Add FPIS state-abbreviation information to configuration files
    df = pd.merge(_df_cfg, _df_ids, how='inner', on=['State'])

    # Clean and normalize the configuration data
    df.columns = df.columns.to_series().apply(lower_and_underscore)
    df.set_index('abbreviation', inplace=True) 
    df.rename(columns={'state':'state_name'}, inplace=True)
    df['format'] = df['format'].apply(lower_and_underscore)
    df['uses_joblink_interface?'] = df['uses_joblink_interface?'].apply(to_bool)
    df['archive_url'] = df['archive_url'].apply(lambda x: get_valid_uri(x))

    # Create dictionary of scrape configurations to run
    configs = {index: StateConfig(row.to_dict()) for index, row in df.iterrows()}

    return configs


# =============================================================================
# Configuration settings and state lookup dictionaries
# =============================================================================

# Dictionaries to look up state abbreviation given name and vice versa
name2abbrev = {row['State']:row['Abbreviation'] for index, row in _df_ids.iterrows()}
abbrev2name = {row['Abbreviation']:row['State'] for index, row in _df_ids.iterrows()}


class StateConfig():
    """Store configuration information for an individual state

    This configuration is used to instantiate a WARNSpider class
    or any of its child classes.
    """
    
    def __init__(self, row):
        """Import configuration from a dictionary.
        
        Parameters:
        - row:  dictionary of parameters values to set
        """
        
        # Set required scraping configurations
        try:
            self.STATE_NAME = row['state_name']
            self.STATE_ABBREV = name2abbrev[self.STATE_NAME]
            self.URL = row['archive_url']
            self.FORMAT = row['format']
        except KeyError:
            logging.warning(f"Could not find all necessary keys in entry {row}")
            self.STATE_NAME = None

        # Set optional scraping configurations
        self.STATUS = row.get('current/archive') # TODO: refactor
        self.IS_JOBLINK = row.get('uses_joblink_interface?') # TODO: remove

        # Set field name normalization key-value pairs
        suffix = "_field"
        self.FIELDS = {(re.sub(suffix, '', key)).upper(): value for key, value in row.items() if key.endswith(suffix)}

    def display(self):
        for key, value in vars(self).items():
            print(f"{key}: {value}")


CONFIG  = _create_config_dict()


# =============================================================================
# When testing configuration import, display results
# =============================================================================
if __name__=="__main__":
    print(f"Imported scrape configuration for the following states:\n{CONFIG.keys()}")
    print(f"\nColumns in example state_config:\n{CONFIG['AL'].state_config.keys()}")
    print("\nExample CONFIG entry:")
    CONFIG['AL'].display()
