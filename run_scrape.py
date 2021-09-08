import os
import sys
import pytest
import re
import argparse
import logging
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings

from config import CONFIG
from modules.spiders.utils import get_spider

#Set Logging out to a text file
logging.basicConfig(filename="state.log",
                            filemode='a',
                            level=logging.DEBUG
                            ) 

def main(state=None, overwrite=False):
    """Run WARN scrape

    Runs all states in CONFIG unless one is specified; by default, appends to existing .jsonl file.
    
    Parameters:
    - state:    string (e.g., 'AL') specifying abbreviation of state to run, if only one
    - overwrite:    TODO: make this do anything
    """
    
    # Initialize process
    process = initialize_process()

    if overwrite:
        # TODO: delete all (and only) files to be regenerated
        pass

    # Add a spider instance for each state to be run
    if state:
        add_state_to_process(state, process=process)
    else:
        for s in list(CONFIG.keys()):
            add_state_to_process(s, process=process)

    # Run scrape
    process.start()


def initialize_process():
    """Define process settings and return new CrawlerProcess
    
    To provide a new Item Pipeline for a different export format, include it as 
    part of the ITEM_PIPELINES dictionary (the number value determines priority)

    NOTE: USER_AGENT will eventually need to be updated, as that browser becomes outdated.

    TODO: move these settings to an external .yml file?   
    """

    settings = Settings({'BOT_NAME': 'warnnoticebot',
                         'LOG_LEVEL': 'INFO',
                         'ITEM_PIPELINES': {'modules.pipelines.PerStateJsonlinesExportPipeline': 300},
                         'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36', # This is my actual user agent when using a browser
                         'COOKIES_ENABLED': False,
                         'ROBOTSTXT_OBEY': True,
                         'DOWNLOAD_DELAY': 5.0,
                         'DEFAULT_REQUEST_HEADERS': {
                             'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                             'Accept-Language': 'en',
                             'Upgrade-Insecure-Requests': 1}
                         })
    
    process = CrawlerProcess(settings)   

    return process


def add_state_to_process(state, process=None):
    """Given two-letter state abbreviation, add corresponding Spider to process
    
    Initializes a new process if none has been created yet.
    """

    isNewProcess = False
    if not process:
        isNewProcess = True
        process = initialize_process
    
    # Only crawl spider if url is valid
    if CONFIG[state].URL:
        print(CONFIG[state].URL)
        spider = get_spider(state)   
        process.crawl(spider, state_config=CONFIG[state])
    else:
        logging.warning(f"Skipping {state}: no URL or invalid URL provided")

    if isNewProcess:
        return process


if __name__=="__main__":
    # Add command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--state", choices=list(CONFIG.keys()),
                        help="Scrape only a particular state (use abbreviation)")
    parser.add_argument("-o", "--overwrite", action='store_true',
                        help="If included, overwrites existing feed (else, appends). Placeholder - not yet implemented")
    args = parser.parse_args()

    # Run
    main(args.state, args.overwrite)