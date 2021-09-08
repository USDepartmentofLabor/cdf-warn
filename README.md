# cdf-warn: Civic Digital Fellowship WARN Archive Scraping
Scrape, parse, and normalize WARN notice archives from every state with public databases (or local files sent to BLS by state officials).

## Overview
Two main goals of this project which have not been addressed by previous efforts to scrape WARN notices include:
- consolidating the scraping of individual state websites so that all states could be updated at once, and
- normalizing data so that aggregate analyses across all states can be done easily.

This code is being written with maintainability in mind, given challenges such as:
- individual states make their data public in a variety of formats (HTML tables, PDF tables, Excel sheets, Google Sheets) and with differing amounts of detail about the individual WARN notices, and that
- individual states' websites may change arbitrarily.

To consolidate and streamline the scraping (accessing web content) and exporting (saving parsed WARN archive data) processes, we used the web scraping framework [Scrapy](https://github.com/scrapy/scrapy), with occasional use of [Selenium](https://selenium-python.readthedocs.io/) for dynamic web pages. This allowed us to define a `Spider` for each state, which inherits from a custom `Spider` class which includes scraping/parsing methods common to most states. This also enabled the definition of a standardized `Item` and corresponding `Pipeline`, allowing all states' WARN Notice data to be exported in an common format (currently, as [JSON Lines](https://jsonlines.org/), with intermediate `.csv` files generated for debugging).

When a state's spider is run, the spider will get the website (or local file) content, follow any links if necessary (e.g., for yearly PDF archive files), and then parse the content as a pandas DataFrame. To reduce code duplication and provide layers of abstraction, we built a small, flexible library of parsing functions for HTML tables, PDF tables, Excel sheets, and CSV files (defined in `modules/datatodf`, i.e., "data-to-DataFrame"). Where possible, each state uses one of these functions, with changes in the keyword parameters if necessary. If that is not sufficient, a custom state-specific parsing function is defined (in `modules/customdatatodf`) and provided to override the default parsing function.

Some of the code maintenance can be done outside of the Python code, in the `config.yml` and `config/warn_config.csv` files. The `warn_config.csv`, intended to be editable in Excel, is imported by `config.py` to update the URL/filepath for each state, default parsing format, and column name normalization information. If you need multiple versions of this file (e.g., when you want to scrape from a local file instead of the website), the `config.yml` file can be used to designate a different `warn_config.csv` file to import. 

## How to install
- Install Anaconda, and Git. 
- Clone the repository in the directory of your choice
- Create a Python environment based on the package requirements file:
    `conda env create --file environment.yml -n warn`
and activate the environment:
    `conda activate warn`
- The states which use Selenium to interact with the webpage require a browser driver for Chrome. Download the ChromeDriver version corresponding to your current Chrome version from [the Chromium website](https://chromedriver.chromium.org/downloads), extract the executable file from the zip file, and move the file to the `config` folder. The file must be renamed `chromedriver.exe` if it is not already named that.
- If you are running the code from a network drive and are working on a Windows computer, mount the drive (e.g., to 'D:') and navigate to that drive before running the code

## How to use
To run a particular state:
`python run_scrape.py -s [two-letter state abbreviation, e.g., AL]`

To run all states:
`python run_scrape.py`
Note that this will take a LONG time!

To test new parsing parameters/functions on a file saved locally in `tmp/test`, update `test_file_parsing` as needed and run
`python test_file_parsing.py` 

NOTE: all the test scripts are obsolete, but they've been for reference.

## Main loose ends to be tied up
- Notes on the current status of each state are provided in `documentation/coverage.xlsx`. **7** states states still require some work to scrape some/all of the archive, and **22** states need a bit of intermediate processing (cleaning the DataFrame before exporting Items from it) to make sure that each item generated is valid.
- Note that the `warn_config.csv` file provides a default format to try to parse, but updating it will not necessarily update the parsing function used internally! This value is often overwritten when multiple files are present, and otherwise often include format-specific keyword parameters that will cause the function to break if the format is changed. This could probably use a bit of refactoring to make this clearer/less brittle.

## Suggestions for improvement/additional functionality
- **Exporting as `.csv` file**: since comma delimited files require the column names to be consistent but field names vary between states (and sometimes even within the states), normalizing the data in `.csv` format is actually a bit less straightforward than for the `.jsonl` format. Provided in the `items` and `pipelines` modules are an unfinished version of a new `UnpackedEntry` `Item` and corresponding `Pipeline` for unpacking and exporting just `fields` and `normalized_fields` dictionaries of each `Entry`.
- **Scraping from a directory**: currently, if you provide a fully specified path to a local file in place of a website URL, Scrapy can handle that as if it were a website. However, to provide a directory of, for example, yearly Excel file archives, you would need to do the following:
    - use `os.listdir` and screen by file extension to get a list of the relevant fully specified file paths
    - make each path a valid [URI](https://en.wikipedia.org/wiki/Uniform_Resource_Identifier) (apply `path_to_uri`) so that Scrapy knows how to find it
    - populate the state spider's `start_urls` list with this URI list
- **Updating existing archive with just newest data**: the difficulty here twofold:
    (1) trying to scrape only what is necessary from a state, and
    (2) providing a unique notice ID that is not dependent on when the entry was scraped, to allow for deduplication.
For some states, the entire archive is in a single file/table, and otherwise for a monthly/quarterly scrape the entire most recent year's archive might need to be scraped. A workaround would be to specify a start date in `WARNSpider` and only yield `Item`s dated after that date, though that could miss updates to older notices depending on how the state denotes them. For states that do not already provide a WARN ID or employer ID, a state-specific unique ID could be generated from the fields (e.g., `f'{company}-{date}-{city}'`) that would be constant across scrapes.

## Acknowledgements
Developed by Lucia Korpas, Coding it Forward Fellow at the Bureau of Labor Statistics.
