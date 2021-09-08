"""Define two base Spider classes containing methods common to most states.

Most state-specific spiders will inherit from WARNSpider, which by default
tries to parse the response to the provided URL as a DataFrame and exports
the resulting rows as Entry items.
"""

import os
import re
import pandas as pd
import logging
from typing import Iterable
import scrapy
from urllib.parse import urljoin, urlparse, urlunparse
from pathlib import Path
from datetime import datetime
from selenium import webdriver

import modules.datatodf as todf
from config import CWD, TMPDIR
from modules.items import Entry, get_normalized_fields
from modules.utils import get_text_of_matching_elements, is_valid_url, remove_reserved_chars, whitespace_to_singlespace


class WARNSpider(scrapy.Spider):
    """Base Spider from which all other custom WARN-scraping spiders inherit

    Store relevant parameters from StateConfig object and define common methods
    that are used in most state-specific spiders.

    By default, the parse() method tries to parse the response from start_urls 
    using the method corresponding to the class instance variable data_format
    with no additional keyword arguments.

    Additional methods provided:
    - parse_as_df:  optionally specify a parsing format (e.g., 'html') with any
                    additional keyword arguments or provide a custom parsing function
    - get_links:    find links in response which match search criteria
    - parse_links:  run get_links, then follow each link with the callback parse_as_df,
                    each with any keyword arguments needed
    - initialize_webdriver:     available for child classes when Selenium is necessary 
    - save_response_to_file:    called only within parse_as_df (TODO: move out?)
    """

    def __init__(self, state_config=None, *args, **kwargs):
        super(WARNSpider, self).__init__(*args, **kwargs)

        assert state_config, "Provide a configuration dictionary for the scrape."

        # Required by Scrapy
        self.start_urls = [state_config.URL]

        
        
       

        # State-specific configuration
        self.config_url = state_config.URL
        self.state_name = state_config.STATE_NAME
        self.state_abbrev = state_config.STATE_ABBREV
        self.data_format = state_config.FORMAT
        self.to_df = getattr(todf, state_config.FORMAT, None)
        self.fields_dict = state_config.FIELDS
        if self.to_df is None:
            logging.warning(f"invalid format {state_config.FORMAT} provided for datatodf method from state configuration; will not parse")

        # For troubleshooting
        self.page_count = 0

    def parse(self, response):
        """Parse the response as the default data_format.
        This method will often be overridden."""

        yield from self.parse_as_df(response)

    def parse_as_df(self, response, parse_as=None, save_to_file=True, next_page_text=None,
                    todf_kwargs={}, custom_todf=None):
        """Given a data format (e.g., 'pdf') and optional keyword arguments for the
        corresponding parsing function, obtain DataFrame of the data in the response
        and yield each row as an Entry item.

        Parameters:
            parse_as: string corresponding to the expected format of the response
            custom_todf: custom function to return a dataframe from this data
        """

        if not parse_as:
            data_format = self.data_format
            to_df = self.to_df
        else:
            data_format = parse_as
            to_df = getattr(todf, parse_as, None)
            if to_df is None:
                logging.warning(f"invalid format {parse_as} provided for datatodf method from state configuration; will not parse")
        if custom_todf is not None:
            to_df = custom_todf

        if to_df:
            logging.info(f"Parsing as DataFrame with format {data_format}: {response.url}")
            timestamp = datetime.now()

            csv_path = None
            if save_to_file:
                # Save response to file
                response_path = self.save_response_to_file(response,
                    annotation=f'_{timestamp.strftime("%Y%m%d%H%M%S")}_',
                    format=data_format)

                # Parse tabular data from the file
                df = to_df(response_path, **todf_kwargs)

                # For debugging: save intermediate file
                csv_path = os.path.splitext(response_path)[0] + "_parsed.csv"
                df.to_csv(csv_path)
            else:
                # Parse tabular data directly from url (will generate new request)
                df = to_df(response.url, **todf_kwargs)                

            # Replace newlines/tabs/etc with singlespace in column names
            df.columns = df.columns.to_series().apply(whitespace_to_singlespace)

            # Apply state-specific cleaning function, if defined in child class
            clean = getattr(self, 'clean_df', None)
            if callable(clean):
                df = clean(df)

                # For debugging:
                if save_to_file and csv_path:
                    clean_path = os.path.splitext(csv_path)[0] + "_cleaned.csv"
                    df.to_csv(clean_path)

            # Drop any remaining empty rows or columns
            df = todf.drop_empty_rows_cols(df)

            # Create Items and yield to pipelines
            for index, row in df.iterrows():
                fields = row.to_dict()
                norm_fields = get_normalized_fields(self.fields_dict, row).to_dict()
                item = Entry(state_name=self.state_name,
                             timestamp=timestamp,
                             url=response.url,
                             fields=fields,
                             normalized_fields=norm_fields)
                yield item

        # Parse multiple HTML pages, if expected
        if next_page_text and data_format == 'html':           
            next_link = response.xpath(f'//*[@href][text()[contains(., "{next_page_text}")]]/@href').get()
            
            if next_link is not None:
                logging.info("Parsing next page of results")
                self.page_count += 1
                yield response.follow(next_link, self.parse_as_df, cb_kwargs={'parse_as':parse_as, 'save_to_file':save_to_file, 'next_page_text':next_page_text, 'todf_kwargs':todf_kwargs})
            else:
                logging.info(f"Downloaded {self.page_count} pages of results for {self.state_name}")

    def save_response_to_file(self, response, format=None, annotation=''):
        """Save response to text file with appropriate extension.
        
        Although Scrapy will ignore SSL certificate verification errors,
        some of the libraries used to parse data from a URL will not. To avoid
        the errors, we can save the response to a file locally and then pass
        the file path to the parsing functions. This also keeps the parsing
        functions from having to generate new requests to the website and
        also helps in debugging.

        Parameters:
            format: ['html', 'pdf', 'excel']
            annotation: string to be appended in filename, if any

        Returns:
            file_path: fully specified path to temporary response file
        """
        
        if format is None:
            logging.error("Specify a format")
            return None

        # Build filename, choosing extension carefully
        url = response.url
        _name, _ext = os.path.splitext(url.split('/')[-1])
        name = remove_reserved_chars(_name)
        if format in ['html', 'pdf']:
            # HTML files might originally have no extension;
            # PDF files may have a non-PDF extension but PDFMiner requires them to have a .pdf extension
            ext = f'.{format}'
            if _ext != '':
                logging.warning(f"Overwriting file extension from url ({_ext}) with expected extension ({ext}) for {url}")
        else:
            if _ext == '':
                # Look up extension from dictionary. Note that Google Sheets are assumed to be exported as CSV files.
                ext = todf.get_ext(format)
                logging.warning("No extension in original url for {format} data: using expected extension {ext}")
            else:
                ext = _ext.split('?')[0] # Remove query portion of URL, if any     
        file_name = f"{self.state_abbrev}{annotation}{name}{ext}"

        # Save HTML and CSV as text, other formats as binary
        file_path = os.path.join(TMPDIR, file_name)
        if ext == '.html' or ext == '.csv':
            try:
                with open(file_path, 'w') as f:
                    f.write(response.text)
            except UnicodeEncodeError:
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(response.text)
            except AttributeError as e:
                logging.error(f"{e}. Check if the format of the content at this URL is html as expected; if not, update the code to specify the correct format (e.g., pdf).")
        else:
            with open(file_path, 'wb') as f:
                f.write(response.body)            

        return file_path

    def parse_links(self, response, link_kwargs={}, todf_kwargs={}, format=None, custom_todf=None, next_page_text=None):
        """Find links matching text and/or href and follow each, parsing responses as DataFrame with given format

        Basically just a wrapper function to combine get_links and parse_as_df.

        Parameters:
            link_kwargs: dictionary of search parameters to pass to get_links
            todf_kwargs: dictionary of parsing parameters to pass to todf through parse_as_df
            format: string corresponding to the expected format of the response
        """

        links = self.get_links(response, **link_kwargs)

        parse_kwargs = {'parse_as': format,
                        'next_page_text': next_page_text,
                        'custom_todf': custom_todf,
                        'todf_kwargs': todf_kwargs}

        for link in links:
            if format == 'google_sheets':
                link = todf.get_google_sheets_export_link(link)
                logging.info(f"updating link to {link}")
            yield response.follow(link, callback=self.parse_as_df, cb_kwargs=parse_kwargs)


    def get_links(self, response, find_in_text=None, find_in_href=None, exclude_from_href=None):
        """Find links matching text and/or href

        There's probably a neater way to do this using xpath match()...

        Parameters:
            find_in_text:   string to search for within text of link
            find_in_href:   string to search for within href of link
            exclude_from_href:  string used to discard irrelevant links

        Returns:
            hrefs:  list of hrefs matching search terms. Note that these may be
                    relative paths (not necessarily complete urls)
        """        
        
        if not (find_in_text or find_in_href):
            logging.error(f"Supply one or both of find_in_text and find_in_href to search for links in {response.url}")
            return None

        hrefs = []
        if find_in_text and find_in_href:
            hrefs = response.xpath(f'//a[contains(text(), "{find_in_text}")][contains(@href, "{find_in_href}")]/@href').getall()
        elif find_in_text:
            hrefs = response.xpath(f'//a[contains(text(), "{find_in_text}")]/@href').getall()
        elif find_in_href:
            hrefs = response.xpath(f'//a[contains(@href, "{find_in_href}")]/@href').getall()
        
        if len(hrefs) == 0:
            logging.error(f"No links found with text containing {find_in_text} and/or href containing {find_in_href} in {response.url}. Please inspect the HTML element(s) you are hoping to find to check if find_in_href and find_in_text need to be updated.")
            return hrefs

        # Remove duplicates, exclude undesired links, and sort
        hrefs = list(set(hrefs))
        if exclude_from_href:
            hrefs = [x for x in hrefs if exclude_from_href not in x]
        hrefs.sort()

        logging.info(f"Found links: {hrefs}")

        return hrefs

    def initialize_webdriver(isHeadless=False):
        """Initialize a Selenium webdriver to use within parse()

        To be used when accessing the archive data requires interacting with dynamic web content.
        Don't forget driver.close() when you are done using it!

        Parameters:
        - isHeadless: boolean for whether the browser runs in the background or as a visible window

        Returns:
        - driver: Selenium webdriver with downloads redirected to the project's temporary directory
        """
        
        chromeOptions = webdriver.ChromeOptions()
        chromeOptions.headless = isHeadless # TODO: figure out why headless=True doesn't actually do anything
        prefs = {'download': {'prompt_for_download': False,
                              'directory_upgrade': True,
                              'default_directory': TMPDIR}
                }
        chromeOptions.add_experimental_option("prefs", prefs)
        webdriverPath = os.path.join(CWD, 'config/chromedriver.exe')

        logging.info('Initializing Selenium webdriver for Chrome')
        driver = webdriver.Chrome(executable_path=webdriverPath, options=chromeOptions)
        driver.maximize_window()
        driver.implicitly_wait(5)

        return driver


class JobLinkSpider(WARNSpider):
    """Child class for JobLink-based WARN archive databases
    
    State-specific classes can inherit from this instead of WARNSpider,
    with no further modification except class name.

    TODO: update this now that I know how to use Scrapy better
    """

    def __init__(self, custom_search={}, *args, **kwargs):
        super(JobLinkSpider, self).__init__(*args, **kwargs)

        # Construct new start URL from query string and original URL
        search = {'notice_eq': 'true', # WARN notices only
                  'notice_on_gteq': '1988-08-04', # Start date of search
                  's': 'notice_on+asc' # Sort by notice date, ascending
                  }
        search.update(custom_search)
        query_string = ('/search/warn_lookups?commit=Search&utf8=✓&' + 
                        '&'.join([f"q[{key}]={value}" for key, value in search.items()]))
        self.base_url = re.sub('/search/warn_lookups.*', '', self.config_url) # Need this for link following
        self.start_urls = [urljoin(self.base_url, query_string)]

        # For logging
        self.page_count = 1

    def parse(self, response):
        """Scrape all search result pages, follow links to individual WARN
        notice pages, and add those to the Entry item before yielding. 
        """
        
        # Get table of data
        table_class = "sortable responsive default"
        table = response.xpath(f'//*[@class="{table_class}"]')
        
        if table:
            # Get column headers, stripping ascending/descending markers
            columns = get_text_of_matching_elements(table, './/th', re_str='[▲|▼]')

            rows = table.xpath('.//tbody/tr')
            for row in rows:
                # Get value for each column
                td = get_text_of_matching_elements(row, './/td')
                fields = dict(zip(columns, td))
                
                # Create item with data dictionary
                item = Entry(state_name=self.state_name,
                             timestamp=datetime.now(),
                             fields=fields)
                
                # Follow link on each entry to get more detailed information,
                # updating the original item before yielding it to the pipelines
                entry_href = row.xpath('td[1]/*/@href').get()
                if entry_href:
                    entry_link = urljoin(self.base_url, entry_href)
                    yield scrapy.Request(entry_link, callback=self.parse_details, cb_kwargs={"item": item})
                else:
                    logging.warning(f"Expecting a link table entry to contain a link; {self.state_name} may need to update xpath selector. Yielding partial Entry")
                    yield item
        else:
            logging.error(f"No table found for {self.state_name}; may need to updated xpath selector")
            yield

        # Do the same for the next page, if any
        next_link = response.xpath("//a[contains(@class, 'next_page')]/@href").get()

        if next_link is not None:
            self.page_count += 1
            yield response.follow(next_link, self.parse)
        else:
            logging.info(f"Downloaded {self.page_count} pages of results for {self.state_name}")

    def parse_details(self, response, item=None):
        """Scrape fields from individual WARN entry page and add to item.
        
        This is run for each row Entry as the search results table is being scraped.
        
        Parameters:
            response: HTML response from URL to individual WARN entry
            item: Scrapy Item; may pass existing Entry item from main scrape
        """
        
        assert item is not None, "Provide an item"
        
        if response:
            # Use individual WARN notice url
            item['url'] = response.url

            fields = item['fields']
            
            dt = get_text_of_matching_elements(response, '//dt')
            dd = get_text_of_matching_elements(response, '//dd')

            data = dict(zip(dt, dd))
            
            # Update fields with additional data
            fields.update(data)
            item['fields'] = fields

            # Generate normalized fields
            norm_fields = get_normalized_fields(self.fields_dict, pd.Series(fields)).to_dict()
            item['normalized_fields'] = norm_fields     

        yield item

