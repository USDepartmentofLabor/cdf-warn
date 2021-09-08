"""Define a Spider class for each state which inherits from either the WARNSpider
or JobLinkSpider class.

The naming convention for the spiders is {state_abbrev}WARNSpider, regardless of
which of the two base classes it inherits from.

By default, the spider will use the parse() method defined in the parent spider.
In many cases, the parse() method is be overridden to find and follow links,
supply a custom parsing function, etc.
"""

import os
import re
import logging
import scrapy
import time
import pandas as pd
from urllib.parse import urljoin, urlparse, urlunparse
from pathlib import Path
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, TimeoutException

import modules.datatodf as todf
import modules.customdatatodf as ctodf
from config import CWD, TMPDIR
from modules.spiders.base import WARNSpider, JobLinkSpider
from modules.items import Entry, get_normalized_fields
from modules.utils import get_base_url, is_valid_url


class ALWARNSpider(WARNSpider):
    """Alabama"""
    name = 'ALWARN'

    def clean_df(self, df):
        """Screen out spurious rows at bottom of page"""
        df = todf.drop_empty_rows_cols(df)
        df = df[df['Closing or Layoff'].apply(lambda x: len(str(x))) > 1]
        return df


class AKWARNSpider(WARNSpider):
    """Alaska"""
    name = 'AKWARN'

    def parse(self, response):
        todf_kwargs = {'use_pandas': False,
                       'link_col': 0,
                       'base_url': get_base_url(response.url)}
        yield from self.parse_as_df(response, todf_kwargs=todf_kwargs)


class AZWARNSpider(JobLinkSpider):
    """Arizona"""
    name = 'AZWARN'


class ARWARNSpider(JobLinkSpider):
    """Arkansas"""
    name = 'ARWARN'


class CAWARNSpider(WARNSpider):
    """California"""
    name = 'CAWARN'

    def parse(self, response):
        # Find and parse current warn report
        link_kwargs = {'find_in_text': 'WARN Report',
                       'find_in_href': '.xlsx'}
        todf_kwargs = {'skip_header': 3,
                       'drop_footer': 4}
        yield from self.parse_links(response, format='excel', link_kwargs=link_kwargs, todf_kwargs=todf_kwargs)

        # Get links to archived WARN Reports
        link_kwargs = {'find_in_text': 'WARN Report',
                       'find_in_href': '.pdf'}
        archive_links = self.get_links(response, **link_kwargs)

        # Parse archived WARN reports       
        todf_kwargs = {'header_on_all_pages': False}
        for link in links: 
            if '7-1-2020' in link:
                # One of the pdfs has a low-contrast header row that the parser cannot read:
                # provide column_names argument just for this one. (Not all pdfs have the same columns)
                additional_kwargs = {'column_names': ["Notice Date", "Effective Date", "Recieved Date", "Company",
                                                      "City", "County", "No. Of Employees", "Layoff/Closure Type"]}
                yield response.follow(link, callback=self.parse_as_df,
                                      cb_kwargs={'parse_as': 'pdf',
                                                 'todf_kwargs': {**todf_kwargs, **additional_kwargs}})
            else:
                yield response.follow(link, callback=self.parse_as_df,
                                      cb_kwargs={'parse_as': 'pdf', 
                                                 'todf_kwargs': todf_kwargs})

    def clean_df(self, df):
        """Normalize column names within the state"""

        df = todf.drop_empty_rows_cols(df)
        column_dict = {'Employees': 'No. Of Employees',
                       'Layoff/Closure Type': 'Layoff/Closure'}
        df.rename(columns=column_dict, errors='ignore', inplace=True)

        return df


class COWARNSpider(WARNSpider):
    """Colorado"""
    name = 'COWARN'

    custom_settings = {'DOWNLOAD_DELAY': 30.0}

    def parse(self, response):
        link_kwargs = {'find_in_href': 'docs.google.com/spreadsheets'}
        yield from self.parse_links(response, link_kwargs=link_kwargs, format='google_sheets') # Need to explicitly provide format, whoops


class CTWARNSpider(WARNSpider):
    """Connecticut"""
    name = 'CTWARN'

    def parse(self, response):
        # Get links to HTML archive for each year       
        links = self.get_links(response, find_in_href='warn')
        for link in links:
            # Table formatting is different for 2016; couln't find a common unique locator across all years
            if '2016' in link:
                todf_kwargs = {'table_class': 'style15'}
            else:
                todf_kwargs = {'table_class': 'MsoNormalTable'}
            yield response.follow(link, callback=self.parse_as_df, cb_kwargs={'todf_kwargs': todf_kwargs})

    def clean_df(self, df):
        """Split notice date and received date into two columns"""

        df = todf.drop_empty_rows_cols(df)
        df['WARN Dates'] = df['WARN Date'].apply(self.split_dates)
        df['Notice Date'] = df['WARN Dates'].apply(lambda x: x[0] if isinstance(x, list) else None)
        df['Received Date'] = df['WARN Dates'].apply(lambda x: x[1] if isinstance(x, list) else None)
        df.drop(columns=['WARN Dates'], inplace=True)

        return df

    def split_dates(self, warn_date):
        """Try to split 'WARN Date' column in two"""
        
        dates = warn_date.split('Rec\'d')
        print(dates)
        if len(dates) == 2:
            return [d.strip() for d in dates]
        else:
            return warn_date


class DEWARNSpider(JobLinkSpider):
    """Delaware"""
    name = 'DEWARN'


class DCWARNSpider(WARNSpider):
    """District of Columbia"""
    name = 'DCWARN'

    def parse(self, response):
        # Get current data from landing page
        yield from self.parse_as_df(response)

        # Get links to archived data
        # At this time the 2014 archive is not properly linked: add it here
        link_kwargs = {'find_in_text': 'Industry Closings and Layoffs'}
        #links = self.get_links(response, **link_kwargs)
        links = []
        if not any('2014' in link for link in links):
            links.append('/page/industry-closings-and-layoffs-warn-notifications-closure%202014')

        # Parse data from linked pages
        for link in links:
            yield response.follow(link, callback=self.parse_as_df)

    def clean_df(self, df_raw):
        """Apply additional cleaning rules
        
        This method, when defined, is run as part of parse_as_df by default.
        """

        df = df_raw.copy(deep=True)
        df = todf.drop_empty_rows_cols(df)

        # Drop spurious extra column generated by extra tables on page
        df.drop(columns=0, errors='ignore', inplace=True)
        
        # The column names are slightly different across years: map to same value throughout years
        # NOTE: The values in the key:value pairs should also match the values in the config file!
        column_names = {'CodeType': 'Code Type',
                        'Number toEmployees Affected': 'Number of Employees Affected',
                        'Number to Employees Affected': 'Number of Employees Affected'}

        df.rename(columns=column_names, errors='ignore', inplace=True)

        # Apply "Code Type" conversion based on legend, keeping original column also
        code_type = {'1': 'Layoff',
                     '2': 'Permanent Closure'}
        df['Code Type (original)'] = df['Code Type']
        df['Code Type'] = df['Code Type'].apply(lambda x: code_type.get(str(x), x))

        return df


class FLWARNSpider(WARNSpider):
    """Florida"""
    name = 'FLWARN'

    def parse(self, response):
        # Find and follow PDF links
        """
        link_kwargs = {'find_in_text': 'WARN Notices',
                       'find_in_href': 'PDF'}
        todf_kwargs = {'pdf_kwargs': {'line_scale': 30},
                       'header_on_all_pages': False}
        yield from self.parse_links(response, format='pdf', link_kwargs=link_kwargs, todf_kwargs=todf_kwargs)
        """
        # Find and follow HTML links, following multiple pages of results
        link_kwargs = {'find_in_text': 'WARN Notices',
                       'find_in_href': 'WarnList',
                       'exclude_from_href': 'PDF'}
        yield from self.parse_links(response, format='html', link_kwargs=link_kwargs, 
                                    next_page_text='>')


class GAWARNSpider(WARNSpider):
    """Georgia"""
    name = 'GAWARN'

    def __init__(self, *args, **kwargs):
        super(GAWARNSpider, self).__init__(*args, **kwargs)
        self.start_date = '08/04/1988' 
        self.end_date = None

    def parse(self, response):
        """Run Selenium to iterate through years of data
        
        Instead of parsing the data from the HTML pages, clicks
        the "Generate Excel" button and parses that.
        """

        driver = self.initialize_webdriver()
        driver.get(response.url)
        
        # Get list of years available
        yearSelector = Select(driver.find_element_by_name('year'))
        options = yearSelector.options
        optionsText = [opt.text for opt in options]
        optionsText.remove('Select Year')

        files = []
        for opt in optionsText:
            # Select "Statewide" data for the current year option
            geoSelector = Select(driver.find_element_by_name('geoArea'))
            geoSelector.select_by_visible_text('Statewide')
            yearSelector = Select(driver.find_element_by_name('year'))
            yearSelector.select_by_visible_text(opt)

            # Execute search
            driver.find_element_by_xpath('//button[@value="search"]').click()
            try:
                # Check if there is a table of results; if not, exception is raised
                table = driver.find_element_by_id('emplrList')

                # Click Excel download link
                driver.find_element_by_xpath('//button[@value="generateExcel"]').click()
                try:
                    # Wait for expected filename to appear in directory; 
                    # if it doesn't appear within 10s, exception is raised
                    downloadFilename = f'WarnIdList_Statewide_{opt}.xlsx'
                    downloadFilepath = os.path.join(TMPDIR, downloadFilename)
                    wait = WebDriverWait(driver, 10)
                    wait.until(file_exists(downloadFilepath)) 

                    # Rename the file according to convention in WARNSpider save_response_to_file
                    timestamp = datetime.now()
                    saveFilename = f'{self.state_abbrev}_{timestamp.strftime("%Y%m%d%H%M%S")}_{downloadFilename}'
                    saveFilepath = os.path.join(TMPDIR, saveFilename)
                    os.rename(downloadFilepath, saveFilepath)
                    files.append(saveFilepath)

                except TimeoutException:
                    logging.error(f"Expected file to be downloaded from {response.url} with name {downloadFilename}. Check if this filename matches the file produced when manually downloading the file.")

            except NoSuchElementException:
                # No table element exists if there are no search results present
                pass
            driver.find_element_by_link_text('Return to search page').click()

        driver.close()

        # Parse DataFrames for each downloaded file and export Items to pipeline
        # TODO: un-hardcode-this by splitting the method in WARNSpider into components and using some of those here
        todf_kwargs = {'skip_header': 5}
        for f in files:
            timestamp = f.split('_')[1]
            df = todf.excel(f, **todf_kwargs)
            df.to_csv(os.path.splitext(f)[0] + "_parsed.csv")    

            for index, row in df.iterrows():
                fields = row.to_dict()
                norm_fields = get_normalized_fields(self.fields_dict, row).to_dict()
                item = Entry(state_name=self.state_name,
                             timestamp=timestamp,
                             url=response.url,
                             fields=fields,
                             normalized_fields=norm_fields)
                yield item


class HIWARNSpider(WARNSpider):
    """Hawaii"""
    name = 'HIWARN'

    def parse(self, response):
        """Use state-specific data-to-DataFrame defined in customtodf module"""
        yield from self.parse_as_df(response, custom_todf=ctodf.html_HI)


class IDWARNSpider(WARNSpider):
    """Idaho"""
    name = 'IDWARN'


class ILWARNSpider(WARNSpider):
    """Illinois"""
    name = 'ILWARN'

    def __init__(self, *args, **kwargs):
        super(ILWARNSpider, self).__init__(*args, **kwargs)
        self.start_date = '08/04/1988' 

    def parse(self, response):
        """Run Selenium to get all results since start_date
        
        """
        driver = self.initialize_webdriver()
        driver.get(response.url)

        # Navigate within iframe
        frame = driver.find_element_by_xpath('//iframe[@title="Page Viewer"]')
        driver.switch_to.frame(frame)

        #Input date(s) and wait for results to load
        selector = Select(driver.find_element_by_tag_name('select'))
        selector.select_by_visible_text('Custom')
        page_range_locator = (By.XPATH, '//div[@class="pagination-range"]/span')
        page_range_text = driver.find_element_by_xpath(page_range_locator[1]).text
        datepickers = driver.find_elements_by_xpath('//input[@ngbdatepicker=""]')
        datepickers[0].send_keys(self.start_date, Keys.ENTER)
        wait = WebDriverWait(driver, 30)
        wait.until(text_has_changed(page_range_locator, page_range_text))

        # Download results once loaded
        downloadButton = driver.find_element_by_xpath('//*[text()[contains(.,"Download")]]')
        downloadButton.click()

        saveFilepath = None
        try:
            # Find downloaded file
            r = re.compile('IebsLayoffs-Public.*.xlsx')
            wait = WebDriverWait(driver, 120) 
            # 07/27/21: Currently this is raising a TimeoutException, and no file has been downloaded.
            # Probably need to add back in the results-loading wait before clicking download, and then
            # maybe further increasing this wait...
            downloadFilename = wait.until(matching_file_in_directory(r, directory=TMPDIR))
            downloadFilepath = os.path.join(TMPDIR, downloadFilename)

            # Rename the file
            timestamp = datetime.now()
            saveFilename = f'{self.state_abbrev}_{timestamp.strftime("%Y%m%d%H%M%S")}_{downloadFilename}'
            saveFilepath = os.path.join(TMPDIR, saveFilename)
            os.rename(downloadFilepath, saveFilepath)
        except IndexError:
            logging.warning(f"Expected file to be downloaded from {response.url}")

        driver.close()

        # Parse DataFrames for each downloaded file and export Items to pipeline
        # TODO: un-hardcode-this by splitting the method in WARNSpider into components and using some of those here
        if saveFilepath:
            sheet_names = ['Layoffs', 'Scheduled Layoffs']
            df_original = todf.excel(saveFilepath, sheets_to_use=[sheet_names[0]])
            df_updates = todf.excel(saveFilepath, sheets_to_use=[sheet_names[1]])
            df_original.to_csv(os.path.splitext(saveFilepath)[0] + "_layoffs_parsed.csv")
            df_updates.to_csv(os.path.splitext(saveFilepath)[0] + "_scheduled_parsed.csv")

            # TODO: merge original and updates?? Ask Gio
            for i, df in enumerate([df_original, df_updates]):
                for index, row in df.iterrows():
                    fields = row.to_dict()
                    fields['Excel Sheet Name': sheet_names[i]]
                    norm_fields = get_normalized_fields(self.fields_dict, row).to_dict()
                    item = Entry(state_name=self.state_name,
                                timestamp=timestamp,
                                url=response.url,
                                fields=fields,
                                normalized_fields=norm_fields)
                    yield item


class INWARNSpider(WARNSpider):
    """Indiana"""
    name = 'INWARN'

    def parse(self, response):
        # Parse landing page
        todf_kwargs = {'use_pandas': False,
                       'link_col':1}
        yield from self.parse_as_df(response, parse_as='html', todf_kwargs=todf_kwargs)
        
        # Follow links to and parse archive for each year (same format as landing page)
        link_kwargs = {'find_in_href': 'archived-warn-notices'}
        yield from self.parse_links(response, format='html', link_kwargs=link_kwargs, todf_kwargs=todf_kwargs)    

    def clean_df(self, df):
        """Apply legend to notice type"""

        notice_type = {'W': 'WARN Notice',
                       'CL': 'Closure',
                       'LO': 'Layoff',
                       'TR': 'Transfer',
                       'RH': 'Reduction in Hours',
                       'Cond.': 'Conditional'}
        df['Notice Type (original)'] = df['Notice Type']
        df['Notice Type'] = df['Notice Type'].apply(lambda x: notice_type.get(str(x), x))
        
        return df


class IAWARNSpider(WARNSpider):
    """Iowa"""
    name = 'IAWARN'

    def parse(self, response):
        link_kwargs = {'find_in_href': '.xlsx'}
        yield from self.parse_links(response, link_kwargs=link_kwargs)    


class KSWARNSpider(JobLinkSpider):
    """Kansas"""
    name = 'KSWARN'


class KYWARNSpider(WARNSpider):
    """Kentucky"""
    name = 'KYWARN'

    def parse(self, response):
        link_kwargs = {'find_in_href': '.xlsx'}
        yield from self.parse_links(response, link_kwargs=link_kwargs) 


class LAWARNSpider(WARNSpider):
    """Louisiana"""
    name = 'LAWARN'

    def parse(self, response):
        link_kwargs = {'find_in_text': 'WARN Notices',
                       'find_in_href': '.pdf'}
        yield from self.parse_links(response,  link_kwargs=link_kwargs)  


class MEWARNSpider(JobLinkSpider):
    """Maine"""
    name = 'MEWARN'


class MDWARNSpider(WARNSpider):
    """Maryland"""
    name = 'MDWARN'

    def parse(self, response):
        link_kwargs = {'find_in_href': 'warn',
                       'exclude_from_href': 'dashboard'}
        todf_kwargs = {'use_pandas': False}
        yield from self.parse_links(response, link_kwargs=link_kwargs, todf_kwargs=todf_kwargs)  

    def clean_df(self, df):
        """Use legends provided on state website to convert values"""

        # Apply "Type Code" conversion based on legend, keeping original column also
        type_code = {'1': 'PLANT CLOSURE',
                     '2': 'MASS LAYOFF'}
        df['Type Code (original)'] = df['Type Code']
        df['Type Code'] = df['Type Code'].apply(lambda x: type_code.get(str(x), x))

        # Apply "Local Area Code" conversion based on legend, keeping original column also
        local_area_code = {'1': 'A.A. CO.',
                           '2': 'BALTO. CO.',
                           '3': 'BALTO. CITY',
                           '4': 'FREDERICK',
                           '5': 'LOWER SHORE',
                           '6': 'MID-MD.',
                           '7': 'MONTGOMERY',
                           '8': 'PRINCE GEORGE\'S',
                           '9': 'SOUTHERN MARYLAND',
                           '10': 'SUSQUEHANNA',
                           '11': 'UPPER SHORE',
                           '12': 'WESTERN MARYLAND',
                           '13': 'STATEWIDE'}
        df['WIA Code (original)'] = df['WIA Code']
        df['WIA Code'] = df['WIA Code'].apply(lambda x: local_area_code.get(str(x), x))

        return df


class MAWARNSpider(WARNSpider):
    """Massachusetts"""
    name = 'MAWARN'


class MIWARNSpider(WARNSpider):
    """Michigan"""
    name = 'MIWARN'

    def parse(self, response):
        # Parse landing page (HTML)
        yield from self.parse_as_df(response, parse_as='html')
        
        # Parse archived pages
        archive_link = self.get_links(response, find_in_text='WARN Archive page')[0]
        yield response.follow(archive_link, callback=self.parse_archive)

    def parse_archive(self, response):
        # Parse landing page of archive (HTML)
        yield from self.parse_as_df(response, parse_as='html')

        # Get PDF links for archived years and set parsing parameters by years
        links = self.get_links(response, find_in_href='pdf', exclude_from_href='Statewide')
        todf_kwargs = {'pdf_kwargs': {'flavor': 'stream'},
                       'header_on_all_pages': False}
        params_by_year = {0: ([2008, 2013],
                              {}),
                          10: ([2001],
                              {'pdf_kwargs': {'flavor': 'stream', 'columns': ['101,234,313,367,413']}}),
                          1: ([2004, 2011, 2012, 2014],
                              {'first_page_header': 1}),
                          2: ([2000, 2002, 2003],
                              {'first_page_header': 2}),
                          3: ([2005, 2006, 2007, 2009, 2010, 2015],
                              {'first_page_header': 3})}
        
        # Apply appropriate parsing parameters to each year
        for link in links:
            for key in params_by_year.keys():
                years, kwargs = params_by_year[key]
                if any([f'warn{year}' in link for year in years]):
                    cb_kwargs = {'parse_as': 'pdf',
                                 'todf_kwargs': {**todf_kwargs, **kwargs}}
                    yield response.follow(link, callback=self.parse_as_df, cb_kwargs=cb_kwargs)

    def clean_df(self, df):
        # Apply "Incident Type" conversion based on legend, keeping original column also
        incident_type = {'1': 'Facility Closure',
                         '2': 'Layoff Event'}
        if 'Incident Type*' in df:
            df['Incident Type'] = df['Incident Type'].apply(lambda x: incident_type.get(str(x), x))

        return df


class MNWARNSpider(WARNSpider):
    """Minnesota"""
    name = 'MNWARN'

    custom_settings = {'DOWNLOAD_DELAY': 30.0}
    PAUSE_AFTER = 5 # Requests
    PAUSE_TIME = 5*60 # seconds

    def parse(self, response):
        """This currently runs fine for about ~8 requests and then always runs into
        CAPTCHA codes.
        
        Tried to use just default Scrapy request, requests.get, the PAUSE parameters
        and updated DOWNLOAD_DELAY listed above, and finally Selenium instead, but 
        none solved the problem..."""
        
        driver = self.initialize_webdriver()
        driver.get(response.url)
        
        # Get links to only WARN notice archive PDF files 
        links = self.get_links(response, find_in_href='.pdf')
        terms = ['mass-layoff', 'plant-closing', 'dislocated-worker', 'warn']
        links = [link for link in links if any((term in link.lower()) for term in terms)]

        # Iterate through links
        base_url = get_base_url(response.url)
        for link in links:
            if not is_valid_url(link):
                link = urljoin(base_url, link)

            # Get PDF and save to file
            driver.get(link)

            # Save contents to file
            timestamp = datetime.now()
            name = link.split('/')[-1].splitext()[0]
            saveFilename = f'{self.state_abbrev}_{timestamp.strftime("%Y%m%d%H%M%S")}_{name}.pdf'
            saveFilepath = os.path.join(TMPDIR, saveFilename)
            with open(os.path.join(TMPDIR, saveFilepath), "wb") as f:
                f.write(driver.page_source)

            # Parse request
            df = todf.html(saveFilepath, **todf_kwargs)
            df.to_csv(os.path.splitext(saveFilepath)[0] + "_parsed.csv")    

            # Yield item
            for index, row in df.iterrows():
                fields = row.to_dict()
                norm_fields = get_normalized_fields(self.fields_dict, row).to_dict()
                item = Entry(state_name=self.state_name,
                             timestamp=timestamp,
                             url=response.url,
                             fields=fields,
                             normalized_fields=norm_fields)
                yield item

        driver.close()

        """
        # Attempt to implement pause between every N requests
        links = self.get_links(response, find_in_href='.pdf')

        todf_kwargs = {'pdf_kwargs': {'flavor': 'stream',
                                      'row_tol': 13},
                       'header_row': 1}
        cb_kwargs = {'todf_kwargs': todf_kwargs}
        for i, link in enumerate(links):
            if i % self.PAUSE_AFTER == 0 and i != 0:
                logging.info(f"Sleeping for {self.PAUSE_TIME/60} minutes after {i} requests")
                time.sleep(self.PAUSE_TIME)
            yield response.follow(link, callback=self.parse_as_df, cb_kwargs=cb_kwargs)
        """


class MSWARNSpider(WARNSpider):
    """Mississippi"""
    name = 'MSWARN'

    def parse(self, response):
        link_kwargs = {'find_in_href': '.pdf',
                       'exclude_from_href': 'map'}
        yield from self.parse_links(response, link_kwargs=link_kwargs)

    def clean_df(self, df):
        """Number of employees gets read as a separate row from the rest of the notice: fix this"""
        
        # Rename original column and create new column to populate
        df.rename(columns={'Type of Action # Affected': 'Type of Action'}, inplace=True)
        df['# Affected'] = ''

        for index, row in df.iterrows():
            if index > 0 and (pd.isnull(row['Company Name']) or row['Company Name'] == ''):
                n_employees = row['Type of Action']
                df.loc[index-1, ['# Affected']] = n_employees
                df.loc[index, ['Type of Action']] = '' # This row will now be empty and deleted by default cleaning
        
        return df


class MOWARNSpider(WARNSpider):
    """Missouri"""
    name = 'MOWARN'

    def __init__(self, *args, **kwargs):
        super(MOWARNSpider, self).__init__(*args, **kwargs)
        """MO has no index page for their archive, so this attempts to scrape
        all pages by generating the expected URLs of each year's archive from
        the given URL. Some of these get redirect to other URLs. 
        
        TODO: also get 2020, which has different url: https://jobs.mo.gov/content/2020-missouri-warn-notices
        """
        
        base_url = re.sub('20\d\d', '', self.config_url)
        start_year = 2015 # Earliest data available online
        current_year = datetime.today().year
        years = list(range(start_year, current_year + 1))
        self.start_urls = [f'{base_url}{y}' for y in years]
        print(self.start_urls)


class MTWARNSpider(WARNSpider):
    """Montana"""
    name = 'MTWARN'

    def parse(self, response):
        link_kwargs = {'find_in_href': 'warn.xlsx'}
        yield from self.parse_links(response, link_kwargs=link_kwargs)


class NEWARNSpider(WARNSpider):
    """Nebraska"""
    name = 'NEWARN'

    def parse(self, response):
        todf_kwargs = {'use_pandas': False,
                       'link_col': 2}
        yield from self.parse_as_df(response, todf_kwargs=todf_kwargs)


class NVWARNSpider(WARNSpider):
    """Nevada"""
    name = 'NVWARN'

    def parse(self, response):
        link_kwargs = {'find_in_href': '.pdf',
                       'find_in_text': 'WARN'}
        todf_kwargs = {'pdf_kwargs': {'flavor': 'stream'}, 
                       'header_on_all_pages': False}
        yield from self.parse_links(response, link_kwargs=link_kwargs, todf_kwargs=todf_kwargs)


class NHWARNSpider(WARNSpider):
    """New Hampshire"""
    name = 'NHWARN'


class NJWARNSpider(WARNSpider):
    """New Jersey"""
    name = 'NJWARN'

    def parse(self, response):
        """Use state-specific data-to-DataFrame defined in customtodf module"""
        link_kwargs = {'find_in_text': 'Warn Notices'}
        yield from self.parse_links(response, link_kwargs=link_kwargs,
                                    custom_todf=ctodf.html_NJ)


class NMWARNSpider(WARNSpider):
    """New Mexico"""
    name = 'NMWARN'

    def parse(self, response):
        """Note that currently New Mexico's robots.txt does not allow scraping WARN notices.
        This code can scrape the website, but in the future, should probably switch to
        scraping from a directory of Excel sheets, which is what has been provided
        """
        
        link_kwargs = {'find_in_href': '.pdf',
                       'exclude_from_href': 'Handbook'}
        todf_kwargs = {'pdf_kwargs': {'iterations': 2},
                       'header_row': 1,
                       'col_delimiter': '\n'}
        yield from self.parse_links(response, link_kwargs=link_kwargs, todf_kwargs=todf_kwargs)


class NYWARNSpider(WARNSpider):
    """New York"""
    name = 'NYWARN'


class NCWARNSpider(WARNSpider):
    """North Carolina"""
    name = 'NCWARN'

    def parse(self, response):
        # Get archive page links from landing page
        links = self.get_links(response, find_in_href='warn-report-20')
        links.extend(self.get_links(response, find_in_href='report-archives'))
        for link in links:
            yield response.follow(link, callback=self.parse_archive)

    def parse_archive(self, response):
        # Follow the PDF link(s) on each archive page
        links = self.get_links(response, find_in_href='.pdf')
        pdf_kwargs = {'flavor': 'stream',
                      'columns': ['35,94,163,335,400,473']}
        for link in links:
            if any([(year in link) for year in ['2015', '2016', '2017']]):
                todf_kwargs = {'pdf_kwargs': pdf_kwargs,
                               'first_page_header': 1}
            elif '2014' in link:
                todf_kwargs = {'pdf_kwargs': pdf_kwargs,
                               'first_page_header': 2}
            else:
                todf_kwargs = {}
            yield response.follow(link, callback=self.parse_as_df, cb_kwargs={'todf_kwargs': todf_kwargs})


class NDWARNSpider(WARNSpider):
    """North Dakota"""
    name = 'NDWARN'   

    def parse(self, response):
        todf_kwargs = {'pdf_kwargs': {'flavor': 'stream'},
                       'header_on_all_pages': False,
                       'header_row': 1}
        yield from self.parse_as_df(response, todf_kwargs=todf_kwargs)


class OHWARNSpider(WARNSpider):
    """Ohio"""
    name = 'OHWARN'

    def parse(self, response):
        """Get links to all archive pages, then parse as PDF or HTML depending on content type"""
        links_stm = self.get_links(response, find_in_href='current.stm')
        links_stm.extend(self.get_links(response, find_in_href='archive.stm'))
        links_stm.extend(self.get_links(response, find_in_href='WARN20'))
        for link in links_stm:
            # Can't tell from hrefs whether the response is HTML or PDF, check before passing to parse_as_df
            yield response.follow(link, callback=self.parse_archive)

    def parse_archive(self, response):
        """Check whether the response received is a PDF or not"""
        content_type = response.headers.get("content-type", "").lower() # Binary string
        if content_type.startswith(b'application/pdf'):
            todf_kwargs = {'header_row':1}
            yield from self.parse_as_df(response, parse_as='pdf', todf_kwargs=todf_kwargs)
        else:
            yield from self.parse_as_df(response, parse_as='html')


class OKWARNSpider(JobLinkSpider):
    """Oklahoma"""
    name = 'OKWARN'


class ORWARNSpider(WARNSpider):
    """Oregon"""
    name = 'ORWARN'

    def parse(self, response):
        """Run Selenium to generate and download Excel file

        Note that to capture notice PDFs and notice date, would need to also scrape HTML version!
        """

        driver = self.initialize_webdriver()
        driver.get(response.url)
        
        # Generate .xlsx file
        formatSelector = Select(driver.find_element_by_id('WARNFormat'))
        formatSelector.select_by_value('xlsx')
        driver.find_element_by_xpath('//input[@value="Create WARN List"]').click()

        try:
             # Download .xlsx link once link appears to do so
            wait = WebDriverWait(driver, 10)
            downloadLink = wait.until(EC.presence_of_element_located((By.XPATH, '//a[contains(@href, ".xlsx")]')))
            downloadLink.click()
        except TimeoutException:
            logging.error(f'Timed out waiting for link to download Excel file from {response.url}. Try again maybe?')
        
        saveFilepath = None
        try:
            # Wait for downloaded file to appear
            r = re.compile('WARNList_\d{6}.xlsx')    
            wait = WebDriverWait(driver, 15)
            downloadFilename = wait.until(matching_file_in_directory(r, directory=TMPDIR))
            downloadFilepath = os.path.join(TMPDIR, downloadFilename)

            # Rename the file
            timestamp = datetime.now()
            saveFilename = f'{self.state_abbrev}_{timestamp.strftime("%Y%m%d%H%M%S")}_{downloadFilename}'
            saveFilepath = os.path.join(TMPDIR, saveFilename)
            os.rename(downloadFilepath, saveFilepath)
        except (TimeoutError, IndexError):
            logging.warning(f"Timed out waiting for, or couldn't find, downloaded Excel file from {response.url}. Disregard if link error appeard as well. Otherwise, check manually if a file was downloaded and see if the filename searched for needs updating.")

        driver.close()

        # Parse DataFrames for each downloaded file and export Items to pipeline
        # TODO: un-hardcode-this by splitting the method in WARNSpider into components and using some of those here
        if saveFilepath:
            todf_kwargs = {'skip_header': 2}
            df = todf.excel(saveFilepath, **todf_kwargs)
            df.to_csv(os.path.splitext(saveFilepath)[0] + "_parsed.csv")           

            for index, row in df.iterrows():
                fields = row.to_dict()
                norm_fields = get_normalized_fields(self.fields_dict, row).to_dict()
                item = Entry(state_name=self.state_name,
                             timestamp=timestamp,
                             url=response.url,
                             fields=fields,
                             normalized_fields=norm_fields)
                yield item


class PAWARNSpider(WARNSpider):
    """Pennsylvania"""
    name = 'PAWARN'

    def parse(self, response):
        """Use state-specific data-to-DataFrame defined in customtodf module"""
        link_kwargs = {'find_in_href': 'warn/notices/Pages'}
        yield from self.parse_links(response, link_kwargs=link_kwargs, 
                                    custom_todf=ctodf.html_PA)


class RIWARNSpider(WARNSpider):
    """Rhode Island"""
    name = 'RIWARN'


class SCWARNSpider(WARNSpider):
    """South Carolina"""
    name = 'SCWARN'

    def parse(self, response):
        #link_kwargs = {'find_in_text': 'Layoff Notifications',
        #               'find_in_href': '.pdf'}
        links = self.get_links(response, find_in_text='Layoff Notifications', find_in_href='.pdf')
        for link in links:
            if any([(year in link) for year in ['2010', '2011', '2012']]):
                todf_kwargs = {'header_row': 1,
                               'header_on_all_pages': False}
            else:
                todf_kwargs = {'pdf_kwargs': {'flavor': 'stream',
                                              'row_tol': 6},
                               'header_row': 3} # All this is not quite enough but getting there!
            yield response.follow(link, callback=self.parse_as_df, cb_kwargs={'todf_kwargs': todf_kwargs})


class SDWARNSpider(WARNSpider):
    """South Dakota"""
    name = 'SDWARN'

    def parse(self, response):
        todf_kwargs = {'use_pandas': False,
                       'link_col': 1}
        yield from self.parse_as_df(response, todf_kwargs=todf_kwargs)


class TNWARNSpider(WARNSpider):
    """Tennessee"""
    name = 'TNWARN'

    def parse(self, response):
        """Use state-specific data-to-DataFrame defined in customtodf module"""

        # Use state-specific function for current data
        yield from self.parse_as_df(response, parse_as='html',
                                    custom_todf=ctodf.html_TN)

        # Parse archived PDFs
        link_kwargs = {'find_in_href': 'WarnReportByMonth.pdf'}
        yield from self.parse_links(response, format='pdf', link_kwargs=link_kwargs)


class TXWARNSpider(WARNSpider):
    """Texas"""
    name = 'TXWARN'

    def parse(self, response):
        link_kwargs = {'find_in_href': '.xls'}
        yield from self.parse_links(response, link_kwargs=link_kwargs)


class UTWARNSpider(WARNSpider):
    """Utah"""
    name = 'UTWARN'


class VTWARNSpider(JobLinkSpider):
    """Vermont"""
    name = 'VTWARN'


class VAWARNSpider(WARNSpider):
    """Virginia"""
    name = 'VAWARN'

    def parse(self, response):
        link_kwargs = {'find_in_href': '.csv',
                       'find_in_text': 'Download'}
        yield from self.parse_links(response, format='csv', link_kwargs=link_kwargs)


class WAWARNSpider(WARNSpider):
    """Washington"""
    name = 'WAWARN'

    def __init__(self, *args, **kwargs):
        super(WAWARNSpider, self).__init__(*args, **kwargs)
        self.page_count = 0

    def parse(self, response):
        """Run Selenium to click through pages of HTML archive
        """
        driver = self.initialize_webdriver()
        driver.get(response.url)

        todf_kwargs = {'drop_first_row': True}

        # Parse first page
        yield from self.parse_page(driver, response, todf_kwargs=todf_kwargs)

        # While pages remain, click next page link and parse the resulting page
        wait = WebDriverWait(driver, 5)
        nextPageElement = self.find_next_page_element(driver)
        while nextPageElement is not None:
            self.page_count += 1
            print(f"Following to page {self.page_count}")
            nextPageElement.click()
            wait.until(EC.staleness_of(nextPageElement))
            nextPageElement = self.find_next_page_element(driver)
            yield from self.parse_page(driver, response, todf_kwargs=todf_kwargs)
            
        else:
            logging.info(f"Parsed {self.page_count} pages")  

        driver.close()

    def parse_page(self, driver, response, todf_kwargs={}):
        """Parse the current page of the archive"""
        
        timestamp = datetime.now()
        saveFilename = f'{self.state_abbrev}_{timestamp.strftime("%Y%m%d%H%M%S")}_page{self.page_count}.html'
        saveFilepath = os.path.join(TMPDIR, saveFilename)
        with open(os.path.join(TMPDIR, saveFilepath), "w") as f:
            f.write(driver.page_source)

        df = todf.html(saveFilepath, **todf_kwargs)
        df.to_csv(os.path.splitext(saveFilepath)[0] + "_parsed.csv")    

        for index, row in df.iterrows():
            fields = row.to_dict()
            norm_fields = get_normalized_fields(self.fields_dict, row).to_dict()
            item = Entry(state_name=self.state_name,
                            timestamp=timestamp,
                            url=response.url,
                            fields=fields,
                            normalized_fields=norm_fields)
            yield item

    def find_next_page_element(self, driver):
        """Get the next active element after the current page element
        
        The pages are listed as 1 2 3 4 5 6 7 8 9 10 ...
        with the active page number non-clickable and the ... clickable.
        When 10 is reached, clicking ... will move to page 11 and reset 
        the page numbers listed. When the final page is reached, there
        will be no more clickable elements after it."""

        pageSection = driver.find_element_by_xpath('//tr[@style="color:#000066;background-color:#E6F2F9;"]')
        pageRow = pageSection.find_element_by_xpath('.//tr')
        pageElements = pageRow.find_elements_by_xpath('.//td')
        currentPageIndex = [i for i, x in enumerate(pageElements) if not element_has_href(x)][0]

        if currentPageIndex < len(pageElements) - 1:
            return pageElements[currentPageIndex + 1].find_element_by_xpath('.//a')
        else:
            return None


class WVWARNSpider(WARNSpider):
    """West Virginia"""
    name = 'WVWARN'

    def parse(self, response):
        """Use state-specific data-to-DataFrame defined in customtodf module"""
        
        download_link = self.get_links(response, find_in_text='Download Document')[0]
        yield response.follow(download_link, callback=self.parse_as_df,
                              cb_kwargs={'custom_todf': ctodf.pdf_WV})

class WIWARNSpider(WARNSpider):
    """Wisconsin"""
    name = 'WIWARN'

    def parse(self, response):
        """Selenium"""
        
        # Parse current data from landing pages
        todf_kwargs = {'use_pandas': False,
                       'link_col': 1,
                       'use_unicode': True}
        
        # For some reason tables aren't showing up on the html page downloaded
        # by Scrapy, only for the landing page, so using Selenium here only
        driver = self.initialize_webdriver()
        driver.get(response.url)
        
        # Save request to file
        timestamp = datetime.now()
        saveFilename = f'{self.state_abbrev}_{timestamp.strftime("%Y%m%d%H%M%S")}_.html'
        saveFilepath = os.path.join(TMPDIR, saveFilename)
        with open(os.path.join(TMPDIR, saveFilepath), "w", encoding='utf-8') as f:
            f.write(driver.page_source)

        driver.close()

        # Parse request
        df = todf.html(saveFilepath, **todf_kwargs)
        df.to_csv(os.path.splitext(saveFilepath)[0] + "_parsed.csv")    

        # Yield item
        for index, row in df.iterrows():
            fields = row.to_dict()
            norm_fields = get_normalized_fields(self.fields_dict, row).to_dict()
            item = Entry(state_name=self.state_name,
                            timestamp=timestamp,
                            url=response.url,
                            fields=fields,
                            normalized_fields=norm_fields)
            yield item

        #yield from self.parse_as_df(response, todf_kwargs=todf_kwargs)

        # Follow links, parsing in same format
        link_kwargs = {'find_in_text': 'Layoff Notice Information'}
        yield from self.parse_links(response, link_kwargs=link_kwargs, todf_kwargs=todf_kwargs)


class WYWARNSpider(WARNSpider):
    """Wyoming"""
    name = 'WYWARN'


# =============================================================================
# Helper functions for Selenium-based scraping
# =============================================================================  
class file_exists(object):
    """Selenium wait condition: has the file been downloaded (does it exist)?
    TODO: put this somewhere else!"""
    
    def __init__(self, filepath):
        self.filepath = filepath

    def __call__(self, driver):
        if os.path.isfile(self.filepath):
            return True
        else:
            return False    


class matching_file_in_directory(object):
    """Selenium wait condition: does a file matching a regular expression exist
    in the given directory (or CWD if no directory provided)?

    Returns file with "largest" value (should be the newest) if multiple exist

    TODO: put this somewhere else!"""
    
    def __init__(self, r, directory=''):
        """r: compiled regular expression"""
        self.r = r
        self.directory = directory 

    def __call__(self, driver):
        fileList = os.listdir(self.directory)
        matchingFiles = list(filter(self.r.match, fileList))
        if len(matchingFiles) > 0:
            matchingFiles.sort(reverse=True)
            return matchingFiles[0]
        else:
            return False   


class find_next_page_element(object):
    """Selenium wait condition: Return next page element to click, or None if none exists
    TODO: put this somewhere else!"""

    def __call__(self, driver):
        pageSection = driver.find_element_by_xpath('//tr[@style="color:#000066;background-color:#E6F2F9;"]')
        pageRow = pageSection.find_element_by_xpath('.//tr')
        pageElements = pageRow.find_elements_by_xpath('.//td')
        print(pageElements.text)
        currentPageIndex = [i for i, x in enumerate(pageElements) if x.get_attribute('href') is None][0]

        if currentPageIndex < len(pageElements) - 1:
            nextPageElement = pageRow.find_element_by_xpath(f'.//td[{currentPageIndex + 1}]')
            return nextPageElement
        else:
            return False


class text_has_changed(object):
    """Selenium wait condition: Check if element text has changed
    TODO: put this somewhere else!"""

    def __init__(self, locator, original_text):
        """r: compiled regular expression"""
        self.locator = locator
        self.original_text = original_text

    def __call__(self, driver):
        element = driver.find_element(*self.locator)
        return (element.text != self.original_text)


class element_to_not_be_clickable(object):
    """ An Expectation for checking an element is not enabled such that
    you cannnot click it."""
    def __init__(self, locator):
        self.locator = locator

    def __call__(self, driver):
        element = visibility_of_element_located(self.locator)(driver)
        if element and not(element.is_enabled()):
            return element
        else:
            return False

def element_has_href(element):
    """Check if an element (or child of an element) has href without raising exception"""
    try:
        element = element.find_element_by_xpath('.//*[@href]')
        return True
    except NoSuchElementException:
        return False