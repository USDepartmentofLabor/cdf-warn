"""Not all states have a simple tabular structure for their archive. For these cases,
a custom parsing function is necessary.

The naming convention for the custom parsing functions is {format}_{state_abbrev}.

Currently, this module contains custom parsing functions for West Virginia, Hawaii,
Pennsylvania, New Jersey, and Tennessee. 

These can be used in a spider's parse_as_df function in place of the standard todf
function by providing the custom_todf keyword argument.
"""

import os
import re
import logging
import camelot
import pandas as pd
import openpyxl
import numpy as np
import bs4
from datetime import date
from lxml import etree
from lxml.etree import XMLSyntaxError
from urllib.parse import urljoin

from modules.utils import whitespace_to_singlespace, get_text_of_matching_elements_lxml
import modules.datatodf as todf


def pdf_WV(filepath):
    """Convert PDF tables to DataFrame for West Virginia

    Unlike other states, WV's PDF files contain individual tables for each WARN entry.
    This function reads each table, transposes it, and merges rows/columns together
    where necessary. For example, based on how Camelot parses tables with split cells,
    two different locations provided within the same entry would originally appear under
    two different columns, one unnamed; this code appends the value from the unnamed
    column to that of the column to the left.
    """

    kwargs = {'pages': 'all',
              'flavor': 'lattice',
              'split_text': True}
    tables = camelot.read_pdf(filepath, **kwargs)

    df_all = pd.DataFrame()
    for i, table in enumerate(tables):
        df = table.df
        df = df.transpose()

        n_cols_current = len(df.columns)
        prev_col_name = ''
        series = pd.Series()
        for j in range(n_cols_current):
            # Get column names from the first row, then collapse the remaining rows
            # into a single row. Usually there is just one remaining row anyway,
            # but sometimes split cells end up in a second row.
            current_col_name = df[j].iloc[0]
            current_col_val = "\n".join(df[j].iloc[1:]) 
            if j == 0 and not current_col_name:
                # If the notice has been updated, the first column will be unnamed
                col_name = 'Update Note'
                series[col_name] = current_col_val.strip()
                prev_col_name = col_name
            elif not current_col_name:
                # If column is otherwise unnamed, append its value to the previous
                # column and don't update previous column name
                series[prev_col_name] = series[prev_col_name] + '\n' + current_col_val.strip()
            else:
                series[current_col_name] = current_col_val.strip()
                prev_col_name = current_col_name

        df_all = df_all.append(series, ignore_index=True)

    return df_all


def html_HI(filepath, format='html'):
    """Convert HTML database (not a table) to DataFrame for Hawaii
    
    Currently HI only provides "Date Received"-"Company" pairs with the 
    original WARN notice PDF link in the same line and updated notice
    links or other notes in subsequent lines.

    There are a lot of specific rules to catch/parse everything."""
    
    df = pd.DataFrame()
    with open(filepath, 'r') as f:            
        tree = etree.parse(f, etree.HTMLParser())
        content = tree.xpath('//div[contains(@class,"primary-content")]')[-1]
        lines = content.xpath('.//p')
        for line in lines:
            str_list = line.xpath('string(.)').split('–', 1)
            if len(str_list) == 2:
                if len(str_list[-1]) > 100:
                    # Captures 2019 date-company pairs with original notice links
                    full_str_list = line.xpath('string(.)').split('\n')
                    hrefs = line.xpath('.//*/@href')
                    for i, item in enumerate(full_str_list):
                        date_str, company_str = item.split('–')
                        entry = make_HI_Series(date_str, company_str, hrefs[i])
                        df = df.append(entry, ignore_index=True)
                else:
                    # Captures 2020-onward date-company pairs with original notice links
                    date_str, company_str = str_list
                    hrefs = line.xpath('.//*/@href')
                    if len(hrefs) == 1:
                        href = hrefs[0]
                    else:
                        href = ''
                    entry = make_HI_Series(date_str, company_str, href)
                    df = df.append(entry, ignore_index=True)
            elif len(str_list) == 1:
                # Add amended/updated/supplemented notice links or 
                # other notes to the previous entry
                note = str_list[0].strip()
                hrefs = line.xpath('.//*/@href')
                if note.startswith('*'):
                    if len(hrefs) > 0:
                        # Add link to updated notice (there may be multiple such updates)
                        updates_list = df.iloc[-1, df.columns.get_loc('Updated Links')]
                        df.iloc[-1, df.columns.get_loc('Updated Links')] = updates_list + hrefs
                    else:
                        # Add general note
                        # MINOR BUG: this note may not actually correspond to the entry
                        # to which it is attached
                        notes_list = df.iloc[-1, df.columns.get_loc('Notes')]
                        if len(notes_list) > 0:
                            note = '\t' + note
                        df.iloc[-1, df.columns.get_loc('Notes')] = notes_list + note
            else:
                logging.warning("For HI: did not capture following text as part of an entry: {line}")   
    return df


def html_PA(filepath, format='html'):
    """Convert somewhat-tabular HTML to DataFrame for Pennsylvania
    
    Although the format looks the same across all years when displayed,
    the actual HTML formatting varies enough between years to be a pain.

    BUG: Currently this still doesn't quite catch everything!
    """

    df_all = pd.DataFrame()

    with open(filepath, 'r') as f:  
        tree = etree.parse(f, etree.HTMLParser())
        tables = tree.xpath('//table')
        if tables:
            for table in tables:
                rows = table.xpath('.//tbody/tr')
                if len(rows) == 0:
                    logging.error(f"No table rows found; may need to updated xpath selectors or url")
                    return df_all

                for row in rows:
                    td = row.xpath('.//td')

                    for item in td:
                        lines = item.xpath('string(.)')
                        lineContent = lines.strip()
                        if lineContent:
                            # Pop known fields from list of lines and save remaining lines to an extra field
                            
                            boldedLines = item.xpath('.//strong/text()')
                            print(boldedLines)

                            entry = pd.Series()

                            # Split by newline
                            lineStrs = [l.strip() for l in lines.split('\n') if len(l.strip()) > 0]

                            # In later years, field don't always get split up properly: fix that
                            fieldList = ['COUNTY', '# AFFECTED', 'EFFECTIVE DATE', 'CLOSURE OR LAYOFF', 'CLOSING OR LAYOFF']
                            for field in fieldList:
                                try:
                                    updateIndex = [i for (i, l) in enumerate(lineStrs) if field in l][0]
                                    if not lineStrs[updateIndex].startswith(field):
                                        fields = lineStrs.pop(updateIndex).split(field)
                                        fields[-1] = f"{field}{fields[-1]}"
                                        lineStrs.extend(fields)
                                except IndexError:
                                    # Not all fields are included/labeled in all years
                                    pass

                            # Check if update; remove that line from list
                            if 'UPDATE' in lineStrs[0]:
                                print(lineStrs[0])
                                info = lineStrs.pop(0).rsplit('*', 1)
                                entry['Update Status'] = info[0].strip('*').strip()
                                if len(info[1].strip()) > 0:
                                    entry['Company'] = info[1].strip()
                            else:
                                entry['Company'] = lineStrs.pop(0) # Assumes company is only one line! Not sure that's always the case

                            # Assign all remaning lines before a named field to "Address" field
                            
                            nextIndex = [i for (i, l) in enumerate(lineStrs) if any(l.startswith(field) for field in fieldList)][0]
                            entry['Address'] = '\n'.join(lineStrs[:nextIndex]).strip()
                            for i in range(nextIndex):
                                lineStrs.pop(0)

                            # Get other common fields
                            for field in fieldList:
                                try:
                                    index = [i for (i, x) in enumerate(lineStrs) if field in x][0]
                                    value = lineStrs.pop(index)
                                    value = value.replace(f'{field}: ', '')
                                    value = value.replace(f'LAYOFF', '') # Included due to differences in field names across years
                                    entry[field] = value
                                except IndexError:
                                    # Not all fields are included/labeled in all years
                                    pass

                            # Get "Reason" field, if provided w/o label
                            if len(lineStrs) > 0:
                                if lineStrs[-1].isupper():
                                    entry['Reason'] = lineStrs.pop(-1)

                            # Rename closure/closing or layoff field
                            for key in ['CLOSING OR LAYOFF', 'CLOSURE OR LAYOFF']:
                                if key in entry:
                                    entry['Reason'] = entry.pop(key)

                            # Add any leftover information to a separate field
                            entry['Additional Information'] = "\n".join(lineStrs)

                            # Save full text to be on the safe side
                            entry['Full Cell Text'] = lineContent
                        
                            df_all = df_all.append(entry, ignore_index=True)

        else:
            logging.error(f"No table found; may need to updated xpath selectors or url")

    return df_all


def html_NJ(filepath, format='html'):
    """Convert HTML "table" to DataFrame for New Jersey
    
    This format has each row, including column headers, as individual single-row tables.
    This function assumes the first table found contains column headers and the rest
    contain a single entry.
    """

    df_all = pd.DataFrame()
    with open(filepath, 'r') as f:  
        tree = etree.parse(f, etree.HTMLParser())
        tables = tree.xpath('//table')
        if tables:
            for i, table in enumerate(tables):
                rows = table.xpath('.//tr')
                if len(rows) != 1:
                    logging.warning("html_NJ assumes each table has exactly one row, which is not the case - may need to update function")
                if i == 0 and len(rows) > 0:
                    columns = get_text_of_matching_elements_lxml(rows[0], './/td')
                elif len(rows) > 0:
                    td = get_text_of_matching_elements_lxml(rows[0], './/td')
                    fields = dict(zip(columns, td))

                    df_all = df_all.append(fields, ignore_index=True)
        else:
            logging.error(f"No table found; may need to updated xpath selectors or url")

    # For some years, the table has normal formatting - in this case,
    # use the normal html table code since the above code won't find results.
    # If a month just happens not to have results, this doesn't change anything.
    if len(df_all.index) == 0:
        df_all = todf.html(filepath)

    return df_all


def html_TN(filepath, format='html'):
    """For Tennessee"""

    df_all = pd.DataFrame()

    with open(filepath, 'r') as f:  
        tree = etree.parse(f, etree.HTMLParser())
        content = tree.xpath('//div[contains(@class,"tn-rte parbase")]')[1]
        lines = content.xpath('.//p')
        for line in lines:
            str_list = line.xpath('string(.)').split('|')

            if len(str_list) > 7:
                str_lists = line.xpath('string(.)').split('\n')
                hrefs = line.xpath('.//*/@href')
                if len(hrefs) != 2:
                    hrefs = ['', '']
                df_all = df_all.append(make_TN_Series(str_list[0], href[0]), ignore_index=True)
                df_all = df_all.append(make_TN_Series(str_list[-1], href[-1]), ignore_index=True)
            elif len(str_list) > 1:
                hrefs = line.xpath('.//*/@href')
                if len(hrefs) == 1:
                    href = hrefs[0]
                else:
                    href = ''
                df_all = df_all.append(make_TN_Series(str_list, href), ignore_index=True)
    return df_all


# =============================================================================
# Generate pd.Series for specified state
# =============================================================================
def make_TN_Series(str_list, href):
    """For Tennessee: create Series entry that will become Scrapy Item. """

    entry = pd.Series()
    keys = ['Date Notice Posted', 'Company', 'County', 'Affected Workers', 'Closure/Layoff Date', 'Notice/Type']

    for key in keys:
        field = [re.sub(f'{key}:', '', s).strip() for s in str_list if key in s]
        if len(field) == 0:
            field = ''
        else:
            field = field[0]
        entry[key] = field

        entry['Notice Link'] = href

    return entry


def make_HI_Series(date_str, company_str, href_str):
    """For Hawaii: create Series entry that will become Scrapy Item. """

    entry = pd.Series()
    entry['Date Received'] = date_str.strip()
    entry['Company'] = company_str.strip()
    entry['Notice Link'] = href_str
    entry['Updated Links'] = []
    entry['Notes'] = ''
    if entry['Company'].endswith('*'):
        entry['Notes'] = '(Scraping note: see HI WARN notice website for further information)'
        entry['Company'] = entry['Company'].strip('* ')

    return entry