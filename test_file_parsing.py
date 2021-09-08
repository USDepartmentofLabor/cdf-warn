import os
import camelot
import pandas as pd

import modules.datatodf as todf
import modules.customdatatodf as ctodf

directory = 'tmp/test/'
files = os.listdir(directory)

state = 'PA'
extension = '.html'
to_df = ctodf.html_PA
#pdf_kwargs = {'flavor': 'stream', 'columns': ['35,94,163,335,400,473']}
#todf_kwargs = {'pdf_kwargs': pdf_kwargs, 'first_page_header': 2}

def clean_df(df_raw):
    df = df_raw.copy(deep=True)
    df = todf.drop_empty_rows_cols(df)

    return df


def run():
    fpath = f'{directory}{f}'
    df = to_df(fpath)#, **todf_kwargs)
    #df = clean_df(df)
    df = todf.drop_empty_rows_cols(df)
    df.to_csv(os.path.splitext(fpath)[0] + "_parsed_test.csv")


catchExceptions = True
for f in files:
    name, ext = os.path.splitext(f)
    if (name.startswith(state) and ext == extension):
        print(f"Processing {f}...")
        if catchExceptions:
            try:
                run()
            except Exception as e:
                print(f"\n--> Exception for {f}: {e}\n")
        else:
            run()

