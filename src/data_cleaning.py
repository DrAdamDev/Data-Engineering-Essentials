import pandas as pd

def delete_meta_data(data_dict):
    data_dict.pop('Information', None)
    data_dict.pop('Contents', None)

def assign_new_column_names(region, new_names, sheet):
    try:
        region.rename(columns=new_names, inplace=True)
        region.insert(loc=0, column='Region_Name', value=sheet)
    except KeyError as e:
        print("Error in assigning new column names:", str(e))

def drop_redundant_columns(region):
    region.drop([col for col in region.columns if 'Unnamed' in col], axis=1, inplace=True)

def drop_redundant_rows(region, redundant_rows):
    region.drop(redundant_rows, inplace=True, errors='ignore')

def commit_row_changes(region):
    region.reset_index(drop=True, inplace=True)

def process_non_numerical_values(region):
    region.replace({'-': 0, '*': None}, inplace=True)

def update_column_dtypes(region, new_dtypes):
    try:
        for column in new_dtypes:
            if column in region.columns:
                region[column] = region[column].astype(new_dtypes[column], errors='ignore')
    except KeyError as e:
        print("Error in updating column dtypes:", str(e))

def generate_indentifiers(dataframe):
    try:
        dataframe['Region_ID'] = dataframe['Region_Name'].factorize()[0]
        dataframe['BIG_ID'] = dataframe['BIG_Name'].factorize()[0]
        dataframe['Region_BIG_ID'] = pd.MultiIndex.from_frame(dataframe[['Region_Name', 'BIG_Name']]).factorize()[0]
    except Exception as e:
        print("Error in generating identifiers:", str(e))

def clean_data(data_dict):
    new_column_names = {
        'Table 4 - Regional level employment (thousands) by BIG (public/private sector split)': 'BIG_Name',
        'Unnamed: 1': 'FT_Public',
        'Unnamed: 2': 'FT_Private',
        'Unnamed: 3': 'FT_Pub_Priv',
        'Unnamed: 4': 'PT_Public',
        'Unnamed: 5': 'PT_Private',
        'Unnamed: 6': 'PT_Pub_Priv',
        'Unnamed: 7': 'FTPT_Public',
        'Unnamed: 8': 'FTPT_Private',
        'Unnamed: 9': 'FTPT_Pub_Priv',
        'Unnamed: 10': 'All_Public',
        'Unnamed: 11': 'All_Private',
        'Unnamed: 12': 'All_Pub_Priv'
    }

    new_column_dtypes = {
        'BIG_Name': str,
        'FT_Public': float,
        'FT_Private': float,
        'FT_Pub_Priv': float,
        'PT_Public': float,
        'PT_Private': float,
        'PT_Pub_Priv': float,
        'FTPT_Public': float,
        'FTPT_Private': float,
        'FTPT_Pub_Priv': float,
        'All_Public': float,
        'All_Private': float,
        'All_Pub_Priv': float 
    }
    
    redundant_rows = [0, 1, 2, 21, 22, 23, 24, 25, 26, 27, 28]

    delete_meta_data(data_dict)

    regional_dfs = []
    for sheet, region in data_dict.items():
        assign_new_column_names(region, new_column_names, sheet)
        drop_redundant_columns(region)
        drop_redundant_rows(region, redundant_rows)
        commit_row_changes(region)
        process_non_numerical_values(region)
        update_column_dtypes(region, new_column_dtypes)
        regional_dfs.append(region)

    cleaned_regional_data = pd.concat(regional_dfs, ignore_index=True)
    generate_indentifiers(cleaned_regional_data)

    return cleaned_regional_data
