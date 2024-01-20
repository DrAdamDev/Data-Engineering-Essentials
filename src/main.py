import sqlite3
from data_extraction import extract_excel_data
from data_cleaning import clean_data
from database_operations import load_into_database

def main():
    employment_data_url = 'https://www.ons.gov.uk/file?uri=/employmentandlabourmarket/peopleinwork/employmentandemployeetypes/datasets/regionbybroadindustrygroupsicbusinessregisterandemploymentsurveybrestable4/2021provisional/table42021p.xlsx'

    employment_data_dict = extract_excel_data(employment_data_url)
    cleaned_regional_data = clean_data(employment_data_dict)

    conn = sqlite3.connect('regional_UK_employment.db')
    load_into_database(cleaned_regional_data, conn)
    conn.commit()
    conn.close()

if __name__ == '__main__':
    main()
