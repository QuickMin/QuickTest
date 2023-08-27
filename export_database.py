import os
import re
import sqlite3
import pathlib
import pandas as pd
import datetime
import argparse


ap = argparse.ArgumentParser()
ap.add_argument("-v","--value",required=True,help="Input your date")
ap.add_argument("-w","--working_directory")
args = vars(ap.parse_args())
# date_format_regex = r"[0-3]?[0-9]-[0-3]?[0-9]-(?:[0-9]{2})?[0-9]{2}"
try:
    date = args.get("value",1)
    WORKING_DIRCTORY = args.get("working_directory")

    # print(date)
    # print(type(date))
    # # date = str(date).strip()
    # if re.findall(r"[0-3]?[0-9]-[0-3]?[0-9]-(?:[0-9]{2})?[0-9]{2}",date):
    #     date_object = datetime.datetime.strptime("%Y-%m-%d")
    #     print(date_object.days)
    #     WORKING_DIRCTORY = args.get("working_directory")
    #     print(WORKING_DIRCTORY)
    # else:
    #     print("Enter valid date format(year-month-day / 2023-01-04)")
    #     exit()
except  Exception as e:
    print("===================================")
    print("Enter valid date format(year-month-day / 2023-01-04)")
    print("===================================")
    exit() 



root_path = pathlib.Path(__file__).parent
SQLITE_PATH = "C:/RPA/Robotic Process Automation/LetterActions/database/letter_action.db"

class TableCreationError(Exception):
    def __init__(self,message="Table Craetion Error"):
        self.message = message 
        super().__init__(self.message)

    def __str__(self):
        return f'{self.message}'

class MultipleValueReturn(Exception):
    def __init__(self,message="Multivalue Returned."):
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f'{self.message}'

class LetterAction:

    def __init__(
        self) -> None:

        self.table_name = 'action_users'
        self.file_tablename = 'filename'
        # self.db = db
        self.conn = sqlite3.connect(SQLITE_PATH)
        print('Connected')
        self.cursor = self.conn.cursor()
        self.instance = None

    def get_all_filename_on_created_at(self) -> list[str]:
        global date
        query = f"SELECT filename FROM {self.file_tablename} WHERE date(created_at) = date('{date}') "
        df = pd.read_sql_query(query, self.conn)
        res = df.values.tolist()
        return res


    def generate_report(self):
        global date

        dt_type = {
            'sn': int,
            'identification_info_pan_no': int
        }
       
        def transform_value1(row1):
            if row1['status'] == 'Match not Found':
                return ''
            else:
                return row1['action_taken']
            
        def transform_value2(row2):
            if row2['status'] == 'Match not Found':
                return ''
            else:
                return row2['updated_at']

        # temp = filename.split('.')[0]
        # temp = temp.replace(' ', '')
        report_path = f'C:/RPA/Robotic Process Automation/LetterActions/report/{datetime.datetime.now().strftime("%Y-%m-%d")}_dashboard_report.xlsx'
        report_query = f'''
            SELECT 
            sn, source_nrb_no_direct_letter, source_date, instructing_agency_chalani_no, instructing_agency_date, instructing_agency_name,
            instructing_agency_address, `action`, enquiry_type, reason, personal_information_nature_of_account, personal_information_gender,
            personal_information_salutation, personal_information_first_name_name, personal_information_middle_name, personal_information_last_name,
            identification_info_pan_no, identification_info_registration_no, identification_info_citizenship, father_name, father_middle_name,
            father_last_name, grand_father_name, grand_father_middle_name, grand_father_last_name, spouse_name, spouse_middle_name, spouse_last_name,
            account_number, address_country, address_state,address_district, address_mn_vdc, address_ward_no, address_tole, status, client_code,
            main_code, search_type, updated_at, `action` as action_taken, file
            FROM {self.table_name}
            WHERE date(updated_at) = date('{date}')
        '''
        df = pd.read_sql(report_query, self.conn, dtype=dt_type)
        df.insert(39, 'account_holder', '')
        values = df['status'].values.tolist()

        print(f'values = {values}')
        df['file'] = [f'ftp://10.20.101.11/Enforcement_Correspondence/EnforcementLetters/{x}' if str(x).find('None') else
                      'ftp://10.20.101.11/Enforcement_Correspondence/EnforcementLetters/'
                       for x in df['file'].values.tolist()]
        df = df.replace(to_replace=['processed'], value=['Match Found'])
        df = df.replace(to_replace=['la_error'], value=['Match Found'])
        df = df.replace(to_replace=['success'], value=['Match Found'])
        df = df.replace(to_replace=['error'], value=['Match Found'])
        df = df.replace(to_replace=['multimatched'], value=['Match not Found'])
        df = df.replace(to_replace= ['retry'], value=['Match not Found'])
        df = df.replace(to_replace=[ 'new'], value=['Match not Found'])
        df = df.replace(to_replace=['w_error'], value=['Match not Found'])
        df['action_taken'] = df.apply(transform_value1, axis=1)
        df['updated_at'] = df.apply(transform_value2, axis=1)
        df.rename(columns={
            'status':"in_pumari",
            'search_type':'role'
        }, inplace=True)
        with pd.ExcelWriter(report_path) as writer:
            df.to_excel(writer, sheet_name='success', index=False)
        return report_path



if __name__ == "__main__":
    print('Report generation started.')
    letter_action = LetterAction()

    files_list = letter_action.get_all_filename_on_created_at()
    # for file in files_list:
    # print(f'filelist = {isinstance(file, list)}')
    # if isinstance(file, list):
    #     temp_file = file[0]
    # elif isinstance(file, str):
    #     temp_file = file
    # else:
    #     raise Exception(f'Data format error required string but found {type(file)}')
    path = letter_action.generate_report()
    if path:
        print("========================")
        print("Successfully database is exported to excel.")
        print("\n")
        print(f'Report file path is {path}')
        print("\n")
        print("Keep smiling. :)")
        print("========================")
    else:
        print("========================")
        print("Error on database export.")
        print(f"Either there is no data for this {date} or mistaken a data format.")
        print("\n\n")
        print("Keep smiling. :)")
        print("========================")

# import os
# os.system("python export_database.py")
    
   
