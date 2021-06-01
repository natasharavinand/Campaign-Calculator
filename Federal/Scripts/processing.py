
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen

import sys

import re

import pandas as pd
import numpy as np

from fuzzywuzzy import fuzz

if __name__ == '__main__':
    
    # cm_header_url = input("Enter URL for Candidate Master Header File: ")

    cm_header_url = "https://www.fec.gov/files/bulk-downloads/data_dictionaries/cn_header_file.csv"
    cm_header = pd.read_csv(cm_header_url)

    # cm_url = input("Enter URL for Candidate Master File: ")

    cm_url_201112 = "https://www.fec.gov/files/bulk-downloads/2012/cn12.zip"

    cm_resp_201112 = urlopen(cm_url_201112)
    cm_zipfile_201112 = ZipFile(BytesIO(cm_resp_201112.read()))
    cm_file_201112 = cm_zipfile_201112.namelist()[0]

    original_stdout = sys.stdout

    with open(cm_file_201112, "w+") as f_201112:
        sys.stdout = f_201112
        for line in cm_zipfile_201112.open(cm_file_201112).readlines():
            print(line.decode('utf-8'))

    sys.stdout = original_stdout

    candidate_master_201112 = pd.read_csv('./cn.txt', delimiter='|', names=list(cm_header.columns))

    cm_url_201314 = "https://www.fec.gov/files/bulk-downloads/2014/cn14.zip"

    cm_resp_201314 = urlopen(cm_url_201314)
    cm_zipfile_201314 = ZipFile(BytesIO(cm_resp_201314.read()))
    cm_file_201314 = cm_zipfile_201314.namelist()[0]

    original_stdout = sys.stdout

    with open(cm_file_201314, "w+") as f_201314:
        sys.stdout = f_201314
        for line in cm_zipfile_201314.open(cm_file_201314).readlines():
            print(line.decode('utf-8'))

    sys.stdout = original_stdout

    candidate_master_201314 = pd.read_csv('./cn.txt', delimiter='|', names=list(cm_header.columns))

    cm_url_201516 = "https://www.fec.gov/files/bulk-downloads/2016/cn16.zip"

    cm_resp_201516 = urlopen(cm_url_201516)
    cm_zipfile_201516 = ZipFile(BytesIO(cm_resp_201516.read()))
    cm_file_201516 = cm_zipfile_201516.namelist()[0]

    original_stdout = sys.stdout

    with open(cm_file_201516, "w+") as f_201516:
        sys.stdout = f_201516
        for line in cm_zipfile_201516.open(cm_file_201516).readlines():
            print(line.decode('utf-8'))

    sys.stdout = original_stdout

    candidate_master_201516 = pd.read_csv('./cn.txt', delimiter='|', names=list(cm_header.columns))

    cm_url_201718 = "https://www.fec.gov/files/bulk-downloads/2018/cn18.zip"

    cm_resp_201718 = urlopen(cm_url_201718)
    cm_zipfile_201718 = ZipFile(BytesIO(cm_resp_201718.read()))
    cm_file_201718 = cm_zipfile_201718.namelist()[0]

    original_stdout = sys.stdout

    with open(cm_file_201718, "w+") as f_201718:
        sys.stdout = f_201718
        for line in cm_zipfile_201718.open(cm_file_201718).readlines():
            print(line.decode('utf-8'))

    sys.stdout = original_stdout

    candidate_master_201718 = pd.read_csv('./cn.txt', delimiter='|', names=list(cm_header.columns))

    cm_url_201920 = "https://www.fec.gov/files/bulk-downloads/2020/cn20.zip"

    cm_resp_201920 = urlopen(cm_url_201920)
    cm_zipfile_201920 = ZipFile(BytesIO(cm_resp_201920.read()))
    cm_file_201920 = cm_zipfile_201920.namelist()[0]

    original_stdout = sys.stdout

    with open(cm_file_201920, "w+") as f_201920:
        sys.stdout = f_201920
        for line in cm_zipfile_201920.open(cm_file_201920).readlines():
            print(line.decode('utf-8'))

    sys.stdout = original_stdout

    candidate_master_201920 = pd.read_csv('./cn.txt', delimiter='|', names=list(cm_header.columns))

    candidate_master = candidate_master_201112.append(candidate_master_201314).append(candidate_master_201516).append(candidate_master_201718).append(candidate_master_201920)

    '''
    Loading in candidate-committee linkage CSV from FEC bulk data site
    '''

    ccl_header_url = "https://www.fec.gov/files/bulk-downloads/data_dictionaries/ccl_header_file.csv"
    ccl_header = pd.read_csv(ccl_header_url)

    ccl_url_201112 = "https://www.fec.gov/files/bulk-downloads/2012/ccl12.zip"

    ccl_resp_201112 = urlopen(ccl_url_201112)
    ccl_zipfile_201112 = ZipFile(BytesIO(ccl_resp_201112.read()))
    ccl_file_201112 = ccl_zipfile_201112.namelist()[0]

    original_stdout = sys.stdout

    with open(ccl_file_201112, "w+") as file_201112:
        sys.stdout = file_201112
        for line in ccl_zipfile_201112.open(ccl_file_201112).readlines():
            print(line.decode('utf-8'))

    sys.stdout = original_stdout

    ccl_201112 = pd.read_csv('./ccl.txt', delimiter='|', names=list(ccl_header.columns))

    ccl_url_201314 = "https://www.fec.gov/files/bulk-downloads/2014/ccl14.zip"

    ccl_resp_201314 = urlopen(ccl_url_201314)
    ccl_zipfile_201314 = ZipFile(BytesIO(ccl_resp_201314.read()))
    ccl_file_201314 = ccl_zipfile_201314.namelist()[0]

    original_stdout = sys.stdout

    with open(ccl_file_201314, "w+") as file_201314:
        sys.stdout = file_201314
        for line in ccl_zipfile_201314.open(ccl_file_201314).readlines():
            print(line.decode('utf-8'))

    sys.stdout = original_stdout

    ccl_201314 = pd.read_csv('./ccl.txt', delimiter='|', names=list(ccl_header.columns))

    ccl_url_201516 = "https://www.fec.gov/files/bulk-downloads/2016/ccl16.zip"

    ccl_resp_201516 = urlopen(ccl_url_201516)
    ccl_zipfile_201516 = ZipFile(BytesIO(ccl_resp_201516.read()))
    ccl_file_201516 = ccl_zipfile_201516.namelist()[0]

    original_stdout = sys.stdout

    with open(ccl_file_201516, "w+") as file_201516:
        sys.stdout = file_201516
        for line in ccl_zipfile_201516.open(ccl_file_201516).readlines():
            print(line.decode('utf-8'))

    sys.stdout = original_stdout

    ccl_201516 = pd.read_csv('./ccl.txt', delimiter='|', names=list(ccl_header.columns))

    ccl_url_201718 = "https://www.fec.gov/files/bulk-downloads/2018/ccl18.zip"

    ccl_resp_201718 = urlopen(ccl_url_201718)
    ccl_zipfile_201718 = ZipFile(BytesIO(ccl_resp_201718.read()))
    ccl_file_201718 = ccl_zipfile_201718.namelist()[0]

    original_stdout = sys.stdout

    with open(ccl_file_201718, "w+") as file_201718:
        sys.stdout = file_201718
        for line in ccl_zipfile_201718.open(ccl_file_201718).readlines():
            print(line.decode('utf-8'))

    sys.stdout = original_stdout

    ccl_201718 = pd.read_csv('./ccl.txt', delimiter='|', names=list(ccl_header.columns))

    ccl_url_201920 = "https://www.fec.gov/files/bulk-downloads/2020/ccl20.zip"

    ccl_resp_201920 = urlopen(ccl_url_201920)
    ccl_zipfile_201920 = ZipFile(BytesIO(ccl_resp_201920.read()))
    ccl_file_201920 = ccl_zipfile_201920.namelist()[0]

    original_stdout = sys.stdout

    with open(ccl_file_201920, "w+") as file_201920:
        sys.stdout = file_201920
        for line in ccl_zipfile_201920.open(ccl_file_201920).readlines():
            print(line.decode('utf-8'))

    sys.stdout = original_stdout

    ccl_201920 = pd.read_csv('./ccl.txt', delimiter='|', names=list(ccl_header.columns))

    ccl = ccl_201112.append(ccl_201314).append(ccl_201516).append(ccl_201718).append(ccl_201920)

    """# Loading in Operating Expenditures"""

    '''
    Loading in operating expenditures CSV from FEC bulk data site
    '''

    opexp_header_url = "https://www.fec.gov/files/bulk-downloads/data_dictionaries/oppexp_header_file.csv"
    opexp_header = pd.read_csv(opexp_header_url)

    opexp_url_201112 = "https://www.fec.gov/files/bulk-downloads/2012/oppexp12.zip"

    opexp_resp_201112 = urlopen(opexp_url_201112)
    opexp_zipfile_201112 = ZipFile(BytesIO(opexp_resp_201112.read()))
    opexp_file_201112 = opexp_zipfile_201112.namelist()[0]

    original_stdout = sys.stdout

    with open(opexp_file_201112, "w+") as f_201112:
        sys.stdout = f_201112
        for line in opexp_zipfile_201112.open(opexp_file_201112).readlines():
            print(line.decode('utf-8'))

    sys.stdout = original_stdout

    opp_exp_csv_201112 = pd.read_csv('./oppexp.txt', delimiter='|', header = None)

    opp_exp_csv_201112 = opp_exp_csv_201112.drop(25, axis=1)

    opp_exp_csv_201112.columns = list(opexp_header.columns)

    opexp_url_201314 = "https://www.fec.gov/files/bulk-downloads/2014/oppexp14.zip"

    opexp_resp_201314 = urlopen(opexp_url_201314)
    opexp_zipfile_201314 = ZipFile(BytesIO(opexp_resp_201314.read()))
    opexp_file_201314 = opexp_zipfile_201314.namelist()[0]

    original_stdout = sys.stdout

    with open(opexp_file_201314, "w+") as f_201314:
        sys.stdout = f_201314
        for line in opexp_zipfile_201314.open(opexp_file_201314).readlines():
            print(line.decode('utf-8'))

    sys.stdout = original_stdout

    opp_exp_csv_201314 = pd.read_csv('./oppexp.txt', delimiter='|', header = None)

    opp_exp_csv_201314 = opp_exp_csv_201314.drop(25, axis=1)

    opp_exp_csv_201314.columns = list(opexp_header.columns)

    opexp_url_201516 = "https://www.fec.gov/files/bulk-downloads/2016/oppexp16.zip"

    opexp_resp_201516 = urlopen(opexp_url_201516)
    opexp_zipfile_201516 = ZipFile(BytesIO(opexp_resp_201516.read()))
    opexp_file_201516 = opexp_zipfile_201516.namelist()[0]

    original_stdout = sys.stdout

    with open(opexp_file_201516, "w+") as f_201516:
        sys.stdout = f_201516
        for line in opexp_zipfile_201516.open(opexp_file_201516).readlines():
            print(line.decode('utf-8'))

    sys.stdout = original_stdout

    opp_exp_csv_201516 = pd.read_csv('./oppexp.txt', delimiter='|', header = None)

    opp_exp_csv_201516 = opp_exp_csv_201516.drop(25, axis=1)

    opp_exp_csv_201516.columns = list(opexp_header.columns)

    opexp_url_2011718 = "https://www.fec.gov/files/bulk-downloads/2018/oppexp18.zip"

    opexp_resp_201718 = urlopen(opexp_url_2011718)
    opexp_zipfile_201718 = ZipFile(BytesIO(opexp_resp_201718.read()))
    opexp_file_201718 = opexp_zipfile_201718.namelist()[0]

    original_stdout = sys.stdout

    with open(opexp_file_201718, "w+") as f_201718:
        sys.stdout = f_201718
        for line in opexp_zipfile_201718.open(opexp_file_201718).readlines():
            print(line.decode('utf-8'))

    sys.stdout = original_stdout

    opp_exp_csv_201718 = pd.read_csv('./oppexp.txt', delimiter='|', header = None)

    opp_exp_csv_201718 = opp_exp_csv_201718.drop(25, axis=1)

    opp_exp_csv_201718.columns = list(opexp_header.columns)

    opexp_url_201920 = "https://www.fec.gov/files/bulk-downloads/2020/oppexp20.zip"

    opexp_resp_201920 = urlopen(opexp_url_201920)
    opexp_zipfile_201920 = ZipFile(BytesIO(opexp_resp_201920.read()))
    opexp_file_201920 = opexp_zipfile_201920.namelist()[0]

    original_stdout = sys.stdout

    with open(opexp_file_201920, "w+") as f_201920:
        sys.stdout = f_201920
        for line in opexp_zipfile_201920.open(opexp_file_201920).readlines():
            print(line.decode('utf-8'))

    sys.stdout = original_stdout

    opp_exp_csv_201920 = pd.read_csv('./oppexp.txt', delimiter='|', header = None)

    opp_exp_csv_201920 = opp_exp_csv_201920.drop(25, axis=1)

    opp_exp_csv_201920.columns = list(opexp_header.columns)

    opp_exp = opp_exp_csv_201112.append(opp_exp_csv_201314).append(opp_exp_csv_201516).append(opp_exp_csv_201718).append(opp_exp_csv_201920)

    opp_exp.drop(columns=[col for col in opp_exp if col not in ['CMTE_ID', 'NAME', 'TRANSACTION_DT', 'TRANSACTION_AMT', 'PURPOSE']], inplace=True)

    """# Merge Candidate Master and Candidate-Committee Linkage Information. Then only select data from current officials."""

    # join the candidate-committee linkage and candidate master files on candidate ID

    cm_ccl = candidate_master.merge(ccl, left_on="CAND_ID", right_on="CAND_ID")

    cm_ccl_reduced = cm_ccl[cm_ccl['FEC_ELECTION_YR'].isin([2016, 2017, 2018, 2019, 2020])]

    # drop all columns besides the ones relevant to us

    cm_ccl_reduced = cm_ccl.drop(columns=[col for col in cm_ccl if col not in ['CAND_ID', 'CAND_NAME', 'CAND_OFFICE_ST', 'CAND_PCC', 'CMTE_ID', 'CAND_OFFICE', 'FEC_ELECTION_YR']])

    cm_ccl_reduced.dropna(inplace=True)

    cm_ccl_reduced = cm_ccl_reduced[cm_ccl_reduced['CAND_OFFICE'].isin(['H', 'S'])]

    cm_ccl_reduced.sort_values(by='FEC_ELECTION_YR', ascending=False, inplace=True)

    cm_ccl_reduced.drop_duplicates(subset=['CAND_ID', 'CAND_OFFICE'], keep='first', inplace=True)

    house_reps_to_keep = cm_ccl_reduced[(cm_ccl['CAND_OFFICE'] == 'H') & (cm_ccl['FEC_ELECTION_YR'] == 2020)]
    cm_ccl_reduced = cm_ccl_reduced[cm_ccl_reduced['CAND_OFFICE'] == 'S'].append(house_reps_to_keep)

    federal_officials = pd.read_csv('../References/FEDERAL_OFFICIALS.csv')

    federal_officials_names = list(federal_officials['NAME'])
    federal_officials_names = [official.replace('ñ', 'N').replace('á', 'A').replace('é', 'E').replace('í', 'I') for official in federal_officials_names]
    federal_officials_last_names = list(set([official.split()[-1] for official in federal_officials_names]))

    """Roughly reduce size of cm_ccl and operating expenditures by using last names of current candidates..."""

    fec_candidates = list(set(cm_ccl_reduced['CAND_NAME']))

    names_to_keep = []

    for candidate in fec_candidates: 
    candidate_last_name = candidate.split()[0].replace(',', '')

    federal_officials_last_names_subset = [x for x in federal_officials_last_names if x[0] == candidate_last_name[0]]

    for last_name in federal_officials_last_names_subset:
        if last_name.strip() == candidate_last_name.strip():
        names_to_keep.append(candidate)
        break

    cm_ccl_reduced = cm_ccl_reduced[cm_ccl_reduced['CAND_NAME'].isin(names_to_keep)]

    cmte_ids_to_keep = list(set(cm_ccl_reduced['CMTE_ID'].values))
    cand_pccs_to_keep = list(set(cm_ccl_reduced['CAND_PCC'].values))

    opp_exp = opp_exp[(opp_exp['CMTE_ID'].isin(cmte_ids_to_keep) | (opp_exp['CMTE_ID'].isin(cand_pccs_to_keep)))]


    def get_cand_name(row):

    committee_id = row['CMTE_ID']

    cand_name = cm_ccl_reduced[(cm_ccl_reduced['CAND_PCC'] == committee_id) | (cm_ccl_reduced['CMTE_ID'] == committee_id)]['CAND_NAME']

    if cand_name.any():
        return cand_name.iloc[0]
    
    return '0'

    # applying the get_cand_name() function to the `CAND_NAME` column of the operating expenditures file to match all committees with candidates, if possible
    opp_exp['CAND_NAME'] = opp_exp.apply(get_cand_name, axis=1)

    opp_exp.drop(columns=[col for col in opp_exp.columns if col not in ['CMTE_ID', 'NAME', 'TRANSACTION_DT', 'TRANSACTION_AMT', 'PURPOSE', 'CAND_NAME']], inplace=True)

    opp_exp.reset_index(drop=True, inplace=True)

    opp_exp['TRANSACTION_DT'] = opp_exp['TRANSACTION_DT'].apply(lambda x:str(x).replace('/', ''))

    opp_exp.rename(columns={'NAME':'PAYEE', 'TRANSACTION_DT':'DATE', 'TRANSACTION_AMT':'AMOUNT', 'PURPOSE':'CATEGORY', 'CAND_NAME':'NAME'}, inplace=True)

    opp_exp.to_csv('../Processed_Data/PROCESSED.csv', index=None)