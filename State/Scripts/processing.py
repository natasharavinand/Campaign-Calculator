import pandas as pd
import numpy as np
import os
import re
import math

# Goal is to get data from each state from raw files to a single file with standardized columns (CANDIDATE/COMMITTEE NAME, PAYEE, AMOUNT, DATE, CATEGORY)

"""## Functions"""

# if payee is provided in the form of a name (ex. First Name Last Name), combine them
def fill_payee_with_name(row, firstName, lastName):
  first, last = row[firstName], row[lastName]
  payee = ''
  if type(first) == str:
    payee += first
  if type(last) == str and type(first) == str:
    payee += ' ' + last 
  elif type(last) == str and type(first) != str:
    payee += last 
  
  if not payee and type(row['PAYEE']) != str:
    return 'NOT FOUND'
  elif not payee and type(row['PAYEE']) == str:
    return row['PAYEE']

  return payee

# if company name is provided but payee name is blank, fill with company name
def fill_not_found_with_company(row, companyName, payeeName):
  company, payee = row[companyName], row[payeeName]
  if type(companyName) != float and payee == 'NOT FOUND':
    return company
  else:
    return payee

# convert dates to string format (ex. '07182001')
def format_date(date_text, flip_date=False):
  try:
    if type(date_text) == float or type(date_text) == int:
      date_text = str(int(date_text))
    if date_text.replace(' ', '') == '':
      return 'NOT FOUND'
    new_date_text = date_text.replace('/', '').replace('00:00:00', '').replace('-', '').replace('12:00:00', '').replace('AM', '').replace('PM', '')
    new_date_text = re.sub('(?:[01]\d|2[0-3]):(?:[0-5]\d):(?:[0-5]\d)', '', new_date_text).strip()
    if not flip_date:
      return new_date_text.strip()
    else:
      return (new_date_text[4:] + new_date_text[:4]).replace(' ', '').strip()
  except:
    return 'NOT FOUND'

# if purpose and category are both provided, combine into one entity
def combine_purpose_category(row, purposeName, categoryName):
  purpose, category = row[purposeName], row[categoryName]
  if type(purpose) != str and type(category) != str:
    return 'NOT FOUND'
  if type(purpose) != str:
    return category.upper() 
  if type(category) != str:
    return purpose.upper()
  return (purpose + ' ' + category).upper()

# if candidate name and committee name are both provided, combine into one entity
def combine_candidate_committee(row, candidateName, committeeName):
  candidate, committee = row[candidateName], row[committeeName]
  if type(candidate) != str and type(committee) != str:
    return 'NOT FOUND'
  if type(candidate) != str:
    return committee.upper() 
  if type(committee) != str:
    return candidate.upper()
  return (candidate + '-' + committee).upper()

# if payee and payee name are both provided, combine into one entity
def combine_payee_name(row, payee, name):
  payee = row[payee]
  name = row[name]
  if type(payee) != str and type(name) != str:
    return 'NOT FOUND'

  if type(payee) != str:
    return name.upper()
  else:
    return payee.upper()

# convert string dollar formats to floats
def convert_dollar_to_float(dollar):
  if type(dollar) == float:
    if dollar >= 0:
      return dollar
    else:
      return dollar * -1
  try:
    dollar = dollar.replace('$', '').replace(',', '').replace('-', '')
    return float(dollar)
  except:
    return 0.0

# run most cleaning functions in this helper function
def clean_cols(dataframe, additional_cols=[], flip_date=False):
  if not flip_date:
    dataframe['DATE'] = dataframe['DATE'].apply(format_date)
  else:
    dataframe['DATE'] = dataframe['DATE'].apply(format_date, flip_date=True)
  dataframe['AMOUNT'] = dataframe['AMOUNT'].apply(convert_dollar_to_float)
  dataframe['PAYEE'] = dataframe['PAYEE'].apply(lambda x: x.upper() if x and type(x) == str else 'NOT FOUND')
  dataframe['CANDIDATE/COMMITTEE NAME'] = dataframe['CANDIDATE/COMMITTEE NAME'].apply(lambda x: x.upper() if x and type(x) == str else 'NOT FOUND')
  dataframe = dataframe[~dataframe['CANDIDATE/COMMITTEE NAME'].str.contains('\\bPAC\\b|POLITICAL ACTION COMMITTEE|CORPORATION')]

  if len(additional_cols) == 0:
    return dataframe.reset_index(drop=True)

  for col in additional_cols:
    dataframe[col] = dataframe[col].apply(lambda x: x.upper() if x and type(x) == str else 'NOT FOUND')

  return dataframe.reset_index(drop=True)

# Florida specific - get office from committee name
def get_fl_office(cc_name):
  if type(cc_name) == float:
    return 'NOT FOUND'
  office = cc_name[-4:-1]
  return office

# Remove party from committee name
def remove_party(cc_name):
  try:
    return re.sub(r'\([^)]*\)', '', cc_name).strip()
  except:
    return 'NOT FOUND'

if __name__ == '__main__':

  """## Alabama"""

  alabama_path = '../Raw_Data/ALABAMA/'
  al_cols_to_keep = ['OrgID', 'ExpenditureAmount', 'ExpenditureDate', 'LastName', 'FirstName', 'Explanation', 'CommitteeType', 'CandidateName']
  merged_al = pd.DataFrame(data=None, columns=al_cols_to_keep)

  for file in os.listdir(alabama_path):
    file_path = alabama_path + file
    curr = pd.read_csv(file_path, encoding= 'latin1', error_bad_lines=False, warn_bad_lines=False, usecols=al_cols_to_keep)
    merged_al = merged_al.append(curr)

  merged_al.rename(columns={'OrgID':'ORD_ID', 'ExpenditureAmount': 'AMOUNT', 'ExpenditureDate': 'DATE', 'CommitteeType':'COMMITTEE TYPE', 'Explanation':'CATEGORY', 'CandidateName':'CANDIDATE/COMMITTEE NAME'}, inplace=True)

  merged_al.loc[:,'PAYEE'] = ''

  merged_al['PAYEE'] = merged_al.apply(fill_payee_with_name, firstName='FirstName', lastName='LastName', axis=1)

  merged_al.drop(columns=['LastName', 'FirstName', 'COMMITTEE TYPE'], inplace=True) 

  merged_al = clean_cols(merged_al, additional_cols=['CATEGORY'])

  merged_al.to_csv('../Processed_Data/ALABAMA_PROCESSED.csv', index=None)

  """## Colorado"""

  colorado_path = '../Raw_Data/COLORADO/'

  co_cols_to_keep = ['CO_ID', 'ExpenditureAmount', 'ExpenditureDate', 'LastName', 'FirstName', 'Explanation', 'ExpenditureType', \
                    'CommitteeType', 'CommitteeName', 'CandidateName', 'Jurisdiction']

  merged_co = pd.DataFrame(data=None, columns=co_cols_to_keep)

  for file in os.listdir(colorado_path):
    file_path = colorado_path + file
    curr = pd.read_csv(file_path, encoding= 'latin1', error_bad_lines=False, warn_bad_lines=False, usecols=co_cols_to_keep)
    merged_co = merged_co.append(curr)

  merged_co.rename(columns={'ExpenditureAmount': 'AMOUNT', 'ExpenditureDate': 'DATE', \
                            'CommitteeType':'COMMITTEE TYPE', 'CandidateName':'CANDIDATE NAME', 'Explanation':'PURPOSE', \
                            'ExpenditureType':'CATEGORY', 'Jurisdiction':'JURISDICTION', 'CommitteeName':'COMMITTEE NAME'}, inplace=True)

  merged_co.loc[:,'PAYEE'] = ''

  merged_co['PAYEE'] = merged_co.apply(fill_payee_with_name, firstName='FirstName', lastName='LastName', axis=1)

  merged_co['CATEGORY'] = merged_co.apply(combine_purpose_category, purposeName='PURPOSE', categoryName='CATEGORY', axis=1)

  merged_co.drop(columns=['LastName', 'FirstName', 'COMMITTEE TYPE', 'PURPOSE'], inplace=True) 

  merged_co['CANDIDATE/COMMITTEE NAME'] = merged_co.apply(combine_candidate_committee, candidateName='CANDIDATE NAME', committeeName='COMMITTEE NAME', axis=1)

  merged_co.drop(columns=['COMMITTEE NAME', 'CANDIDATE NAME'], inplace=True) 

  merged_co = clean_cols(merged_co, additional_cols = ['CATEGORY', 'JURISDICTION'], flip_date=True)

  merged_co.to_csv('../Processed_Data/COLORADO_PROCESSED.csv', index=None)

  """## Delaware"""

  delaware_path = '../Raw_Data/DELAWARE/'

  de_cols_to_keep = ['Expenditure Date', 'Payee Name', 'Amount($)', 'Committee Name', 'Expense Category', 'Expense Purpose', 'CF ID']

  merged_de = pd.DataFrame(data=None, columns=de_cols_to_keep)

  for file in os.listdir(delaware_path):
    file_path = delaware_path + file
    curr = pd.read_csv(file_path, encoding='latin1', error_bad_lines=False, warn_bad_lines=False, usecols=de_cols_to_keep)
    merged_de = merged_de.append(curr)

  merged_de.rename(columns={'Expenditure Date':'DATE', 'Payee Name':'PAYEE', 'Amount($)':'AMOUNT', 'Committee Name':'CANDIDATE/COMMITTEE NAME', 'Expense Category':'CATEGORY', 'Expense Purpose':'PURPOSE', 'CF ID':'CF_ID'}, inplace=True)

  merged_de['CATEGORY'] = merged_de.apply(combine_purpose_category, purposeName='PURPOSE', categoryName='CATEGORY', axis=1)

  merged_de.drop(columns=['PURPOSE'], inplace=True) 

  merged_de = clean_cols(merged_de, additional_cols=['CATEGORY'])

  merged_de.to_csv('../Processed_Data/DELAWARE_PROCESSED.csv', index=None)

  """## Hawaii"""

  hawaii_file = pd.read_csv('../Raw_Data/HAWAII/HI.csv')

  hawaii_file.rename(columns={'Candidate Name':'CANDIDATE/COMMITTEE NAME', 'Vendor Name':'PAYEE', 'Date':'DATE', 'Amount':'AMOUNT', 'Expenditure Category':'CATEGORY', 'Purpose of Expenditure':'PURPOSE', 'Office':'OFFICE', 'Reg No':'REG_NO_ID'}, inplace=True)

  hawaii_file['CATEGORY'] = hawaii_file.apply(combine_purpose_category, purposeName='PURPOSE', categoryName='CATEGORY', axis=1)

  hawaii_file.drop(columns=['Vendor Type', 'Authorized Use', 'Address 1', 'Address 2', 'City', 'State', 'Zip Code', 'District', 'County', 'Party', 'Election Period', 'Mapping Location', 'InOutState', 'PURPOSE'], inplace=True) 

  hawaii_file = clean_cols(hawaii_file, additional_cols=['CATEGORY', 'OFFICE'])

  hawaii_file.to_csv('../Processed_Data/HAWAII_PROCESSED.csv', index=None)

  """## Maine"""

  maine_file = pd.read_csv('../Raw_Data/MAINE/ME.csv')

  me_cols_to_keep = ['Election Year', 'Committee Name', 'CanCommOffice', 'CanCommJurisdiction', 'Date', \
                    'Payee', 'Expenditure Purpose', 'Expenditure Description', 'Amount']

  maine_file.drop(columns=[col for col in maine_file if col not in me_cols_to_keep], inplace=True)

  maine_file.rename(columns={'Election Year':'ELECTION YEAR', 'Committee Name':'CANDIDATE/COMMITTEE NAME', 'CanCommOffice':'OFFICE', 'CanCommJurisdiction':'JURISDICTION', 'Date':'DATE', 'Payee':'PAYEE', 'Expenditure Purpose':'PURPOSE', 'Expenditure Description':'CATEGORY','Amount':'AMOUNT'}, inplace=True)

  maine_file['CATEGORY'] = maine_file.apply(combine_purpose_category, purposeName='PURPOSE', categoryName='CATEGORY', axis=1)

  maine_file.drop(columns=['PURPOSE'], inplace=True)

  maine_file['ELECTION YEAR'] = maine_file['ELECTION YEAR'].apply(lambda x: str(x))

  maine_file = clean_cols(maine_file, additional_cols=['ELECTION YEAR', 'OFFICE', 'JURISDICTION', 'CATEGORY'])

  maine_file.to_csv('../Processed_Data/MAINE_PROCESSED.csv', index=None)

  """## Maryland"""

  maryland_file = pd.read_csv('../Raw_Data/MARYLAND/MD.csv', error_bad_lines=False, warn_bad_lines=False, engine='python')

  md_cols_to_keep = ['Expenditure Date', 'Payee Name', 'Amount($)', 'Committee Name', 'Expense Category', 'Expense Purpose', 'Amount($)']

  maryland_file.rename(columns={'Expenditure Date':'DATE', 'Payee Name':'PAYEE', 'Amount($)':'AMOUNT', 'Committee Name':'CANDIDATE/COMMITTEE NAME', 'Expense Category':'CATEGORY', 'Expense Purpose':'PURPOSE'}, inplace=True)

  maryland_file['CATEGORY'] = maryland_file.apply(combine_purpose_category, purposeName='PURPOSE', categoryName='CATEGORY', axis=1)

  maryland_file.drop(columns=['PURPOSE', 'Address', 'Payee Type', 'Expense Toward', 'Expense Method', 'Vendor', 'Fundtype', 'Comments', 'Unnamed: 13'], inplace=True)

  maryland_file = clean_cols(maryland_file, additional_cols=['CATEGORY'])

  maryland_file.to_csv('../Processed_Data/MARYLAND_PROCESSED.csv', index=None)

  """## New Hampshire"""

  nh_file = pd.read_csv('../Raw_Data/NEW HAMPSHIRE/NH.csv', index_col=None)

  nh_file['DATE'] = nh_file.index

  nh_file['PAYEE'] = nh_file['Payee Type']

  nh_file['CANDIDATE/COMMITTEE NAME'] = nh_file['Payee Address']

  nh_file['COMMITTEE TYPE'] = nh_file['Registrant Name']

  nh_file['OFFICE'] = nh_file['Registrant Type']

  nh_file['ELECTION CYCLE'] = nh_file['County']

  nh_file['AMOUNT'] = nh_file['Expenditure Purpose']

  nh_file['CATEGORY'] = nh_file['Expenditure Type']

  nh_file['PURPOSE'] = nh_file['Expenditure Amount']

  nh_file['CF_ID'] = nh_file['Transaction Date']

  nh_file = nh_file[nh_file['COMMITTEE TYPE'].str.contains('Candidate', na=False)]

  nh_file.drop(columns=[col for col in nh_file.columns if col not in ['DATE','PAYEE', 'CANDIDATE/COMMITTEE NAME', 'COMMITTEE TYPE', 'OFFICE', 'ELECTION CYCLE', 'AMOUNT', 'CATEGORY', 'CF_ID']], inplace=True)

  nh_file = nh_file.reset_index(drop=True)

  nh_file.drop(columns=['COMMITTEE TYPE'], inplace=True)

  nh_file['CF_ID'] = nh_file['CF_ID'].apply(lambda x: str(int(x) if not np.isnan(x) else '0'))

  nh_file = clean_cols(nh_file, additional_cols=['ELECTION CYCLE', 'OFFICE', 'CATEGORY'])

  nh_file.to_csv('../Processed_Data/NEW HAMPSHIRE_PROCESSED.csv', index=None)

  """## Ohio"""

  oh_file = pd.read_csv('../Raw_Data/OHIO/OH.csv', encoding='latin1')

  oh_file.drop(columns=['Address', 'City', 'State', 'Zip', 'Year', 'Report Type', 'Event Date'], inplace=True)

  oh_file.rename(columns={'Payee':'PAYEE', 'Non Individual':'NAME', 'Expend Date':'DATE', 'Amount':'AMOUNT', 'Purpose':'CATEGORY', 'Committee':'CANDIDATE/COMMITTEE NAME'}, inplace=True)

  oh_file['PAYEE'] = oh_file.apply(combine_payee_name, payee='PAYEE', name='NAME', axis=1)

  oh_file.drop(columns=['NAME'],inplace=True)

  oh_file = clean_cols(oh_file, additional_cols=['CATEGORY'])

  oh_file.to_csv('../Processed_Data/OHIO_PROCESSED.csv', index=None)


  """## Florida"""

  florida_path = '../Raw_Data/FLORIDA/'

  fl_cols_to_keep = ['Candidate/Committee', 'Date', 'Amount', 'Payee Name', 'Purpose']

  merged_fl = pd.DataFrame(data=None, columns=fl_cols_to_keep)

  for file in os.listdir(florida_path):
    if file[0] != '.':
      file_path = florida_path + file
      curr = pd.read_csv(file_path, encoding= 'latin1', error_bad_lines=False, warn_bad_lines=False, sep='\t', usecols=fl_cols_to_keep)
      merged_fl = merged_fl.append(curr)

  merged_fl.rename(columns={'Candidate/Committee':'CANDIDATE/COMMITTEE NAME', 'Date':'DATE', 'Amount':'AMOUNT', 'Payee Name':'PAYEE', 'Purpose':'CATEGORY'}, inplace=True)

  merged_fl['OFFICE'] = merged_fl['CANDIDATE/COMMITTEE NAME'].apply(get_fl_office)

  merged_fl['CANDIDATE/COMMITTEE NAME'] = merged_fl['CANDIDATE/COMMITTEE NAME'].apply(remove_party)

  merged_fl = clean_cols(merged_fl, additional_cols=['CATEGORY', 'OFFICE'])

  merged_fl.to_csv('../Processed_Data/FLORIDA_PROCESSED.csv', index=None)

  """## Tennessee"""

  tn_path = '../Raw_Data/TENNESSEE/'

  tn_cols_to_keep = ['Amount', 'Date', 'Election Year', 'Candidate/PAC Name', 'Vendor Name', 'Purpose']

  merged_tn = pd.DataFrame(data=None, columns=tn_cols_to_keep)

  for file in os.listdir(tn_path):
    file_path = tn_path + file
    curr = pd.read_csv(file_path, encoding='latin1', error_bad_lines=False, warn_bad_lines=False, usecols=tn_cols_to_keep)
    merged_tn = merged_tn.append(curr)

  merged_tn.rename(columns={'Amount':'AMOUNT', 'Date':'DATE', 'Election Year':'ELECTION YEAR', 'Candidate/PAC Name':'CANDIDATE/COMMITTEE NAME', 'Vendor Name':'PAYEE', 'Purpose':'CATEGORY'}, inplace=True)

  merged_tn = clean_cols(merged_tn, additional_cols=['CATEGORY'])

  merged_tn['ELECTION YEAR'] = merged_tn['ELECTION YEAR'].apply(lambda x: str(x) if x else 'NOT FOUND')

  merged_tn['ELECTION YEAR'] = merged_tn['ELECTION YEAR'].apply(lambda x: x if x.replace(' ', '') else 'NOT FOUND')

  merged_tn.to_csv('../Processed_Data/TENNESSEE_PROCESSED.csv', index=None)

  """## Vermont"""

  vt_file = pd.read_csv('../Raw_Data/VERMONT/VT.csv')

  new_vt_file = pd.DataFrame(data = None, columns=['DATE', 'PAYEE', 'CANDIDATE/COMMITTEE NAME', 'OFFICE', 'ELECTION CYCLE', 'CATEGORY', 'AMOUNT'])

  vt_file['Date'] = vt_file.index

  vt_file = vt_file.reset_index(drop=True)

  new_vt_file['DATE'] = vt_file['Date']

  new_vt_file['PAYEE'] = vt_file['Payee Type']

  new_vt_file['CANDIDATE/COMMITTEE NAME'] = vt_file['Payee Address']

  new_vt_file['OFFICE'] = vt_file['Registrant Type']

  new_vt_file['ELECTION CYCLE'] = vt_file['Office']

  new_vt_file['CATEGORY'] = vt_file['Expenditure Type']

  new_vt_file['AMOUNT'] = vt_file['Expenditure Purpose']

  new_vt_file = clean_cols(new_vt_file, additional_cols=['OFFICE', 'ELECTION CYCLE', 'CATEGORY'])

  new_vt_file.to_csv('../Processed_Data/VERMONT_PROCESSED.csv', index=None)

  """## Wisconsin"""

  wi_file = pd.read_csv('../Raw_Data/WISCONSIN/WI.csv')

  wi_cols_to_keep = ['RegistrantName', 'ETHCFID', 'PayeeName', 'TransactionDate', 'ExpensePurpose', 'ExpenseCategory', 'Amount']

  wi_file.drop(columns=[col for col in wi_file if col not in wi_cols_to_keep], inplace=True)

  wi_file.rename(columns={'RegistrantName':'CANDIDATE/COMMITTEE NAME', 'PayeeName':'PAYEE', 'TransactionDate':'DATE', 'ExpensePurpose':'PURPOSE', 'ExpenseCategory':'CATEGORY', 'Amount':'AMOUNT'}, inplace=True)

  wi_file['CATEGORY'] = wi_file.apply(combine_purpose_category, categoryName='CATEGORY', purposeName='PURPOSE', axis=1)

  wi_file.drop(columns=['PURPOSE'], inplace=True)

  wi_file = clean_cols(wi_file, additional_cols=['CATEGORY'])

  wi_file['ETHCFID'] = wi_file['ETHCFID'].apply(lambda x: str(x))

  wi_file.to_csv('../Processed_Data/WISCONSIN_PROCESSED.csv', index=None)

  """## Wyoming"""

  wy_path = '../Raw_Data/WYOMING/'

  wy_cols_to_keep = ['Filer Name', 'Payee', 'Purpose',  'Date', 'Amount']

  merged_wy = pd.DataFrame(data=None, columns=wy_cols_to_keep)

  for file in os.listdir(wy_path):
    file_path = wy_path + file
    curr = pd.read_csv(file_path, usecols=wy_cols_to_keep)
    merged_wy = merged_wy.append(curr)

  merged_wy.rename(columns={'Filer Name':'CANDIDATE/COMMITTEE NAME', 'Payee':'PAYEE', 'Purpose':'CATEGORY', 'Date':'DATE', 'Amount':'AMOUNT'}, inplace=True)

  merged_wy = clean_cols(merged_wy, additional_cols=['CATEGORY'])

  merged_wy.to_csv('../Processed_Data/WYOMING_PROCESSED.csv', index=None)

  """## Texas"""

  tx_path = '../Raw_Data/TEXAS/'

  tx_cols_to_keep = ['expendDt', 'filerName', 'filerIdent','expendAmount',  'expendDescr', 'payeeNameOrganization', 'payeeNameLast', 'payeeNameFirst']

  merged_tx = pd.DataFrame(data=None, columns=tx_cols_to_keep)

  for file in os.listdir(tx_path):
    if file[0] != '.':
      file_path = tx_path + file
      curr = pd.read_csv(file_path, usecols=tx_cols_to_keep, engine='python')
      merged_tx = merged_tx.append(curr)

  merged_tx.rename(columns={'expendDt':'DATE', 'filerName':'CANDIDATE/COMMITTEE NAME', 'filerIdent':'FILER_ID','expendAmount':'AMOUNT', 'expendDescr':'CATEGORY', 'payeeNameOrganization':'PAYEE'}, inplace=True)

  merged_tx['PAYEE'] = merged_tx.apply(fill_payee_with_name, firstName='payeeNameFirst', lastName='payeeNameLast', axis=1)

  merged_tx.drop(columns=['payeeNameLast', 'payeeNameFirst'], inplace=True)

  merged_tx = clean_cols(merged_tx, flip_date=True, additional_cols=['CATEGORY'])

  merged_tx['FILER_ID'] = merged_tx['FILER_ID'].apply(lambda x: str(x))

  merged_tx.to_csv('../Processed_Data/TEXAS_PROCESSED.csv', index=None)

  """## Alaska"""

  alaska_file = pd.read_csv('../Raw_Data/ALASKA/AK.CSV')

  new_alaska_file = pd.DataFrame(data=None, columns=['DATE', 'AMOUNT', 'LAST/BUSINESS NAME', 'FIRST NAME', 'CATEGORY', 'OFFICE', 'CANDIDATE/COMMITTEE NAME'])

  new_alaska_file['DATE'] = alaska_file['Result']
  new_alaska_file['AMOUNT'] = alaska_file['Payment Detail']
  new_alaska_file['LAST/BUSINESS NAME'] = alaska_file['Amount']
  new_alaska_file['FIRST NAME'] = alaska_file['Last/Business Name']
  new_alaska_file['CATEGORY'] = alaska_file['Employer']
  new_alaska_file['OFFICE'] = alaska_file['Municipality']
  new_alaska_file['CANDIDATE/COMMITTEE NAME'] = alaska_file['Filer Type']

  new_alaska_file['PAYEE'] = ''
  new_alaska_file['PAYEE'] = new_alaska_file.apply(fill_payee_with_name, firstName='FIRST NAME', lastName='LAST/BUSINESS NAME', axis=1)

  new_alaska_file.drop(columns=['LAST/BUSINESS NAME', 'FIRST NAME'], inplace=True)

  new_alaska_file = clean_cols(new_alaska_file, additional_cols=['CATEGORY', 'OFFICE'])

  new_alaska_file.to_csv('../Processed_Data/ALASKA_PROCESSED.csv', index=None)

  """## Connecticut"""

  ct_path = '../Raw_Data/CONNECTICUT/'

  ct_cols_to_keep = ['Committee',
  'Office Sought',
  'Committee Type',
  'Election Date',
  'Payee',
  'Purpose of Expenditure',
  'Description',
  'Amount',
  'Payment Date']

  merged_ct = pd.DataFrame(data=None, columns=ct_cols_to_keep)

  for file in os.listdir(ct_path):
    file_path = ct_path + file
    curr = pd.read_csv(file_path)
    merged_ct = merged_ct.append(curr)

  merged_ct.rename(columns={'Committee':'CANDIDATE/COMMITTEE NAME', 'Office Sought':'OFFICE', 'Election Date':'ELECTION DATE', 'Payee':'PAYEE', 'Purpose of Expenditure':'PURPOSE', 'Description':'CATEGORY', 'Amount':'AMOUNT', 'Payment Date':'DATE'}, inplace=True)

  merged_ct['ELECTION DATE'] = merged_ct['ELECTION DATE'].apply(lambda x: x[:-2] + '20' + x[-2:] if type(x) == str else 'NOT FOUND')

  merged_ct['DATE'] = merged_ct['DATE'].apply(lambda x: x[:-2] + '20' + x[-2:] if type(x) == str else 'NOT FOUND')

  merged_ct['CATEGORY'] = merged_ct.apply(combine_purpose_category, purposeName='PURPOSE', categoryName='CATEGORY',axis=1)

  merged_ct = clean_cols(merged_ct, ['OFFICE', 'CATEGORY'])

  merged_ct['ELECTION DATE'] = merged_ct['ELECTION DATE'].apply(format_date)

  merged_ct.drop(columns=['Committee Type', 'PURPOSE'], inplace=True)

  merged_ct.to_csv('../Processed_Data/CONNECTICUT_PROCESSED.csv', index=None)

  """## Idaho"""

  idaho_file = pd.read_csv('../Raw_Data/IDAHO/ID.csv')

  idaho_file.rename(columns={'Name':'CANDIDATE/COMMITTEE NAME', 'Expenditure Information':'DATE', 'Unnamed: 3':'AMOUNT', 'Unnamed: 5':'PAYEE'}, inplace=True)

  idaho_file.drop(columns=[col for col in idaho_file.columns if col not in ['CANDIDATE/COMMITTEE NAME', 'DATE', 'AMOUNT', 'PAYEE']], inplace=True)

  idaho_file = idaho_file[1:]

  idaho_file = clean_cols(idaho_file)

  idaho_file['CATEGORY'] = 'NOT FOUND'

  idaho_file.to_csv('../Processed_Data/IDAHO_PROCESSED.csv', index=None)

  """## Louisiana"""

  la_path = '../Raw_Data/LOUISIANA/'

  la_cols_to_keep = ['FilerLastName', 'FilerFirstName', 'RecipientName', 'ExpenditureDescription', 'ExpenditureDate', 'ExpenditureAmt']

  merged_la = pd.DataFrame(data=None, columns=la_cols_to_keep)

  for file in os.listdir(la_path):
    if file[0] != '.':
      file_path = la_path + file
      curr = pd.read_csv(file_path, usecols=la_cols_to_keep)
      merged_la = merged_la.append(curr)

  merged_la['CANDIDATE/COMMITTEE NAME'] = ''
  merged_la['CANDIDATE/COMMITTEE NAME'] = merged_la.apply(fill_payee_with_name, firstName='FilerFirstName', lastName='FilerLastName', axis=1)

  merged_la.drop(columns=['FilerLastName', 'FilerFirstName'], inplace=True)

  merged_la.rename(columns={'RecipientName':'PAYEE', 'ExpenditureDescription':'CATEGORY', 'ExpenditureDate':'DATE', 'ExpenditureAmt':'AMOUNT'}, inplace=True)

  merged_la = clean_cols(merged_la, additional_cols=['CATEGORY'])

  merged_la.to_csv('../Processed_Data/LOUISIANA_PROCESSED.csv', index=None)

  """## Nebraska"""

  ne_file = pd.read_csv('../Raw_Data/NEBRASKA/NE.txt', sep='|')

  ne_file.drop(columns=['Date Received', 'Payee Address', 'In-Kind'], inplace=True)

  ne_file.rename(columns={'Committee Name':'CANDIDATE/COMMITTEE NAME', 'Payee Name':'PAYEE', 'Expenditure Purpose':'CATEGORY', 'Expenditure Date':'DATE', 'Amount':'AMOUNT', 'Committee ID':'COMMITTEE ID'}, inplace=True)

  ne_file['CATEGORY'] = ne_file['CATEGORY'].apply(lambda x: x.strip() if type(x) == str else 'NOT FOUND')

  ne_file = clean_cols(ne_file, additional_cols=['CATEGORY'])

  ne_file.to_csv('../Processed_Data/NEBRASKA_PROCESSED.csv', index=None)

  """## Indiana"""

  indiana_path = '../Raw_Data/INDIANA/'

  in_cols_to_keep = ['CommitteeType', 'Committee', 'CandidateName', 'ExpenditureCode', 'Name', 'OfficeSought', 'Purpose', 'Amount', 'Expenditure_Date']

  merged_in = pd.DataFrame(data=None, columns=in_cols_to_keep)

  for file in os.listdir(indiana_path):
    if file[0] != '.':
      file_path = indiana_path + file
      curr = pd.read_csv(file_path, usecols=in_cols_to_keep, encoding='latin1')
      merged_in = merged_in.append(curr)

  merged_in.drop(columns=['CommitteeType'], inplace=True)

  merged_in.rename(columns={'Committee':'COMMITTEE NAME', 'CandidateName':'CANDIDATE NAME', 'ExpenditureCode':'CATEGORY', 'Name':'PAYEE', 'OfficeSought':'OFFICE', 'Purpose':'PURPOSE', 'Amount':'AMOUNT', 'Expenditure_Date':'DATE'}, inplace=True)

  merged_in['CATEGORY'] = merged_in.apply(combine_purpose_category, purposeName='PURPOSE', categoryName='CATEGORY', axis=1)

  merged_in.drop(columns=['PURPOSE'], inplace=True)

  merged_in['CANDIDATE/COMMITTEE NAME'] = merged_in.apply(combine_candidate_committee, candidateName='CANDIDATE NAME', committeeName = 'COMMITTEE NAME', axis=1)

  merged_in.drop(columns=['COMMITTEE NAME', 'CANDIDATE NAME'], inplace=True)

  merged_in = clean_cols(merged_in, flip_date=True, additional_cols=['OFFICE', 'CATEGORY'])

  merged_in.to_csv('../Processed_Data/INDIANA_PROCESSED.csv', index=None)

  """## Utah"""

  ut_path = '../Raw_Data/UTAH/'

  ut_cols_to_keep = ['PCC', 'TRAN_TYPE', 'TRAN_DATE', 'TRAN_AMT', 'NAME', 'PURPOSE']

  merged_ut = pd.DataFrame(data=None, columns=ut_cols_to_keep)

  for file in os.listdir(ut_path):
    if file[0] != '.':
      file_path = ut_path + file
      curr = pd.read_csv(file_path, usecols=ut_cols_to_keep, error_bad_lines=False)
      merged_ut = merged_ut.append(curr)

  merged_ut.rename(columns={'PCC':'CANDIDATE/COMMITTEE NAME', 'TRAN_DATE':'DATE', 'TRAN_AMT':'AMOUNT', 'NAME':'PAYEE', 'PURPOSE':'CATEGORY'}, inplace=True)

  merged_ut.drop(columns=['TRAN_TYPE'], inplace=True)

  merged_ut = clean_cols(merged_ut, additional_cols=['CATEGORY'])

  merged_ut.to_csv('../Processed_Data/UTAH_PROCESSED.csv', index=None)

  """## Washington """

  wa_file = pd.read_csv('../Raw_Data/WASHINGTON/WA.csv')

  wa_cols_to_keep = ['type', 'filer_name', 'office', 'jurisdiction', 'committee_id', 'election_year', 'amount', 'expenditure_date', 'description', 'recipient_name']

  wa_file.drop(columns=[col for col in wa_file.columns if col not in wa_cols_to_keep], inplace=True)

  wa_file.drop(columns=['type'], inplace=True)

  wa_file.rename(columns={'filer_name':'CANDIDATE/COMMITTEE NAME', 'office':'OFFICE', 'jurisdiction':'JURISDICTION', 'election_year':'ELECTION YEAR', 'amount':'AMOUNT', 'expenditure_date':'DATE', 'description':'CATEGORY', 'recipient_name':'PAYEE', 'committee_id':'COMMITTEE_ID'}, inplace=True)

  wa_file['ELECTION YEAR'] = wa_file['ELECTION YEAR'].apply(lambda x: str(x))

  wa_file = clean_cols(wa_file, additional_cols=['OFFICE', 'JURISDICTION', 'CATEGORY'])

  wa_file.to_csv('../Processed_Data/WASHINGTON_PROCESSED.csv', index=None)

  """## Iowa"""

  iowa_file = pd.read_csv('../Raw_Data/IOWA/IA.csv')

  iowa_file.drop(columns=['Transaction ID', 'Receiving Committee Code', 'Address - Line 1', 'Address - Line 2', 'City', 'State', 'Zip'], inplace=True)

  iowa_file.rename(columns={'Date':'DATE', 'Committee Name':'CANDIDATE/COMMITTEE NAME', 'Receiving Organization Name':'PAYEE', 'First Name':'FIRST NAME', 'Last Name':'LAST NAME', 'Expenditure Amount':'AMOUNT', 'Committee Code':'COMMITTEE_ID'}, inplace=True)

  iowa_file['PAYEE'] = iowa_file.apply(fill_payee_with_name, firstName='FIRST NAME', lastName='LAST NAME', axis=1)

  iowa_file.drop(columns=['FIRST NAME', 'LAST NAME'], inplace=True)

  iowa_file = clean_cols(iowa_file)

  iowa_file['CATEGORY'] = 'NOT FOUND'

  iowa_file.to_csv('../Processed_Data/IOWA_PROCESSED.csv', index=None)

  """## Kansas"""

  kansas_file = pd.read_html('../Raw_Data/KANSAS/KS.xls')

  kansas_file = kansas_file[0]

  kansas_file.rename(columns={"Candidate's Name":'CANDIDATE/COMMITTEE NAME', 'Recipient':'PAYEE', 'Date':'DATE', 'Expenditure Description':'CATEGORY', 'Amount':'AMOUNT'}, inplace=True)

  kansas_file.drop(columns=['Period Covered'], inplace=True)

  kansas_file['PAYEE'] = kansas_file['PAYEE'].str.extract('(.+?(?=[0-9]|P.?O.?|-))')

  kansas_file = clean_cols(kansas_file, additional_cols=['CATEGORY'])

  kansas_file.to_csv('../Processed_Data/KANSAS_PROCESSED.csv', index=None)


  """## Minnesota"""

  minn_file = pd.read_csv('../Raw_Data/MINNESOTA/MN.csv')

  minn_cols_to_use = ['Committee reg num', 'Committee name', 'Vendor name', 'Amount', 'Date', 'Purpose']

  minn_file.drop(columns=[col for col in minn_file.columns if col not in minn_cols_to_use], inplace=True)

  minn_file.rename(columns={'Committee name':'CANDIDATE/COMMITTEE NAME', 'Vendor name':'PAYEE', 'Amount':'AMOUNT', \
                            'Date':'DATE', 'Purpose':'CATEGORY', 'Committee reg num':'COM_REG_NUM_ID'}, inplace=True)

  minn_file = clean_cols(minn_file, additional_cols=['CATEGORY'])

  minn_file.to_csv('../Processed_Data/MINNESOTA_PROCESSED.csv', index=None)

  """## Mississippi"""

  ms_file = pd.read_csv('../Raw_Data/MISSISSIPPI/MS.csv')

  ms_file.drop(columns=['Unnamed: 0', 'Report Type'], inplace=True)

  ms_file.rename(columns={'Filer':'CANDIDATE/COMMITTEE NAME', 'Paid To':'PAYEE', 'Date Received':'DATE', \
                          'Amount ($)':'AMOUNT'}, inplace=True)

  ms_file['DATE'] = ms_file['DATE'].apply(lambda x: x[:-2] + '20' + x[-2:])

  ms_file = clean_cols(ms_file)

  ms_file['CATEGORY'] = 'NOT FOUND'

  ms_file.to_csv('../Processed_Data/MISSISSIPPI_PROCESSED.csv', index=None)

  """## Michigan"""

  mi_path = '../Raw_Data/MICHIGAN/'

  mi_cols_to_use = ['com_legal_name', 'com_type', 'exp_desc', 'purpose', \
                    'f_name', 'lname_or_org', 'exp_date', 'amount', 'vend_name', 'cfr_com_id']

  merged_mi = pd.DataFrame(data=None, columns=mi_cols_to_use)

  for file in os.listdir(mi_path):
    if file[0] != '.':
      file_path = mi_path + file
      curr = pd.read_csv(file_path, usecols=mi_cols_to_use, sep='\t', encoding='latin1')
      merged_mi = merged_mi.append(curr)

  merged_mi.rename(columns={'com_legal_name':'CANDIDATE/COMMITTEE NAME', 'exp_desc':'CATEGORY', \
                            'purpose':'PURPOSE', 'f_name':'FIRST NAME', 'lname_or_org':'LAST NAME', \
                            'exp_date':'DATE', 'amount':'AMOUNT', 'vend_name':'PAYEE', 'cfr_com_id':'COM_ID'}, inplace=True)

  merged_mi['CATEGORY'] = merged_mi.apply(combine_purpose_category, categoryName='CATEGORY', \
                                          purposeName='PURPOSE', axis=1)

  merged_mi['PAYEE'] = merged_mi.apply(fill_payee_with_name, firstName='FIRST NAME', \
                                      lastName='LAST NAME', axis=1)

  merged_mi.drop(columns=['com_type', 'PURPOSE', 'FIRST NAME', 'LAST NAME'], inplace=True)

  merged_mi = clean_cols(merged_mi, additional_cols=['CATEGORY'])

  merged_mi.to_csv('../Processed_Data/MICHIGAN_PROCESSED.csv', index=None)

  """## Illinois"""

  illinois_expn = pd.read_csv('../Raw_Data/ILLINOIS/IL.txt', encoding='latin1', sep='\t', \
                              error_bad_lines=False, warn_bad_lines=False)

  illinois_comms = pd.read_csv('../Raw_Data/ILLINOIS/IL_COMS.csv', \
                              error_bad_lines=False, warn_bad_lines=False)

  illinois_file = illinois_expn.merge(illinois_comms, left_on='CommitteeID', right_on='ID')

  illinois_file.drop(columns=[col for col in illinois_file.columns if col not in \
                              ['LastOnlyName', 'FirstName', 'ExpendedDate', 'Amount', 'Purpose', \
                              'Office', 'Name', 'TypeOfCommittee', 'CommitteeID']], inplace=True)

  illinois_file.rename(columns={'LastOnlyName':'LAST NAME', 'FirstName':'FIRST NAME', 'ExpendedDate':'DATE', \
                                'Amount':'AMOUNT', 'Purpose':'CATEGORY', 'Office':'OFFICE', \
                                'Name':'CANDIDATE/COMMITTEE NAME', 'CommitteeID':'COM_ID'}, inplace=True)

  illinois_file = illinois_file[illinois_file['DATE'].str.contains('2021|2020|2019|2018|2017|2016|2015|2014', na=False)]

  illinois_file = illinois_file[~illinois_file['TypeOfCommittee'].str.contains('Political Action|Political Party|Independent Expenditure|Ballot Initiative',na=False)]

  illinois_file['PAYEE'] = ''
  illinois_file['PAYEE'] = illinois_file.apply(fill_payee_with_name, firstName='FIRST NAME', lastName='LAST NAME', axis=1)

  illinois_file.drop(columns=['LAST NAME', 'FIRST NAME', 'TypeOfCommittee'], inplace=True)

  illinois_file = clean_cols(illinois_file, additional_cols=['CATEGORY','OFFICE'], flip_date=True)

  illinois_file.to_csv('../Processed_Data/ILLINOIS_PROCESSED.csv', index=None)

  """## New York"""

  ny_path = '../Raw_Data/NEW YORK/'

  ny_cols_to_keep = ['CANDIDATE/COMMITTEE NAME', 'COM_ID', 'ELECTION YEAR', 'DATE', 'CATEGORY',
        'PAYEE', 'AMOUNT']

  merged_ny = pd.DataFrame(data=None, columns=ny_cols_to_keep)

  for file in os.listdir(ny_path):
    if file[0] != '.':
      file_path = ny_path + file
      curr = pd.read_csv(file_path, header=None, encoding='latin1', error_bad_lines=False)
      curr.drop(columns=[col for col in curr.columns if col not in [1, 2, 3, 11, 15, 22, 24, 36]], inplace=True)
      curr = curr[curr[11].str.contains('Expenditure|Payments|Expense', na=False)]
      curr.rename(columns={1:'COM_ID', 2:'CANDIDATE/COMMITTEE NAME', 3:'ELECTION YEAR', 15:'DATE', 22:'CATEGORY', \
                      24:'PAYEE', 36:'AMOUNT'}, inplace=True)
      curr.drop(columns=[11], inplace=True)
      merged_ny = merged_ny.append(curr)

  merged_ny['ELECTION YEAR'] = merged_ny['ELECTION YEAR'].apply(lambda x: str(x))

  merged_ny = clean_cols(merged_ny, flip_date=True, additional_cols=['CATEGORY'])

  merged_ny.to_csv('../Processed_Data/NEW YORK_PROCESSED.csv', index=None)

  """## Georgia"""

  georgia_file = pd.read_html('../Raw_Data/GEORGIA/GA.csv')

  georgia_file = georgia_file[0]

  georgia_file.columns = list(georgia_file.iloc[0,:].values)

  georgia_file = georgia_file.iloc[1:,:]

  georgia_file.drop(columns=['Key', 'Ref', 'Type', 'Address', 'City', 'State', 'Zip', \
                            'Occupation_or_Employer', 'Other', 'Candidate_Suffix', 'Candidate_FirstName', 'Candidate_MiddleName', 'Candidate_LastName'], inplace=True)

  georgia_file['PAYEE'] = ''
  georgia_file['PAYEE'] = georgia_file.apply(fill_payee_with_name, firstName='FirstName', lastName='LastName', axis=1)

  georgia_file.drop(columns=['LastName', 'FirstName'], inplace=True)

  georgia_file.rename(columns={'Date':'DATE', 'Purpose':'CATEGORY', 'Paid':'AMOUNT', 'Election':'ELECTION TYPE', \
                              'Election_Year':'ELECTION YEAR', 'Committee_Name':'CANDIDATE/COMMITTEE NAME', 'FilerID':'FILER_ID'}, inplace=True)

  georgia_file['ELECTION YEAR'] = georgia_file['ELECTION YEAR'].apply(lambda x: str(x))

  georgia_file['ELECTION YEAR'] = georgia_file['ELECTION YEAR'].apply(lambda x: 'NOT FOUND' if x == 'nan' else x)

  georgia_file = clean_cols(georgia_file, additional_cols=['CATEGORY', 'ELECTION TYPE'])

  georgia_file.to_csv('../Processed_Data/GEORGIA_PROCESSED.csv', index=None)

  """## Virginia"""

  virginia_report_path = '../Raw_Data/VIRGINIA/REPORT/'
  virginia_schedule_d_path = '../Raw_Data/VIRGINIA/SCHEDULE D/'

  sd_cols_to_keep = ['ReportId', 'FirstName', 'LastOrCompanyName', 'TransactionDate', \
                                                            'Amount', 'ItemOrService']

  rep_cols_to_keep = ['ReportId', 'CommitteeName', 'CommitteeCode','CommitteeType', 'CandidateName', 'OfficeSought']

  sd = pd.DataFrame(data=None, columns=sd_cols_to_keep)
  rep = pd.DataFrame(data=None, columns=rep_cols_to_keep)

  for file in os.listdir(virginia_report_path):
    if file[0] != '.':
      file_path = virginia_report_path + file
      curr = pd.read_csv(file_path, error_bad_lines=False)
      curr.drop(columns=[col for col in curr.columns if col not in rep_cols_to_keep], inplace=True)
      rep = rep.append(curr)

  for file in os.listdir(virginia_schedule_d_path):
    if file[0] != '.':
      file_path = virginia_schedule_d_path + file
      curr = pd.read_csv(file_path, error_bad_lines=False)
      curr.drop(columns=[col for col in curr.columns if col not in sd_cols_to_keep], inplace=True)
      sd = sd.append(curr)

  merged_va = rep.merge(sd, on='ReportId')

  merged_va['PAYEE'] = ''
  merged_va['PAYEE'] = merged_va.apply(fill_payee_with_name, firstName='FirstName', lastName='LastOrCompanyName', axis=1)

  merged_va['CANDIDATE/COMMITTEE NAME'] = merged_va.apply(combine_candidate_committee, candidateName='CandidateName', committeeName='CommitteeName', axis=1)

  merged_va.drop(columns=['ReportId', 'CommitteeName', 'CommitteeType', 'CandidateName', 'FirstName', \
                          'LastOrCompanyName'], inplace=True)

  merged_va.rename(columns={'OfficeSought':'OFFICE', 'TransactionDate':'DATE', 'Amount':'AMOUNT', 'ItemOrService':'CATEGORY', 'CommitteeCode':'COM_CODE_ID'}, inplace=True)

  merged_va = clean_cols(merged_va, additional_cols=['OFFICE', 'CATEGORY'])

  merged_va.to_csv('../Processed_Data/VIRGINIA_PROCESSED.csv', index=None)

  """## West Virginia"""

  wv_path = '../Raw_Data/WEST VIRGINIA/'

  wv_cols_to_keep = ['Expenditure Amount', 'Expenditure Date', 'Last Name', 'First Name', 'Purpose', \
                  'Committee Type', 'Committee Name', "Candidate Name"]

  merged_wv = pd.DataFrame(data=None, columns=wv_cols_to_keep)

  for file in os.listdir(wv_path):
      if file[0] != '.':
        file_path = wv_path + file
        curr = pd.read_csv(file_path, warn_bad_lines=False, error_bad_lines=False)
        curr.drop(columns=[col for col in curr.columns if col not in wv_cols_to_keep], inplace=True)
        merged_wv = merged_wv.append(curr)

  merged_wv['PAYEE'] = merged_wv.apply(fill_payee_with_name, firstName='First Name', lastName='Last Name', axis=1)

  merged_wv['CANDIDATE/COMMITTEE NAME'] = ''
  merged_wv['CANDIDATE/COMMITTEE NAME'] = merged_wv.apply(combine_candidate_committee, candidateName='Candidate Name', committeeName='Committee Name', axis=1)

  merged_wv.rename(columns={'Expenditure Amount':'AMOUNT', 'Expenditure Date':'DATE', \
                            'Purpose':'CATEGORY', 'Committee Type':'COMMITTEE TYPE'}, inplace=True)

  merged_wv.drop(columns=['Last Name', 'First Name', 'Committee Name', 'Candidate Name'], inplace=True)

  merged_wv = clean_cols(merged_wv, additional_cols=['COMMITTEE TYPE', 'CATEGORY'])

  merged_wv.to_csv('../Processed_Data/WEST VIRGINIA_PROCESSED.csv', index=None)

  """## Massachusetts"""

  ma_file = pd.read_csv('../Raw_Data/MASSACHUSETTS/MA.csv')

  ma_file['PAYEE'] = 'NOT FOUND'
  ma_file['PAYEE'] = ma_file.apply(fill_payee_with_name, firstName = 'First_Name', lastName='Name', axis=1)

  ma_file = ma_file[ma_file['Amount'].notna()]

  ma_file = ma_file[ma_file['OCPF_Full_Name'].notna()]

  ma_file.rename(columns={'Report_ID':'REPORT_ID', 'Date':'DATE', 'Amount':'AMOUNT', 'Description':'CATEGORY', \
                          'OCPF_Full_Name':'CANDIDATE/COMMITTEE NAME'}, inplace=True)

  ma_file.drop(columns=['Name', 'First_Name'], inplace=True)

  ma_file = clean_cols(ma_file, additional_cols=['CATEGORY'])

  ma_file.to_csv('../Processed_Data/MASSACHUSETTS_PROCESSED.csv', index=None)

  """## California"""

  ca_file = pd.read_csv('../Raw_Data/CALIFORNIA/CA.csv')

  ca_file.drop(columns=['ENTITY_CD', 'CAND_NAML', 'CAND_NAMF', 'OFFICE_CD_x'], inplace=True)

  ca_file['PAYEE'] = 'NOT FOUND'
  ca_file['PAYEE'] = ca_file.apply(fill_payee_with_name, firstName='PAYEE_NAMF', lastName='PAYEE_NAML', axis=1)

  ca_file.rename(columns={'EXPN_DATE':'DATE', 'EXPN_CODE':'CATEGORY', 'ENTITY':'CANDIDATE/COMMITTEE NAME', \
                          'OFFICE_CD_y':'OFFICE'}, inplace=True)

  ca_file.drop(columns=['PAYEE_NAML', 'PAYEE_NAMF', 'ENTITY CODE'], inplace=True)

  ca_file = clean_cols(ca_file, additional_cols=['CATEGORY', 'OFFICE'])

  ca_file.to_csv('../Processed_Data/CALIFORNIA_PROCESSED.csv', index=None)

  """## New Mexico"""

  new_mexico_path = '../Raw_Data/NEW MEXICO/'

  nm_cols_to_keep = ['First Name', 'Last Name', 'Amount', 'ContribExpenditure Description', 'ContribExpenditure First Name', \
                    'ContribExpenditure Last Name', 'Company Name', 'Date Added']

  merged_nm = pd.DataFrame(data=None, columns=nm_cols_to_keep)

  for file in os.listdir(new_mexico_path):
      if file[0] != '.':
        file_path = new_mexico_path + file
        curr = pd.read_csv(file_path, warn_bad_lines=False, error_bad_lines=False, encoding='latin1')
        curr = curr[curr['IsContribution'] != 1]
        curr.drop(columns=[col for col in curr.columns if col not in nm_cols_to_keep], inplace=True)
        merged_nm = merged_nm.append(curr)

  merged_nm['CANDIDATE/COMMITTEE NAME'] = 'NOT FOUND'
  merged_nm['CANDIDATE/COMMITTEE NAME'] = merged_nm.apply(fill_payee_with_name, firstName='First Name', lastName='Last Name', axis=1)

  merged_nm['PAYEE'] = 'NOT FOUND'
  merged_nm['PAYEE'] = merged_nm.apply(fill_payee_with_name, firstName='ContribExpenditure First Name', lastName='ContribExpenditure Last Name', axis=1)
  merged_nm['PAYEE'] = merged_nm.apply(fill_not_found_with_company, companyName='Company Name', payeeName='PAYEE', axis=1)

  merged_nm.rename(columns={'Amount':'AMOUNT', 'ContribExpenditure Description':'CATEGORY', 'Date Added':'DATE'}, inplace=True)

  merged_nm.drop(columns=['First Name', 'Last Name', 'ContribExpenditure First Name', 'ContribExpenditure Last Name', 'Company Name'], inplace=True)

  merged_nm = clean_cols(merged_nm, additional_cols=['CATEGORY'], flip_date=True)

  merged_nm.to_csv('../Processed_Data/NEW_MEXICO_PROCESSED.csv', index=None)

  """## Oregon"""

  oregon_file = pd.read_csv('../Raw_Data/OREGON/OR.txt', encoding='latin1', sep='\t', error_bad_lines=False, warn_bad_lines=False)

  or_cols_to_keep = ['FILER_ID', 'FILER', 'TRAN_DATE', 'AMOUNT', 'PAYEE', 'PURPOSE_CODES']

  oregon_file.drop(columns=[col for col in oregon_file.columns if col not in or_cols_to_keep], inplace=True)

  oregon_file.rename(columns={'FILER_ID':'FILER ID','FILER':'CANDIDATE/COMMITTEE NAME', 'TRAN_DATE':'DATE', \
                              'PURPOSE_CODES':'CATEGORY'}, inplace=True)

  oregon_file = clean_cols(oregon_file, additional_cols=['CATEGORY'])

  oregon_file.to_csv('../Processed_Data/OREGON_PROCESSED.csv', index=None)

  """## Arizona """

  az_file = pd.read_csv('../Raw_Data/ARIZONA/AZ.csv')

  az_file.drop(columns=['Transaction Type'], inplace=True)

  az_file['Name'] = az_file['Name'].apply(lambda x: x.replace('(Check)', ''))

  az_file['Name'] = az_file['Name'].apply(lambda x: x[:x.index('(')])

  az_file.rename(columns={'Name':'CANDIDATE/COMMITTEE NAME', 'Date':'DATE', 'Paid to Name':'PAYEE', 'Amount':'AMOUNT'}, inplace=True)

  az_file = clean_cols(az_file, flip_date=True)

  az_file.to_csv('../Processed_Data/ARIZONA_PROCESSED.csv', index=None)

  """## Kentucky """

  ky_file = pd.read_csv('../Raw_Data/KENTUCKY/KY.csv')

  ky_file['CANDIDATE/COMMITTEE NAME'] = ''
  ky_file['CANDIDATE/COMMITTEE NAME'] = ky_file.apply(fill_payee_with_name, firstName='FromCandidateFirstName', lastName='FromCandidateLastName', axis=1)

  ky_file['PAYEE'] = ''
  ky_file['PAYEE'] = ky_file.apply(fill_payee_with_name, firstName='RecipientFirstName', lastName='RecipientLastName', axis=1)

  ky_file['PAYEE'] = ky_file.apply(lambda x: x['OrganizationName'] if not x['PAYEE'].replace(' ', '') else x['PAYEE'], axis=1)

  ky_file.rename(columns={'Purpose':'CATEGORY', 'DispursementAmount':'AMOUNT', 'DispursementDate':'DATE'}, inplace=True)

  ky_file.drop(columns=['RecipientLastName', 'RecipientFirstName', 'OrganizationName', 'Occupation', 'DispursementCode', \
                        'FromCandidateFirstName', 'FromCandidateLastName'], inplace=True)

  ky_file = clean_cols(ky_file, additional_cols=['CATEGORY'])

  ky_file.to_csv('../Processed_Data/KENTUCKY_PROCESSED.csv', index=None)