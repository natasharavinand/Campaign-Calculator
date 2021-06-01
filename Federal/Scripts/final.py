import pandas as pd
import os
import numpy as np
import re
from fuzzywuzzy import fuzz

pd.options.mode.chained_assignment = None

# general fuzzymatching function
def is_fuzzymatched(row, ref_entity, threshold=95, return_ratio=False):
    top_ratio = 0

    if not return_ratio:
        ws = fuzz.ratio(row, ref_entity) >= threshold or fuzz.partial_ratio(row, ref_entity) >= threshold

        if ws:
            return True
        else:
            return False
    else:
        top_ratio = max(fuzz.ratio(row, ref_entity),fuzz.partial_ratio(row, ref_entity))
        return top_ratio

'''
Function taking in a row that analyzes if the row identifies as a vendor in the payroll spaces, looks at the purpose of the transaction, and classifies the name
as either a transaction for payroll tax, fees, or payroll itself
'''

def payroll_vendors(row):
  try:
    vendor = row['PAYEE']
    purpose = row['CATEGORY']

    payroll_vendors = ['AUTOMATIC DATA PROCESSING (ADP)', 'COMPLETE PAYROLL SOLUTIONS', 'GUSTO', 'PAYCHEX', 'PAYLOCITY', 'PAYROLL DATA PROCESSING', 'PAYROLL NETWORK', 'ZENEFITS']

    if re.search('\\bADP\\b|AUTOMATIC DATA PROCESSING', vendor):
      vendor = 'AUTOMATIC DATA PROCESSING (ADP)'

    for payroll_vendor in payroll_vendors:
      if fuzz.ratio(payroll_vendor, vendor) > 85:
        if re.search('\\bTAX\\b|\\bTAXES\\b', purpose):
          vendor = payroll_vendor + ' (PAYROLL TAX)'
        elif 'FEE' in purpose:
          vendor = payroll_vendor + ' (FEES)'
        else:
          vendor = payroll_vendor + ' (PAYROLL)'
    return vendor
  except:
    return vendor

# formatting names (ex. WARNOCK PEOPLE FOR -> PEOPLE FOR WARNOCK, SMITH JOHN -> JOHN SMITH)
def format_names(row, names):
  payee = row['PAYEE'].strip()
  original_payee = payee
  changed = False

  if 'TOWN OF' in payee and payee.index('TOWN OF') != 0:
    index = payee.index('TOWN OF')
    payee = (payee[index:] + ' ' + payee[:index]).strip()
    return payee
  elif 'PEOPLE FOR' in payee and payee.index('PEOPLE FOR') != 0:
    index = payee.index('PEOPLE FOR')
    payee = (payee[index:] + ' ' + payee[:index]).strip()
    return payee
  elif 'CITY OF' in payee and payee.index('CITY OF') != 0:
    index = payee.index('CITY OF')
    payee = (payee[index:] + ' ' + payee[:index]).strip()
    return payee
  elif 'FRIENDS OF' in payee and payee.index('FRIENDS OF') != 0:
    index = payee.index('FRIENDS OF')
    payee = (payee[index:] + ' ' + payee[:index]).strip()
    return payee
  elif 'CITIZENS FOR' in payee and payee.index('CITIZENS FOR') != 0:
    index = payee.index('CITIZENS FOR')
    payee = (payee[index:] + ' ' + payee[:index]).strip()
    return payee
  elif 'SOCIETY OF' in payee and payee.index('SOCIETY OF') != 0:
    index = payee.index('SOCIETY OF')
    payee = (payee[index:] + ' ' + payee[:index]).strip()
    return payee
  elif 'MUSEUM OF' in payee and payee.index('MUSEUM OF') != 0:
    index = payee.index('MUSEUM OF')
    payee = (payee[index:] + ' ' + payee[:index]).strip()
    return payee
  elif 'COMMITTEE TO' in payee and payee.index('COMMITTEE TO') != 0:
    index = payee.index('COMMITTEE TO')
    payee = (payee[index:] + ' ' + payee[:index]).strip()
    return payee
  elif 'ATHLETIC CLUB OF' in payee and payee.index('ATHLETIC CLUB OF') != 0:
    index = payee.index('ATHLETIC CLUB OF')
    payee = (payee[index:] + ' ' + payee[:index]).strip()
    return payee

  if len(payee.split()) > 1 and (payee.split()[-1] in names or payee.split()[-2] in names):
    if len(payee.split()[-1]) != 1:
      if payee.split()[-1] in names and not re.search('NGP', payee): # if we have a name as our last string (ex. SMITH JOHN) we want to reverse (ex. JOHN SMITH)
        payee = payee.split()[-1] + ' ' + ' '.join(payee.split()[:-1])
        changed = True
    else:
      if payee.split()[-2] in names and len(payee.split()[-1]) == 1 and not re.search('NGP', payee):
        payee = payee.split()[-2] + ' ' + payee.split()[-1] + ' ' + ' '.join(payee.split()[:-2]) # if we have a name + middle initial as our last string (ex. SMITH JOHN H) reverse (ex. JOHN H SMITH)
        changed = True

  if not changed:
    return ' '.join(payee.split()).strip()
  else:
    if payee.split()[-1] in names:
      return ' '.join(original_payee.split()).strip()
    else:
      return ' '.join(payee.split()).strip()

# runs the main cleaning functions
def main_cleaning(df, names):
  df['PAYEE'] = df['PAYEE'].apply(lambda x: 'UNKNOWN' if type(x) == float or not x.replace(' ', '') else x)

  df['PAYEE'] = df['PAYEE'].apply(lambda x: re.sub('INCORPORATED|\\bINC.?$|CORPORATION|\\bCORP.?$', '', x).strip())
  df['PAYEE'] = df['PAYEE'].apply(lambda x: re.sub(' P?.?L.?L.?C.?$| L.?T.?D.?$|\\bLLC\\b|\\bL.?T.?D.?\\b', '', x).strip())
  df['PAYEE'] = df['PAYEE'].apply(lambda x: x.replace('.ORG', '').replace('.COM', '').strip())
  df['PAYEE'] = df['PAYEE'].apply(lambda x: x.replace('CMTE', 'COMMITTEE').strip())
  df['PAYEE'] = df['PAYEE'].apply(lambda x: re.sub('^MRS.? ?|^MR.? ?|JR.? ?|SR.? ?', '', x).strip())
  df['PAYEE'] = df['PAYEE'].apply(lambda x: x.replace('.', '').replace(',', '').replace('&AMP', '').strip())

  df['PAYEE'] = df['PAYEE'].apply(lambda x: x[:x.find('/') - 1].strip() if x.find('/') != -1 else x)
  df['PAYEE'] = df['PAYEE'].apply(lambda x: x[:x.find('- 2') - 1].strip() if x.find('- 2') != -1 else x)

  df['PAYEE'] = df['PAYEE'].apply(lambda x: x.replace('/', ''))
  df['PAYEE'] = df['PAYEE'].apply(lambda x: x.replace('\\', ''))
  
  df['PAYEE'] = df['PAYEE'].apply(lambda x: x.replace('-', '').replace('–', '') if (len(x.split()) > 1 and x.split()[0] not in names and x.split()[-1] not in names) else x)
  df['PAYEE'] = df['PAYEE'].apply(lambda x: x.replace('|', ''))
  df['PAYEE'] = df['PAYEE'].apply(lambda x: x.replace(';', ''))
  df['PAYEE'] = df['PAYEE'].apply(lambda x: x.replace(':', ''))
  df['PAYEE'] = df['PAYEE'].apply(lambda x: x.replace('"', ''))

  df['PAYEE'] = df['PAYEE'].apply(lambda x: x.replace('SOFT WARE', 'SOFTWARE').replace('FREDMEYER', 'FRED MEYER').replace('GRASOTS', 'GRASSROOTS'))
  df['PAYEE'] = df['PAYEE'].apply(lambda x: x if not re.search('\\bASSOC\\b', x) else x.replace('ASSOC', 'ASSOCIATION'))
  df['PAYEE'] = df['PAYEE'].apply(lambda x: x if not re.search('\\bDEPT\\b', x) else x.replace('DEPT', 'DEPARTMENT'))
  df['PAYEE'] = df['PAYEE'].apply(lambda x: x if not re.search('\\bUSPO\\b', x) else x.replace('USPO', 'USPS'))
  df['PAYEE'] = df['PAYEE'].apply(lambda x: x if not re.search('\\bNEWSMEDI\\b', x) else x.replace('NEWSMEDI', 'NEWS MEDIA'))
  df['PAYEE'] = df['PAYEE'].apply(lambda x: x if not re.search('\\bJOURNA\\b', x) else x.replace('JOURNA', 'JOURNAL'))
  df['PAYEE'] = df['PAYEE'].apply(lambda x: x if not re.search('\\bCONF\\b', x) else x.replace('CONF', 'CONFERENCE'))
  
  df['PAYEE'] = df['PAYEE'].apply(lambda x: 'FIRST BANK' if '1ST BANK' in x else x)

  df['PAYEE'] = df.apply(format_names, names=names, axis=1)
  df['PAYEE'] = df.apply(payroll_vendors, axis=1)

  df['PAYEE'] = df['PAYEE'].apply(lambda x: ' '.join(x.split()).strip())

  return df

# replacing long payee names with shorter versions if keyword found
def replacements(row):
  payee = row['PAYEE']
  if not payee:
      return 'NOT FOUND'
  payee = payee.strip()
  payee_no_ws = payee.replace(' ', '')
  quick_catches = ['AMAZON', 'FACEBOOK', 'GOOGLE', 'TWITTER', 'VERIZON', 'WALMART', 'ANEDOT', 'SHELL', 'OFFICE DEPOT', 'STRIPE', \
                   'SQUARE', 'STAPLES', 'PAYPAL', 'HOME DEPOT', 'COSTCO', 'CHEVRON', 'MAILCHIMP', 'TD BANK', \
                   'WELLS FARGO', 'MICROSOFT', 'TEXACO', 'YMCA']

  for catch in quick_catches:
    if catch in payee:
      return catch

  if 'POST OFFICE' in payee or 'POSTAL SERVICE' in payee or 'USPS' in payee or 'POSTAL' in payee:
    return 'US POSTAL SERVICE/POSTMASTER (USPS)'
  elif re.search('^\\bUPS\\b$', payee) or 'UPS STORE' in payee:
    return 'THE UPS STORE'
  elif 'FEDEX' in payee:
    return 'FEDERAL EXPRESS (FEDEX)'
  elif 'VANTIV' in payee or 'WORLDPLAY' in payee or 'WORLD PLAY' in payee:
    return 'WORDPLAY (VANTIV)'
  elif 'ACTBLUE' in payee or 'ACT BLUE' in payee:
    return 'ACTBLUE'
  elif 'WINRED' in payee or 'WIN RED' in payee:
    return 'WIN RED'
  elif 'MULTIPLE CONTRIBUTORS' in payee or 'MULTIPLE VENDORS' in payee:
    return 'UNITEMIZED EXPENSE'
  elif 'G SUITE' in payee:
    return 'GOOGLE'
  elif 'VISTAPRINT' in payee or 'VISTA PRINT' in payee:
    return 'VISTAPRINT'
  elif 'CHASE' in payee and 'MARKETING' not in payee:
    return 'CHASE'
  elif 'TREASURY' in payee:
    return 'UNITED STATES (US) TREASURY'
  elif 'HILTON' in payee:
    return 'HILTON HOTELS'
  elif 'MARRIOTT' in payee:
    return 'MARRIOTT HOTELS'
  elif 'HYATT' in payee:
    return 'HYATT HOTELS'
  elif re.search('\\bCOMM\\b', payee):
    return payee.replace('COMM', 'COMMITTEE')
  elif 'UNITEMIZED' in payee or 'EXPENSES UNDER' in payee or 'EXPENSES OF' in payee or 'MISCELLANEOUS' in payee or re.search('^TOTAL$', payee):
    return 'UNITEMIZED EXPENSE'
  elif 'EXXON' in payee:
    return 'EXXON MOBIL'
  elif 'GO DADDY' in payee or 'GODADDY' in payee:
    return 'GO DADDY'
  elif 'LOWES' in payee or "LOWE'S" in payee:
    return 'LOWES'
  elif re.search("^\\bSAM'?S\\b$", payee) or re.search("SAMS CLUB", payee):
    return "SAM'S CLUB"
  elif 'AT&T' in payee_no_ws:
    return 'AT&T'
  elif re.search('\\bARENA\\b', payee):
    return 'ARENA'
  elif re.search('\\bAMEX\\b', payee) or 'AMERICAN EXPRESS' in payee:
    return 'AMERICAN EXPRESS (AMEX)'
  elif re.search('\\bIMGE\\b', payee):
    return 'IMGE'
  elif 'NGP' in payee:
    return 'NGP VAN'
  elif 'OFFICE MAX' in payee or 'OFFICEMAX' in payee:
    return 'OFFICE MAX'
  elif 'L2' in payee:
    return 'L2 INC'
  elif 'BALANCE TRANSFER' in payee:
    return 'BALANCE TRANSFER TO NEXT ELECTION'
  elif 'AXMEDIA' in payee or 'AX MEDIA' in payee:
    return 'AX MEDIA'
  elif 'RUN THE WORLD' in payee:
    return 'RUN THE WORLD'
  elif 'BLUEWEST' in payee or 'BLUE WEST' in payee:
    return 'BLUE WEST MEDIA'
  return payee

# function to fuzzymatch categories depending on keywords found
def category_matching(row, payee_references, payee_refs_df):
  payee_name = row['PAYEE'].strip()
  curr_category = row['CATEGORY']
  if not curr_category or type(curr_category) == float:
      return 'GENERAL OPERATIONS & SUPPLIES'
  payee_refs_df = payee_refs_df
  if payee_name in payee_references:
    return payee_refs_df[payee_refs_df['NAME'] == payee_name]['CATEGORY'].values[0]
  else:
    if (re.search('PAYROLL|SALARY|WAGES|COMPENSATION|CHECK|STAFF|WORKER|CONTRACT|ORGANIZER',curr_category) or re.search('GUSTO|PAYCHEX|PAYROLL|AUTOMATIC DATA PROCESSING|COMPLETE PAYROLL SOLUTIONS|PAYROLL NETWORK|ZENEFITS',payee_name)) and not re.search('FEE|TAX', curr_category):
      return 'SALARY & PAYROLL'
    elif (re.search('PAYROLL|SALARY|WAGES|COMPENSATION|CHECK|STAFF|WORKER|CONTRACT|ORGANIZER',curr_category) or re.search('GUSTO|PAYCHEX|PAYROLL|AUTOMATIC DATA PROCESSING|COMPLETE PAYROLL SOLUTIONS|PAYROLL NETWORK|ZENEFITS',payee_name)) and re.search('TAX', curr_category):
      return 'PAYROLL TAX'
    elif (re.search('PAYROLL|SALARY|WAGES|COMPENSATION|CHECK|STAFF|WORKER|CONTRACT|ORGANIZER',curr_category) or re.search('GUSTO|PAYCHEX|PAYROLL|AUTOMATIC DATA PROCESSING|COMPLETE PAYROLL SOLUTIONS|PAYROLL NETWORK|ZENEFITS',payee_name)) and re.search('FEE|CHARGE', curr_category):
      return 'SALARY & PAYROLL PROCESSING FEES'
    elif re.search('FACEBOOK|GOOGLE|TWITTER|YOUTUBE|DIGITAL ACQUISITION|DIGITAL ADVERTISING|DIGITAL AD|SMS', curr_category) or re.search('FACEBOOK|GOOGLE|TWITTER|YOUTUBE|DIGITAL ACQUISITION|DIGITAL ADVERTISING|DIGITAL AD|SMS', payee_name):
      return 'DIGITAL ADVERTISING'
    elif re.search('\\bTECH\\b|TECHNOLOGY|SOFTWARE|COMPUTER|LAPTOP', curr_category) or re.search('\\bTECH\\b|TECHNOLOGY|\\bIT\\b|SOFTWARE|COMPUTER|LAPTOP', payee_name):
      return 'SOFTWARE & TECHNOLOGY'
    elif re.search('\\bTAX\\b|TAXES', curr_category) or re.search('\\bTAX\\b|TAXES', payee_name):
      return 'TAXES'
    elif re.search('MARKETING|COMMUNICATIONS|\\bPR\\b|PUBLIC RELATION', curr_category) or re.search('MARKETING|COMMUNICATIONS|\\bPR\\b|PUBLIC RELATION', payee_name):
      return 'PR, COMMUNICATIONS, & MARKETING'
    elif re.search('LEGAL|\\bLAW\\b|ATTORNEY', curr_category) or re.search('LEGAL|\\bLAW\\b|ATTORNEY', payee_name):
      return 'LEGAL SERVICES'
    elif re.search('PAYMENT|\\bBANK\\b', curr_category) or re.search('PAYMENT|\\bBANK\\b|BANK OF AMERICA', payee_name):
      return 'CREDIT CARD PAYMENT & BANK FEES'
    elif re.search('PROCESSING|TRANSACTION|MERCHANT|SERVICE CHARGE', curr_category) or re.search('\\bCHASE\\b|AMERICAN EXPRESS|\\bAMEX\\b', payee_name):
      return 'CREDIT CARD PROCESSING FEES'
    elif re.search('\\bGAS\\b|FUEL|MILEAGE', curr_category) or re.search('\\bGAS\\b|FUEL|MILEAGE', payee_name):
      return 'FUEL & GAS'
    elif re.search('FOOD|COFFEE|\\bCATER\\b|\\bMEAL\\b|DINNER|LUNCH|BREAKFAST|PIZZA|BURGER|BARBEQUE|BBQ|CAFE|RESTAURANT|DINING|LIQUOR|BAKERY|\\bBAKE\\b|PIZZA|GRILL|\\bBREW\\b|BISTRO|STEAKHOUSE|CATER|SANDWICH|BAGEL', curr_category) or re.search('FOOD|COFFEE|\\bCATER\\b|\\bMEAL\\b|DINNER|LUNCH|BREAKFAST|PIZZA|BURGER|BARBEQUE|BBQ|CAFE|RESTAURANT|DINING|LIQUOR|BAKERY|\\bBAKE\\b|PIZZA|GRILL|\\bBREW\\b|BISTRO|STEAKHOUSE|CATER|SANDWICH|BAGEL', payee_name):
      return 'MEALS & CATERING'
    elif re.search('TRAVEL|CAR|TAXI|LODGING|PLANE|CAR|FLIGHT|LIMO|JET|TRANSPORTATION|HOTEL|\\bINN\\b|MARRIOTT|HILTON|HOUSING|AIRPORT|CARGO|LUGGAGE|INTERCONTINENTAL|COURTYARD|METRO|LODGE', curr_category) or re.search('TRAVEL|CAR|TAXI|LODGING|PLANE|CAR|FLIGHT|LIMO|JET|TRANSPORTATION|HOTEL|\\bINN\\b|MARRIOTT|HILTON|HOUSING|AIRPORT|CARGO|LUGGAGE|INTERCONTINENTAL|COURTYARD|METRO|LODGE', payee_name):
      return 'TRAVEL & LODGING'
    elif re.search('\\bAD\\b|ADVERTISEMENT|ADVERTISING|RADIO|TV|TELEVISION|MEDIA|PRODUCTION|NEWS|STATION|STUDIO|BROADCAST|PAPER|\\bAM\\b|\\bFM\\b|JOURNAL|PRESS|MAGAZINE|TIMES|COURIER|REPORTER|HERALD|GAZETTE', curr_category) or re.search('\\bAD\\b|ADVERTISEMENT|ADVERTISING|RADIO|TV|TELEVISION|MEDIA|PRODUCTION|NEWS|STATION|STUDIO|BROADCAST|PAPER|\\bAM\\b|\\bFM\\b|JOURNAL|PRESS|MAGAZINE|TIMES|COURIER|REPORTER|HERALD|GAZETTE', payee_name):
      return 'MEDIA & ADS'
    elif re.search('PAID PHONE CALL|VOLUNTEER|CANVASS|PHONE BANK|VOTER|FIELD|COORDINAT|CAMPAIGN SERVICE|ROBOCALL|ROBO CALL', curr_category) or re.search('PAID PHONE CALL|VOLUNTEER|CANVASS|PHONE BANK|VOTER|FIELD|COORDINAT|CAMPAIGN SERVICE|ROBOCALL|ROBO CALL', payee_name):
      return 'CANVASSING & FIELD WORK'
    elif re.search('UTILITIES|PHONE|CELL|CITY OF|POWER|PHONE SERVICES|CALLFIRE|CALL FIRE|WIRELESS', payee_name) or re.search('UTILITIES|PHONE|CELL|CITY OF|POWER|PHONE SERVICES|CALLFIRE|CALL FIRE|WIRELESS', payee_name):
      return 'UTILITIES'
    elif re.search('OFFICE|RENT|PARKING|STORAGE|ENVELOPE|REALTY|PROPERTY', curr_category) or re.search('OFFICE|RENT|PARKING|STORAGE|ENVELOPE|REALTY|PROPERTY', payee_name):
      return 'OFFICE EXPENSES'
    elif re.search('SIGN|SHIRT|HAT|STICKERS|PARAPHERNALIA|PROMOTIONAL GOOD|BUTTON|TSHIRT|BANNER|\\bMUGS?\\b|\\bFLAGS?\\b', curr_category) or re.search('SIGN|SHIRT|CAP|HAT|STICKERS|PARAPHERNALIA|PROMOTIONAL GOOD|BUTTON|TSHIRT|BANNER|\\bMUGS?\\b|\\bFLAGS?\\b', payee_name):
      return 'CAMPAIGN SIGNS & PARAPHERNALIA'
    elif re.search('EMAIL|\\bWEB\\b|WEBSITE', curr_category) or re.search('EMAIL|\\bWEB\\b|WEBSITE', payee_name):
      return 'WEBSITE SERVICES'
    elif re.search('PRINT|GRAPHIC|DESIGN|PHOTO|POSTER|ART|COPY', curr_category) or re.search('PRINT|GRAPHIC|DESIGN|PHOTO|POSTER|ART|COPY', payee_name):
      return 'PRINTING'
    elif re.search('SHIPPING|POSTAGE|\\bSHIP\\b|SHIPMENT', curr_category) or re.search('SHIPPING|POSTAGE|\\bSHIP\\b|MAIL|SHIPMENT', payee_name):
      return 'SHIPPING & POSTAGE'
    elif re.search('DIRECT|MAILING|\\bMAIL\\b|MAILERS', curr_category) or re.search('DIRECT|MAILING|\\bMAIL\\b|MAILERS', payee_name):
      return 'DIRECT MAIL'
    elif re.search('CLUB|EVENT|SOUND|LIGHTING|VENUE|TICKET|BANQUET|PARADE|REGISTRATION|SPORT|GIANTS|MUSIC|HOSTING|FESTIVAL|OPERA|FORUM|ENTERTAINMENT|AUDIO|\\bBAND\\b', curr_category) or re.search('CLUB|EVENT|SOUND|LIGHTING|VENUE|TICKET|BANQUET|PARADE|REGISTRATION|SPORT|GIANTS|MUSIC|HOSTING|FESTIVAL|OPERA|FORUM|ENTERTAINMENT|AUDIO|\\bBAND\\b', payee_name):
      return 'EVENT EXPENSES'
    elif re.search('FUNDRAISING', curr_category) or re.search('FUNDRAISING', payee_name):
      return 'FUNDRAISING & FUNDRAISING CONSULTING' 
    elif re.search('COMPLIANCE', curr_category) or re.search('COMPLIANCE', payee_name):
      return 'COMPLIANCE SERVICES'
    elif re.search('FINANC|ACCOUNTING|\\bCPA\\b|ACCOUNT|TREASURER|INVEST', curr_category) or re.search('FINANC|ACCOUNTING|\\bCPA\\b|ACCOUNT|TREASURER|INVEST', payee_name):
      return 'ACCOUNTING & FINANCIAL MANAGEMENT'
    elif re.search('DONATION|DUES|CHURCH|SCHOLARSHIP|CHARITY|FOR LIFE|CONTRI.*TION|LITTLE LEAGUE|YOUTH|BASEBALL|EDUCATION|SCHOOL|CHAPTER|MUSEUM|FOUNDATION|SPONSORSHIP|ST JUDE|HOSPITAL|CHARITY|LEADERSHIP|MISSION|ASSOCIATION|LIVING CENTER|HOSPICE|FOUNDATION|GIFT|CHILDREN|FEDERATION', curr_category) or \
      re.search('DONATION|CONTRIBUTION|DUES|CHURCH|SCHOLARSHIP|CHARITY|FOR LIFE|CONTRI.*TION|LITTLE LEAGUE|YOUTH|BASEBALL|EDUCATION|SCHOOL|CHAPTER|MUSEUM|FOUNDATION|SPONSORSHIP|ST JUDE|HOSPITAL|CHARITY|LEADERSHIP|MISSION|ASSOCIATION|LIVING CENTER|HOSPICE|FOUNDATION|GIFT|CHILDREN|FEDERATION', payee_name):
      return 'CONTRIBUTION & DONATION'
    elif re.search('POLL|SURVEY|OPINION', curr_category) or re.search('POLL|SURVEY|OPINION', payee_name):
      return 'POLLING & RESEARCH'
    elif re.search('INSURANCE|INSURE', curr_category) or re.search('INSURANCE|INSURE', payee_name):
      return 'INSURANCE'
    elif re.search('MEETING|\\bMTG\\b', payee_name) or re.search('MEETING|\\bMTG\\b', payee_name):
      return 'MEETING EXPENSES'
    elif re.search('\\bPAC\\b|POLITICAL ACTION COMMITTEE|COMMITTEE|SUPPORT|ASSOCIATION|BALLOT|NO ON|YES ON|INITIATIVE|CHAMBER OF COMMERCE|FEDERATION|CITIZENS FOR|ELECTION|REPUBLICAN|DEMOCRAT|CONSERVATIVE|VICTORY FUND|HOUSE FUND|SENATE', curr_category) or re.search('\\bPAC\\b|POLITICAL ACTION COMMITTEE|COMMITTEE|SUPPORT|ASSOCIATION|BALLOT|NO ON|YES ON|INITIATIVE|CHAMBER OF COMMERCE|FEDERATION|CITIZENS FOR|ELECTION|REPUBLICAN|DEMOCRAT|CONSERVATIVE|VICTORY FUND|HOUSE FUND|SENATE', payee_name):
      return 'POLITICAL FUNDS & GROUPS'
    elif re.search('CONSULT|CAMPAIGN MANAGEMENT|STRATEGY|STRATEGIES', curr_category) or re.search('CONSULT|CAMPAIGN MANAGEMENT|STRATEGY|STRATEGIES', payee_name):
      return 'CAMPAIGN CONSULTING'
    elif re.search('DEPARTMENT OF|SECRETARY OF|QUALIFYING|ETHICS COMM|REGISTRATION', curr_category) or re.search('DEPARTMENT OF|SECRETARY OF|QUALIFYING|ETHICS COMM|REGISTRATION', payee_name):
      return 'CANDIDATE FILING & BALLOT FEES'
    else:
      return 'GENERAL OPERATIONS & SUPPLIES'

# function to fuzzymatch reference payees depending on threshold 
def fuzzymatch_top_payees(row, payee_references):
  payee = row['PAYEE']
  if re.search('\(PAYROLL\)|\(PAYROLL TAX\)|\(FEES\)',payee):
    return payee
  ref_dict = {}

  for reference in payee_references:
    reference = reference.strip()
    ref_dict[reference] = is_fuzzymatched(payee, reference, return_ratio=True) # save reference and max ratio in dictionary
    if len(ref_dict) == 0:
      pass
    else:
      maximum = max(ref_dict, key=ref_dict.get)
    if ref_dict[maximum] > 95:
      return maximum
  return payee 

if __name__ == '__main__':
  # initialize category dataframe with name, category, amount

  category_df = pd.DataFrame(data=None, columns=['STATE', 'CANDIDATE', 'OFFICE', 'DISTRICT', 'CATEGORY', 'AMOUNT'])

  # initialize vendor dataframe with name, vendor, amount

  payee_df = pd.DataFrame(data=None, columns=['STATE', 'CANDIDATE', 'OFFICE', 'DISTRICT', 'PAYEE', 'AMOUNT'])

  # initialize overall dataframe with name, vendor, amount

  total_df = pd.DataFrame(data=None, columns=['STATE', 'CANDIDATE', 'OFFICE', 'DISTRICT', 'TOTAL AMOUNT'])

  # initialize cleaned data path and candidate query file

  federal_officials = pd.read_csv('../References/FEDERAL_CANDIDATE_QUERY.csv')
  federal_officials_names = list(federal_officials['NAME'].unique())

  payee_refs_df = pd.read_csv('../References/REFERENCES.csv')
  payee_refs = list(payee_refs_df['NAME'].unique())

  category_refs_df = pd.read_csv('../References/CATEGORIES.csv')
  category_refs = list(category_refs_df['CATEGORY'].unique())

  names = pd.read_csv('../References/NAMES.csv')
  names = list(names['NAME'])

  processed = pd.read_csv('../Processed_Data/PROCESSED.csv')

  counter = 1

  for official in federal_officials_names:
      # get the candidate df of expenses
      print(official)
      print(counter // len(federal_officials_names * 100))
      associated_query = federal_officials[federal_officials['NAME'] == official]['FEC NAME'].values[0]
      cand_df = processed[processed['NAME'].str.contains(associated_query, na=False)]
      cand_df.drop_duplicates(subset=['PAYEE', 'AMOUNT', 'DATE'], inplace=True)

      cand_df.dropna(subset=['DATE'], inplace=True)

      cand_df['DATE'] = cand_df['DATE'].apply(lambda x: str(int(x)))

      cand_df['PAYEE'] = cand_df['PAYEE'].apply(lambda x: x if type(x) == str else 'NOT FOUND')

      # get the total amount information
      election_cycle = int(federal_officials[federal_officials['NAME'] == official]['ELECTION YEAR'].values[0])
      office = federal_officials[federal_officials['NAME'] == official]['OFFICE'].values[0]
      district = federal_officials[federal_officials['NAME'] == official]['DISTRICT'].values[0]
      state_name = federal_officials[federal_officials['NAME'] == official]['STATE'].values[0]

      if 'HOUSE' in office:
          election_cycle_regex = '2019$|2020$'
      else:
          if election_cycle == 2020:
              election_cycle_regex = '2015$|2020$'
          elif election_cycle == 2018:
              election_cycle_regex = '2013$|2018$'
          else:
              election_cycle_regex = '2011$|2016$'

      if len(cand_df) != 0:
          cand_df = cand_df[cand_df['DATE'].str.contains(election_cycle_regex, na=False)] # here is where you segment by year

      # cleaning & fuzzymatching
      cand_df['PAYEE'] = cand_df.apply(replacements,axis=1)
      cand_df = main_cleaning(cand_df, names=names)
      cand_df['PAYEE'] = cand_df.apply(fuzzymatch_top_payees, payee_references=payee_refs, axis=1)
      cand_df['CATEGORY'] = cand_df.apply(category_matching, payee_references=payee_refs, payee_refs_df=payee_refs_df, axis=1)

      total_amount = cand_df['AMOUNT'].sum() 
      curr_row = {'STATE':state_name, 'CANDIDATE':official, 'OFFICE':office, 'DISTRICT':district, 'TOTAL AMOUNT':total_amount}
      total_df = total_df.append(curr_row, ignore_index=True)

      not_found_payee_dict = {'STATE':state_name, 'CANDIDATE':official, 'OFFICE':office, 'DISTRICT':district,'PAYEE':0, 'AMOUNT':0}
      not_found_category_dict = {'STATE':state_name, 'CANDIDATE':official, 'OFFICE':office, 'DISTRICT':district,'CATEGORY':0, 'AMOUNT':0}

      # if we found candidate information (it's not 'NOT FOUND')
      if total_amount > 0:
          # find "rough" top payees
          rough_top_payees = list(cand_df.groupby('PAYEE').sum().sort_values(by='AMOUNT', ascending=False).index.values)

          # find top 10 categories
          cand_top_categories = list(cand_df.groupby('CATEGORY').sum().sort_values(by='AMOUNT', ascending=False).index.values)[:10]

          cand_top_payees = []
          cand_top_payees.append(rough_top_payees[0])
          
          # write over duplicates (ex JOHN SMITH and JOHN K SMITH both in top 10)
          for payee in rough_top_payees:
              duplicate = False
              for cand_payee in cand_top_payees:
                  if fuzz.ratio(payee, cand_payee) >= 95 or fuzz.ratio(payee.replace(' ', ''), cand_payee.replace(' ', '')) >= 95:
                      duplicate = True
                      cand_df['PAYEE'] = cand_df['PAYEE'].apply(lambda x: cand_payee if x == payee else x)
                      break
              if not duplicate:
                  # if payee in ['NOT FOUND', 'UNITEMIZED EXPENSE', 'UNKNOWN', 'ITEMS 100 OR LESS']:
                  #     payee = 'UNITEMIZED EXPENSE/UNKNOWN'
                  cand_top_payees.append(payee)
              if len(cand_top_payees) == 10:
                  break

          payee_counter = 0
          
          # collect 10 payees, write a row each time
          for i in range(len(cand_top_payees)):
              payee_amount = cand_df[cand_df['PAYEE'] == cand_top_payees[i]]['AMOUNT'].sum()
              if cand_top_payees[i] in ['NOT FOUND', 'UNITEMIZED EXPENSE', 'UNKNOWN', 'ITEMS 100 OR LESS']:
                  cand_top_payees[i] = 'UNITEMIZED EXPENSE/UNKNOWN'
              payee_dict = {'STATE':state_name, 'CANDIDATE':official, 'OFFICE':office, 'DISTRICT':district,'PAYEE':cand_top_payees[i], 'AMOUNT':payee_amount}
              payee_df = payee_df.append(payee_dict, ignore_index=True)
              if payee_counter > 9:
                  break
              payee_counter += 1
                              
          # fill in with not founds if there's less than 10 payees
          while payee_counter < 10:
              payee_df = payee_df.append(not_found_payee_dict, ignore_index=True)
              payee_counter += 1

              
          # collect 10 categories, write a row each time
          category_counter = 0
          for i in range(len(cand_top_categories)):
              category_amount = cand_df[cand_df['CATEGORY'] == cand_top_categories[i]]['AMOUNT'].sum()
              category_dict = {'STATE':state_name, 'CANDIDATE':official, 'OFFICE':office, 'DISTRICT':district,'CATEGORY':cand_top_categories[i], 'AMOUNT':category_amount}
              category_df = category_df.append(category_dict, ignore_index=True)
              if category_counter > 9:
                  break
              category_counter += 1
          
          # fill in with not founds if there's less than 10 categories
          while category_counter < 10:
              category_df = category_df.append(not_found_category_dict, ignore_index=True)
              category_counter += 1
      else:
          # write 10 rows of '0' information for 'NOT FOUND'
          for i in range(10):
              payee_df = payee_df.append(not_found_payee_dict, ignore_index=True)
          for i in range(10):
              category_df = category_df.append(not_found_category_dict, ignore_index=True)
      counter += 1


  payee_df.to_csv('../Final_Data/FEDERAL_PAYEE_DF.csv', index=None)
  category_df.to_csv('../Final_Data/FEDERAL_CATEGORY_DF.csv', index=None)
  total_df.to_csv('../Final_Data/FEDERAL_TOTAL_DF.csv', index=None)