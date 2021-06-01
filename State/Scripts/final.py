import pandas as pd
import os
import numpy as np
import re
from fuzzywuzzy import fuzz

# runs all the main cleaning
def main_cleaning(df, names):
  df['PAYEE'] = df['PAYEE'].apply(lambda x: 'UNKNOWN' if type(x) == float or not x.replace(' ', '') else x)

  df['PAYEE'] = df['PAYEE'].apply(lambda x: re.sub('INCORPORATED|\\bINC.?$|CORPORATION|\\bCORP.?$', '', x).strip())
  df['PAYEE'] = df['PAYEE'].apply(lambda x: re.sub(' P?.?L.?L.?C.?$| L.?T.?D.?$|\\bLLC\\b|\\bLTD\\b', '', x).strip())
  df['PAYEE'] = df['PAYEE'].apply(lambda x: x.replace('.ORG', '').replace('.COM', '').strip())
  df['PAYEE'] = df['PAYEE'].apply(lambda x: x.replace('CMTE', 'COMMITTEE').strip())
  df['PAYEE'] = df['PAYEE'].apply(lambda x: re.sub('^MRS.? ?|^MR.? ?|JR.? ?|SR.? ?', '', x).strip())
  df['PAYEE'] = df['PAYEE'].apply(lambda x: x.replace('.', '').replace(',', '').replace('&AMP', '').strip())

  df['PAYEE'] = df['PAYEE'].apply(lambda x: x[:x.find('- 2') - 1].strip() if x.find('- 2') != -1 else x)

  df['PAYEE'] = df['PAYEE'].apply(lambda x: x.replace('/', ''))
  df['PAYEE'] = df['PAYEE'].apply(lambda x: x.replace('\\', ''))
  
  df['PAYEE'] = df['PAYEE'].apply(lambda x: x.replace('-', '').replace('–', '') if (len(x.split()) > 1 and x.split()[0] not in names and x.split()[-1] not in names) else x)
  df['PAYEE'] = df['PAYEE'].apply(lambda x: x.replace('|', ''))
  df['PAYEE'] = df['PAYEE'].apply(lambda x: x.replace(';', ''))
  df['PAYEE'] = df['PAYEE'].apply(lambda x: x.replace(':', ''))
  df['PAYEE'] = df['PAYEE'].apply(lambda x: x.replace('"', ''))

  df['PAYEE'] = df['PAYEE'].apply(lambda x: x.replace('SOFT WARE', 'SOFTWARE').replace('FREDMEYER', 'FRED MEYER').replace('GRASROTS', 'GRASSROOTS'))
  df['PAYEE'] = df['PAYEE'].apply(lambda x: x if not re.search('\\bASSOC\\b', x) else x.replace('ASSOC', 'ASSOCIATION'))
  df['PAYEE'] = df['PAYEE'].apply(lambda x: x if not re.search('\\bDEPT\\b', x) else x.replace('DEPT', 'DEPARTMENT'))
  df['PAYEE'] = df['PAYEE'].apply(lambda x: x if not re.search('\\bUSPO\\b', x) else x.replace('USPO', 'USPS'))
  df['PAYEE'] = df['PAYEE'].apply(lambda x: x if not re.search('\\bNEWSMEDI\\b', x) else x.replace('NEWSMEDI', 'NEWS MEDIA'))
  df['PAYEE'] = df['PAYEE'].apply(lambda x: x if not re.search('\\bJOURNA\\b', x) else x.replace('JOURNA', 'JOURNAL'))
  df['PAYEE'] = df['PAYEE'].apply(lambda x: x if not re.search('\\bCONF\\b', x) else x.replace('CONF', 'CONFERENCE'))
  df['PAYEE'] = df['PAYEE'].apply(lambda x: 'FIRST BANK' if '1ST BANK' in x else x)

  df['PAYEE'] = df.apply(format_names, names=names, axis=1)

  df['PAYEE'] = df['PAYEE'].apply(lambda x: 'DEMOCRATIC PARTY OF ' + x[:x.find('DEMOCRATIC PARTY OF')] if re.search('DEMOCRATIC PARTY OF', x) and x.find('DEMOCRATIC PARTY OF') != 0 else x)
  df['PAYEE'] = df['PAYEE'].apply(lambda x: 'REPUBLICAN PARTY OF ' + x[:x.find('REPUBLICAN PARTY OF')] if re.search('REPUBLICAN PARTY OF', x) and x.find('REPUBLICAN PARTY OF') != 0 else x)
  try:
      df['PAYEE'] = df['PAYEE'].apply(lambda x: 'THE ' + ' '.join(x.split()[:-1]) if len(x.split()) > 2 and x.split()[-1] == 'THE' else x) 
  except:
      pass

  df['PAYEE'] = df.apply(payroll_vendors, axis=1)

  df['PAYEE'] = df['PAYEE'].apply(lambda x: ' '.join(x.split()).strip())

  if 'CATEGORY' in df.columns:
    df['CATEGORY'] = df['CATEGORY'].apply(lambda x: x if type(x) == str else 'UNKNOWN')

  return df

# adds what kind of transaction to each payroll comapny's name
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

# replacing long payee names with shorter versions if keyword found
def replacements(row):
  payee = row['PAYEE'].strip()
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
  elif re.search('\\bCO$', payee) and '&' not in payee:
    return payee[:-2] + 'COMMITTEE'
  elif 'UNITEMIZED' in payee or 'EXPENSES UNDER' in payee or 'EXPENSES OF' in payee or 'MISCELLANEOUS' in payee or re.search('^TOTAL$', payee):
    return 'UNITEMIZED EXPENSE'
  elif 'EXXON' in payee:
    return 'EXXON MOBIL'
  elif 'GO DADDY' in payee or 'GODADDY' in payee:
    return 'GO DADDY'
  elif 'LOWES' in payee or "LOWE'S" in payee:
    return 'LOWES'
  elif re.search('\\bIRS\\b', payee) or 'INTERNAL REVENUE SERVICE' in payee:
    return 'INTERNAL REVENUE SERVICE (IRS)'
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
  elif re.search('\\bADP\\b', payee):
    return 'AUTOMATIC DATA PROCESSING (ADP)'
  elif 'OFFICE MAX' in payee or 'OFFICEMAX' in payee:
    return 'OFFICE MAX'
  elif 'L2' in payee:
    return 'L2 INC'
  elif 'BALANCE TRANSFER' in payee:
    return 'BALANCE TRANSFER TO NEXT ELECTION'
  return payee

# adding state name to state-specific vendors
def add_state_name(row, state_name):
  payee = row['PAYEE']
  if (('DEMOCRATIC' in payee and 'MAJORITY' not in payee) or ('REPUBLICAN' in payee and 'GOVERNOR' not in payee) or re.search('\\bGOP\\b', payee) or 'COUNTY' in payee or 'STATE' in payee) and state_name not in payee and 'RESEARCH' not in payee:
    payee = payee + ' (' + state_name + ')'
  return payee

# general fuzzymatching helper function
def is_fuzzymatched(row, ref_entity, threshold=95, return_ratio=False):
    top_ratio = 0

    if not return_ratio:
      ws = fuzz.ratio(row, ref_entity) >= threshold or fuzz.partial_ratio(row, ref_entity) >= threshold
    
      if ws:
        return True
      else:
        return False
    else:
      top_ratio = max(fuzz.ratio(row, ref_entity), fuzz.partial_ratio(row, ref_entity))
      return top_ratio

# function to fuzzymatch payees depending on threshold
def payee_fuzzymatching(row, payee_references):
  name = row['PAYEE']
  if type(name) == float:
      return 'NOT FOUND'
  if re.search('\(PAYROLL\)|\(PAYROLL TAX\)|\(FEES\)',name):
      return payee
  name = name.strip()
  if name in payee_references: # if row payee is already a reference, we can return early
    return name
  else:

    ref_dict = {}

    for reference in payee_references:
      reference = reference.strip()
      ref_dict[reference] = is_fuzzymatched(name, reference, return_ratio=True) # save reference and max ratio in dictionary
    if len(ref_dict) == 0:
      pass
    else:
      maximum = max(ref_dict, key=ref_dict.get)
      if ref_dict[maximum] > 95:
        return maximum 

    return name.strip()

# function to fuzzymatch categories depending on keywords found
def category_fuzzymatching(row, payee_references, payee_refs_df):
  payee_name = row['PAYEE'].strip()
  curr_category = row['CATEGORY'].strip()
  if payee_name in payee_references:
    return payee_refs_df[payee_refs_df['NAME'] == payee_name]['CATEGORY'].values[0]
  else:
    if re.search('PAYROLL|SALARY|WAGES|COMPENSATION|CHECK|STAFF|WORKER|CONTRACT|ORGANIZER',curr_category) and re.search('TAX',curr_category):
      return 'PAYROLL TAX'
    elif re.search('PAYROLL|SALARY|WAGES|COMPENSATION|CHECK|STAFF|WORKER|CONTRACT|ORGANIZER',curr_category) and re.search('FEE', curr_category):
      return 'SALARY & PAYROLL PROCESSING FEE'
    elif re.search('PAYROLL|SALARY|WAGES|COMPENSATION|CHECK|STAFF|WORKER|CONTRACT|ORGANIZER',curr_category) or re.search('PAYROLL|SALARY|WAGES|COMPENSATION|CHECK|STAFF|WORKER|CONTRACT|ORGANIZER',payee_name):
      return 'SALARY & PAYROLL'
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
    elif re.search('PAYMENT|\\bBANK\\b', curr_category) or re.search('PAYMENT|\\bBANK\\b', payee_name):
      return 'CREDIT CARD PAYMENT & BANK FEES'
    elif re.search('PROCESSING|TRANSACTION|MERCHANT|SERVICE CHARGE', curr_category) or re.search('PROCESSING|TRANSACTION|MERCHANT|SERVICE CHARGE', payee_name):
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
    elif re.search('UNKNOWN|SUPPLIES|SUBSC.*PTION|\\bMISC\\b|MISCELLANEOUS|OTHER|\\bATM\\b|CAMPAIGN RELATED|LOAN|OFFSET|HARDWARE|FLORIST|SHOP|MARKET|DRUG|PHARMACY|^TOTAL$', curr_category) or re.search('UNKNOWN|SUPPLIES|SUBSC.*PTION|\\bMISC\\b|MISCELLANEOUS|OTHER|\\bATM\\b|CAMPAIGN RELATED|LOAN|OFFSET|HARDWARE|FLORIST|SHOP|MARKET|DRUG|PHARMACY|^TOTAL$', payee_name):
      return 'GENERAL OPERATIONS & SUPPLIES'

    try:
      if (re.search(row['CANDIDATE/COMMITTEE NAME'].replace('\\', '').replace('"', '').replace('(', '\(').replace(')', '\)').strip(), curr_category.replace('\\', '').replace('"', '').strip()) or re.search(row['CANDIDATE/COMMITTEE NAME'].replace('\\', '').replace('"', '').strip(), payee_name.replace('\\', '').replace('"', '').strip())) or (re.search('REIMBURSE', curr_category) or re.search('REIMBURSE', payee_name)):
        return 'REIMBURSEMENT'
    except:
      pass

    return 'GENERAL OPERATIONS & SUPPLIES'

pd.options.mode.chained_assignment = None

if __name__ == '__main__':

  # initialize category dataframe with name, category, amount

  category_df = pd.DataFrame(data=None, columns=['STATE', 'CANDIDATE', 'OFFICE', 'DISTRICT', 'CATEGORY', 'AMOUNT'])

  # initialize vendor dataframe with name, vendor, amount

  payee_df = pd.DataFrame(data=None, columns=['STATE', 'CANDIDATE', 'OFFICE', 'DISTRICT', 'PAYEE', 'AMOUNT'])

  # initialize overall dataframe with name, vendor, amount

  total_df = pd.DataFrame(data=None, columns=['STATE', 'CANDIDATE', 'OFFICE', 'DISTRICT', 'TOTAL AMOUNT'])

  # get name of state -> abbreviation

  us_state_abbrev = {
      'Alabama': 'AL',
      'Alaska': 'AK',
      'American Samoa': 'AS',
      'Arizona': 'AZ',
      'Arkansas': 'AR',
      'California': 'CA',
      'Colorado': 'CO',
      'Connecticut': 'CT',
      'Delaware': 'DE',
      'District of Columbia': 'DC',
      'Florida': 'FL',
      'Georgia': 'GA',
      'Guam': 'GU',
      'Hawaii': 'HI',
      'Idaho': 'ID',
      'Illinois': 'IL',
      'Indiana': 'IN',
      'Iowa': 'IA',
      'Kansas': 'KS',
      'Kentucky': 'KY',
      'Louisiana': 'LA',
      'Maine': 'ME',
      'Maryland': 'MD',
      'Massachusetts': 'MA',
      'Michigan': 'MI',
      'Minnesota': 'MN',
      'Mississippi': 'MS',
      'Missouri': 'MO',
      'Montana': 'MT',
      'Nebraska': 'NE',
      'Nevada': 'NV',
      'New Hampshire': 'NH',
      'New Jersey': 'NJ',
      'New Mexico': 'NM',
      'New York': 'NY',
      'North Carolina': 'NC',
      'North Dakota': 'ND',
      'Northern Mariana Islands':'MP',
      'Ohio': 'OH',
      'Oklahoma': 'OK',
      'Oregon': 'OR',
      'Pennsylvania': 'PA',
      'Puerto Rico': 'PR',
      'Rhode Island': 'RI',
      'South Carolina': 'SC',
      'South Dakota': 'SD',
      'Tennessee': 'TN',
      'Texas': 'TX',
      'Utah': 'UT',
      'Vermont': 'VT',
      'Virgin Islands': 'VI',
      'Virginia': 'VA',
      'Washington': 'WA',
      'West Virginia': 'WV',
      'Wisconsin': 'WI',
      'Wyoming': 'WY'
  }

  # initialize cleaned data path and candidate query file

  processed_data_path = '../Processed_Data/'

  candidate_query = pd.read_csv('../References/STATE_CANDIDATE_QUERY.csv')

  payee_overall_refs_df = pd.read_csv('../References/OVERALL_REFERENCES.csv')

  payee_state_refs_df = pd.read_csv('../References/STATE_REFERENCES.csv')

  category_refs_df = pd.read_csv('../References/CATEGORIES.csv')
  category_refs = list(category_refs_df['CATEGORY'].unique())

  names = pd.read_csv('../References/NAMES.csv')
  names = list(names['NAME'])


  # loop through all states to centralize information

  for file in os.listdir(processed_data_path):
      if file[0] != '.' and not re.search('ALASKA|KANSAS', file): # incomplete data for Alaska and Kansas
          try:
              state_name = state_name = re.search('.+?(?=_PROCESSED.csv)', file).group(0)
              state_abbrev = us_state_abbrev[state_name.title()]
              print('On ', state_name, '\n')

              state_file = pd.read_csv(processed_data_path + file)

              category_df = pd.DataFrame(data=None, columns=['STATE', 'CANDIDATE', 'OFFICE', 'DISTRICT', 'CATEGORY', 'AMOUNT'])
              payee_df = pd.DataFrame(data=None, columns=['STATE', 'CANDIDATE', 'OFFICE', 'DISTRICT', 'PAYEE', 'AMOUNT'])
              total_df = pd.DataFrame(data=None, columns=['STATE', 'CANDIDATE', 'OFFICE', 'DISTRICT', 'TOTAL AMOUNT'])

              if 'CATEGORY' not in state_file.columns:
                  state_file['CATEGORY'] = 'GENERAL OPERATIONS & SUPPLIES'
              
              # quick fixes
              state_file['CATEGORY'] = state_file.apply(lambda x: 'MEDIA & ADS' if x['PAYEE'] == 'COMCAST' else x['CATEGORY'], axis=1)
              state_file['CANDIDATE/COMMITTEE NAME'] = state_file['CANDIDATE/COMMITTEE NAME'].apply(lambda x: x.replace('"', '').replace("'", ''))    
              state_file['DATE'] = state_file['DATE'].apply(lambda x: str(x))

              state_file['PAYEE'] = state_file['PAYEE'].apply(lambda x: 'NOT FOUND' if type(x) == float else x)
              state_file['PAYEE'] = state_file['PAYEE'].apply(lambda x: x.replace('&QUOT', '').replace('GRASROTS', 'GRASSROOTS'))
              state_file['PAYEE'] = state_file['PAYEE'].apply(lambda x: x[:x.index('(')] if x == 'DEMOCRATIC RESEARCH INSITUTE' else x)
              state_file['PAYEE'] = state_file['PAYEE'].apply(lambda x: 'THE SUMMIT GROUP' if re.search('\\bSUMMIT\\b', x) else x)       
              state_file['PAYEE'] = state_file['PAYEE'].apply(lambda x: 'UNITED STATES POSTAL SERVICE/POSTMASTER (USPS)' if re.search('POSTMASTER', x) else x)
              state_file['PAYEE'] = state_file['PAYEE'].apply(lambda x: 'TOSKR' if re.search('TOSKR', x) else x)

              # state-specific fixes
              if state_name == 'ILLINOIS':
                  state_file['PAYEE'] = state_file['PAYEE'].apply(lambda x: 'DEMOCRATIC PARTY OF ILLINOIS' if re.search('ILLINOIS DEMOCRATIC PARTY', x) else x)  
              elif state_name == 'VIRGINIA':
                  state_file['PAYEE'] = state_file['PAYEE'].apply(lambda x: 'TREASURER OF VIRGINIA' if re.search('TREASURER', x) else x)
              elif state_name == 'ALABAMA':
                  state_file['PAYEE'] = state_file['PAYEE'].apply(lambda x: 'ALABAMA REPUBLICAN PARTY' if re.search('ALGOP|AL G.?O.?P|ALABAMA G.?O.?P', x) else x)

              # candidate query by state name
              state_query = candidate_query[candidate_query['STATE'] == state_abbrev]
              state_candidates = list(state_query['NAME'].values)
              state_candidates = [cand.strip() for cand in state_candidates]
              state_candidates = list(set(state_candidates))

              payee_curr_state_refs_df = payee_state_refs_df[payee_state_refs_df['STATE'] == state_name]
              payee_curr_state_refs_df.drop(columns=['STATE'], inplace=True)

              payee_golden_df = payee_overall_refs_df.append(payee_curr_state_refs_df)
              payee_refs = list(payee_golden_df['NAME'].unique())

              for candidate_name in state_candidates:
                  # get the candidate df of expenses
                  associated_query = '^' + state_query[state_query['NAME'] == candidate_name]['ASSOCIATED QUERY'].values[0]
                  cand_df = state_file[state_file['CANDIDATE/COMMITTEE NAME'].str.contains(associated_query, na=False, regex=True)]
                  cand_df.drop_duplicates(subset=['PAYEE', 'AMOUNT', 'DATE'], inplace=True)
                  cand_df.dropna(subset=['DATE'], inplace=True)

                  cand_df['PAYEE'] = cand_df['PAYEE'].apply(lambda x: x if type(x) == str else 'NOT FOUND')
                  cand_df['PAYEE'] = cand_df.apply(add_state_name, state_name=state_name, axis=1)

                  office = state_query[state_query['NAME'] == candidate_name]['OFFICE'].values[0]
                  district = state_query[state_query['NAME'] == candidate_name]['DISTRICT'].values[0]
                  
                  # get the total amount information
                  election_cycle = candidate_query[candidate_query['NAME'] == candidate_name]['ELECTION YEAR'].values[0]
                  if np.isnan(election_cycle):
                      election_cycle = 0
                  else:
                      election_cycle = int(election_cycle)
                  election_cycle_regex = str(election_cycle - 1) + '$|' + str(election_cycle) + '$'
                  cand_df = cand_df[cand_df['DATE'].str.contains(election_cycle_regex, na=False)] # here is where you segment by year

                  #cleaning
                  cand_df['PAYEE'] = cand_df.apply(replacements,axis=1)
                  cand_df = main_cleaning(cand_df, names)
                  cand_df['PAYEE'] = cand_df.apply(payee_fuzzymatching, payee_references=payee_refs, axis=1)
                  cand_df['CATEGORY'] = cand_df.apply(category_fuzzymatching, payee_references=payee_refs, payee_refs_df=payee_golden_df, axis=1)

                  total_amount = cand_df['AMOUNT'].sum() 
                  curr_row = {'STATE':state_name, 'CANDIDATE':candidate_name, 'OFFICE':office, 'DISTRICT':district,'TOTAL AMOUNT':total_amount}
                  total_df = total_df.append(curr_row, ignore_index=True)

                  not_found_payee_dict = {'STATE':state_name, 'CANDIDATE':candidate_name, 'OFFICE':office, 'DISTRICT':district,'PAYEE':0, 'AMOUNT':0}
                  not_found_category_dict = {'STATE':state_name, 'CANDIDATE':candidate_name, 'OFFICE':office, 'DISTRICT':district,'CATEGORY':0, 'AMOUNT':0}
              
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
                              if fuzz.ratio(payee, cand_payee) >= 95 or fuzz.ratio(payee.replace(' ', ''), cand_payee.replace(' ', '')) >= 85 or \
                                  fuzz.token_set_ratio(payee.replace(' ', ''), cand_payee.replace(' ', '')) >= 95 or fuzz.token_set_ratio(payee, cand_payee) >= 95:
                                  duplicate = True
                                  cand_df['PAYEE'] = cand_df['PAYEE'].apply(lambda x: cand_payee if x == payee else x)
                                  break
                          if not duplicate:
                              cand_top_payees.append(payee)
                          if len(cand_top_payees) == 10:
                              break

                      payee_counter = 0
                      
                      # collect 10 payees, write a row each time
                      for i in range(len(cand_top_payees)):
                          payee_amount = cand_df[cand_df['PAYEE'] == cand_top_payees[i]]['AMOUNT'].sum()
                          if cand_top_payees[i] in ['NOT FOUND', 'UNITEMIZED EXPENSE', 'UNKNOWN', 'ITEMS 100 OR LESS']:
                              cand_top_payees[i] = 'UNITEMIZED EXPENSE/UNKNOWN'
                          payee_dict = {'STATE':state_name, 'CANDIDATE':candidate_name, 'OFFICE':office, 'DISTRICT':district,'PAYEE':cand_top_payees[i], 'AMOUNT':payee_amount}
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
                          category_dict = {'STATE':state_name, 'CANDIDATE':candidate_name, 'OFFICE':office, 'DISTRICT':district,'CATEGORY':cand_top_categories[i], 'AMOUNT':category_amount}
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

              print("Saving state file...")
              total_df.to_csv('../Cleaned_State_Data/' +state_name+'_TOTAL.csv', index=None)
              payee_df.to_csv('../Cleaned_State_Data/' +state_name+'_PAYEE.csv', index=None)
              category_df.to_csv('../Cleaned_State_Data/' +state_name+'_CATEGORY.csv', index=None)
          except:
              print('-------------------------------------------------------------')
              print("Problem with ", file)
              print('-------------------------------------------------------------')
              pass

  # add office to dataframe
  def get_office(row):
      name = row['CANDIDATE']
      office = candidate_query[candidate_query['NAME'] == name]['OFFICE'].values[0]
      return office

  # add district to dataframe
  def get_district(row):
      name = row['CANDIDATE']
      district = candidate_query[candidate_query['NAME'] == name]['DISTRICT'].values[0]
      if district == '':
          district = 0
      return district

  total_df = pd.DataFrame(data=None, columns=['STATE', 'CANDIDATE', 'OFFICE', 'DISTRICT', 'TOTAL AMOUNT'])
  payee_df = pd.DataFrame(data=None, columns=['STATE', 'CANDIDATE',  'OFFICE', 'DISTRICT', 'PAYEE', 'AMOUNT'])
  category_df = pd.DataFrame(data=None, columns=['STATE', 'CANDIDATE',  'OFFICE', 'DISTRICT', 'CATEGORY', 'AMOUNT'])

  final_data_path = '../Cleaned_State_Data/'

  for file in os.listdir(final_data_path):
      if file[0] != '.' and not re.search('ALASKA|KANSAS|LOUISIANA', file):
          print(file)
          curr_file = pd.read_csv(final_data_path + file)
          curr_file['OFFICE'] = curr_file.apply(get_office, axis=1)
          curr_file['DISTRICT'] = curr_file.apply(get_district, axis=1)
          if 'TOTAL' in file:
              total_df = total_df.append(curr_file)
          elif 'PAYEE' in file:
              payee_df = payee_df.append(curr_file)
          else:
              category_df = category_df.append(curr_file)

  total_df.to_csv('../Final_Data/STATE_TOTAL_DF.csv', index=None)
  payee_df.to_csv('../Final_Data/STATE_PAYEE_DF.csv', index=None)
  category_df.to_csv('../Final_Data/STATE_CATEGORY_DF.csv', index=None)
          

