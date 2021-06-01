import pandas as pd
import re

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

abbrev_us_state = dict(map(reversed, us_state_abbrev.items()))

us_abbrevs_title = {abbrev.title(): state for abbrev, state in abbrev_us_state.items()}

def replace_state_abbrev_with_name(payee_name):
    if type(payee_name) == str:
        for abbrev in us_abbrevs_title.keys():
            abbrev_regex = '\\b' + abbrev + '\\b'
            if re.search(abbrev_regex, payee_name) and re.search('Republican|Democrat|Party|Caucus|House|Senate|State', payee_name):
                payee_name = re.sub(abbrev_regex, us_abbrevs_title[abbrev], payee_name)
                return payee_name
    return payee_name

def wrangle_payees(df):
    wrangled_payee_columns = ['STATE', 'CANDIDATE', 'OFFICE', 'DISTRICT']

    for i in range(1, 11):
        wrangled_payee_columns.append('PAYEE ' + str(i))
        wrangled_payee_columns.append('PAYEE AMOUNT ' + str(i))

    wrangled_payees = pd.DataFrame(data=None, columns=wrangled_payee_columns)

    for candidate in df['CANDIDATE'].unique():
        candidate_df = df[df['CANDIDATE'] == candidate]

        state, candidate, office, district = candidate_df['STATE'].values[0], candidate_df['CANDIDATE'].values[0], candidate_df['OFFICE'].values[0], candidate_df['DISTRICT'].values[0]
        payee_amounts = candidate_df.groupby('PAYEE').sum().sort_values(by='AMOUNT', ascending=False).reset_index()
        #payees = candidate_df['PAYEE'].values.tolist()
        #amounts = candidate_df['AMOUNT'].values.tolist()
        cand_dict = {'STATE':state, 'CANDIDATE':candidate, 'OFFICE':office, 'DISTRICT':district}

        for i in range(10):
            try:
                cand_dict['PAYEE ' + str(i + 1)] = payee_amounts.iloc[i]['PAYEE']
                cand_dict['PAYEE AMOUNT ' + str(i + 1)] = payee_amounts.iloc[i]['AMOUNT']
            except:
                cand_dict['PAYEE ' + str(i + 1)] = 0
                cand_dict['PAYEE AMOUNT ' + str(i + 1)] = 0

        wrangled_payees = wrangled_payees.append(cand_dict, ignore_index=True)

    return wrangled_payees

def wrangle_categories(df):
    wrangled_category_columns = ['STATE', 'CANDIDATE', 'OFFICE', 'DISTRICT']

    for i in range(1, 11):
        wrangled_category_columns.append('CATEGORY ' + str(i))
        wrangled_category_columns.append('CATEGORY AMOUNT ' + str(i))

    wrangled_categories = pd.DataFrame(data=None, columns=wrangled_category_columns)

    for candidate in df['CANDIDATE'].unique():
        candidate_df = df[df['CANDIDATE'] == candidate]

        state, candidate, office, district = candidate_df['STATE'].values[0], candidate_df['CANDIDATE'].values[0], candidate_df['OFFICE'].values[0], candidate_df['DISTRICT'].values[0]
        categories = candidate_df['CATEGORY'].values.tolist()
        amounts = candidate_df['AMOUNT'].values.tolist()
        cand_dict = {'STATE':state, 'CANDIDATE':candidate, 'OFFICE':office, 'DISTRICT':district}

        for i in range(10):
            cand_dict['CATEGORY ' + str(i + 1)] = categories[i]
            cand_dict['CATEGORY AMOUNT ' + str(i + 1)] = amounts[i]

        wrangled_categories = wrangled_categories.append(cand_dict, ignore_index=True)
    
    return wrangled_categories

if __name__ == '__main__':
    
    federal_total = pd.read_csv('./Federal/Final_Data/FEDERAL_TOTAL_DF.csv')
    federal_payees = pd.read_csv('./Federal/Final_Data/FEDERAL_PAYEE_DF.csv')
    federal_categories = pd.read_csv('./Federal/Final_Data/FEDERAL_CATEGORY_DF.csv')

    state_total = pd.read_csv('./State/Final_Data/STATE_TOTAL_DF.csv')
    state_payees = pd.read_csv('./State/Final_Data/STATE_PAYEE_DF.csv')
    state_categories = pd.read_csv('./State/Final_Data/STATE_CATEGORY_DF.csv')

    wrangled_federal_payees = wrangle_payees(federal_payees)
    wrangled_federal_categories = wrangle_categories(federal_categories)

    wrangled_state_payees = wrangle_payees(state_payees)
    wrangled_state_categories = wrangle_categories(state_categories)

    federal = federal_total.merge(wrangled_federal_payees, on=['STATE', 'CANDIDATE', 'OFFICE', 'DISTRICT']).merge(wrangled_federal_categories, on=['STATE', 'CANDIDATE', 'OFFICE', 'DISTRICT'])

    state = state_total.merge(wrangled_state_payees, on=['STATE', 'CANDIDATE', 'OFFICE', 'DISTRICT']).merge(wrangled_state_categories, on=['STATE', 'CANDIDATE', 'OFFICE', 'DISTRICT'])

    state['STATE'] = state['STATE'].apply(lambda x: us_state_abbrev[x.title()])

    federal = federal.applymap(lambda s:s.title() if type(s) == str else s)

    state = state.applymap(lambda s:s.title() if type(s) == str else s)

    federal['STATE'] = federal['STATE'].apply(lambda x: x.upper())
    state['STATE'] = state['STATE'].apply(lambda x: x.upper())

    final = pd.concat([federal, state])

    # quick fixes
    payee_columns = [col for col in final.columns if 'PAYEE' in col and 'AMOUNT' not in col]
    for col in payee_columns:
        final[col] = final[col].apply(lambda x: str(x))
        final[col] = final[col].apply(lambda x: x.replace('Georgia Warnock For', 'Warnock for Georgia').replace('Actblue', 'ActBlue').replace('Al Media', 'AL Media').replace('Gmmb', 'GMMB').replace('Us Bank', 'US Bank').replace('Onmessage', 'OnMessage').replace('On Message', 'OnMessage').\
            replace('At&T', 'AT&T').replace('At&t', 'AT&T').replace('Dccc', 'DCCC').replace('Dnc', 'DNC').replace('Nrcc', 'NRCC').replace('Rnc', 'RNC').\
                replace('Pol Action', 'Political Action').replace('Cmdi', 'CMDI').replace('Amplifyai', 'Amplify.ai').replace('Cfo', 'CFO').replace('Nm', 'NM').replace('Akpd', 'AKPD').replace('Fp1', 'FP1').replace('Lgm', 'LGM').replace('Ccm', 'CCM').replace('Dna', 'DNA').replace('Lvh', 'LVH').replace('Ps', 'PS').replace("'S", "'s").replace('Fls', 'FLS').\
                    replace('Bb&T', 'BB&T').replace('Bb&t', 'BB&T').replace('Jbw', 'JBW').replace('Pcms', 'PCMS').replace('Jsd', 'JSD').replace('Akm', 'AKM').replace('Mdi', 'MDI').replace('Scm', 'SCM').replace('Asp', 'ASP').replace('Toskr', 'TOSKR').replace('Kb', 'KB').replace('Mpi', 'MPI').replace('Ln', 'LN').replace('Jpm', 'JPM').replace('Imge', 'IMGE').replace('Hrcc', 'HRCC').replace('Hr', 'HR').replace('Chs', 'CHS').replace('Jd', 'JD').\
                        replace('Smcc', 'SMCC').replace('Wsfa', 'WSFA').replace('Cmg', 'CMG').replace('Jj', 'JJ'))
        final[col] = final[col].apply(lambda x: x.replace('Irs', 'IRS') if '(Irs)' in x else x)
        final[col] = final[col].apply(lambda x: x.replace('Usps', 'USPS') if '(Usps)' in x else x)
        final[col] = final[col].apply(lambda x: x.replace('Us', 'US') if '(Us)' in x else x)
        final[col] = final[col].apply(lambda x: x.replace('Adp', 'ADP') if '(Adp)' in x else x)
        final[col] = final[col].apply(lambda x: x.replace('Gps', 'GPS') if '(Gps)' in x else x)
        final[col] = final[col].apply(lambda x: x[:x.find('Mc') + 2] + x[x.find('Mc') + 2:x.find('Mc') + 3].upper() + x[x.find('Mc') + 3:] if 'Mc' in x else x)
        final[col] = final[col].apply(lambda x: re.sub('\\bGop\\b', 'GOP', x) if re.search('\\bGop\\b', x) else x)
        final[col] = final[col].apply(lambda x: re.sub('\\bLp\\b', 'LP', x) if re.search('\\bLp\\b', x) else x)
        final[col] = final[col].apply(lambda x: re.sub('\\bLlp\\b', 'LLP', x) if re.search('\\bLlp\\b', x) else x)
        final[col] = final[col].apply(lambda x: re.sub('\\bLlc\\b', 'LLC', x) if re.search('\\bLlc\\b', x) else x)
        final[col] = final[col].apply(lambda x: re.sub('\\bLc\\b', 'LC', x) if re.search('\\bLc\\b', x) else x)
        final[col] = final[col].apply(lambda x: re.sub('\\bPac\\b', 'PAC', x) if re.search('\\bPac\\b', x) else x)
        final[col] = final[col].apply(lambda x: re.sub('\\bAb\\b', 'AB', x) if re.search('\\bAb\\b', x) else x)
        final[col] = final[col].apply(lambda x: re.sub('\\bGbao\\b', 'GBAO', x) if re.search('\\bGbao\\b', x) else x)
        final[col] = final[col].apply(lambda x: 'Groundswell Public Strategies Inc (GPS Impact)' if re.search('Gps Impact|Gpsimpact', x) else x)
        final[col] = final[col].apply(lambda x: 'BrabenderCox' if re.search('Brabendercox|Brabender Cox|Brabender & Cox', x) else x)
        final[col] = final[col].apply(lambda x: 'AL Media (Adelstein Liston)' if re.search('Al Media|(Al) Media Strategy|Al Media|Al Strateg', x) else x)
        final[col] = final[col].apply(lambda x: 'United States Postal Service/Postmaster (USPS)' if re.search('Postmaster|Usps|Uspo', x) else x)
        final[col] = final[col].apply(lambda x: 'WPA Intelligence (WPAI)' if re.search('\\bWpai\\b|\\bWpa\\b', x) else x)
        final[col] = final[col].apply(lambda x: 'The UPS Store' if re.search('\\bUps\\b', x) else x)
        final[col] = final[col].apply(lambda x: 'Bank of America' if re.search('America Bank Of|Bofa', x) else x)
        final[col] = final[col].apply(lambda x: 'AX Media' if re.search('Ax Media|Axmedia', x) else x)
        final[col] = final[col].apply(lambda x: 'Citizens Election Fund (CEF)' if re.search('\\bCef\\b|(Cef)', x) else x)
        final[col] = final[col].apply(lambda x: 'Citi' if re.search('\\bCiti\\b|\\bCiticard\\b', x) else x)
        
        final[col] = final[col].apply(lambda x: x.upper() if '-Tv' in x or 'Ngp' in x else x)
        final[col] = final[col].apply(lambda x: 'No ranked payee found' if re.search('\\b0\\b', x) else x)
        final[col] = final[col].apply(replace_state_abbrev_with_name)

    category_columns = [col for col in final.columns if 'CATEGORY' in col and 'AMOUNT' not in col]
    for col in category_columns:
        final[col] = final[col].apply(lambda x: str(x))
        final[col] = final[col].apply(lambda x: x.replace('Pr', 'PR') if type(x) == str and re.search('\\bPr\\b', x) else x)
        final[col] = final[col].apply(lambda x: 'No ranked category found' if re.search('\\b0\\b', x) else x)
        
    final['CANDIDATE'] = final['CANDIDATE'].apply(lambda x: x[:x.find('Mc') + 2] + x[x.find('Mc') + 2:x.find('Mc') + 3].upper() + x[x.find('Mc') + 3:] if 'Mc' in x else x)

    final = final[final['TOTAL AMOUNT'] != '0']

    final.reset_index(inplace=True)

    final.to_csv('final.csv', index=None)

