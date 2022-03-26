from dataclasses import replace
from venv import create
import pandas
import glob
import os
import hashlib
import sqlite3
import json

# A unique MD5 hash is created based on the csv file paths present in the input folder, so each set of input files creates a unique hash. Caveat: files with different content but identical names cannot be distinguished. 
data_hash = ''

# For each replacepattern key, the patterns listed in 'pattern' will be searched in the fields listed in 'field', and the contents of that entire field where a match is found will be replaced with the value of key. 
replacepatterns = dict()

# Each key is a tag, which is assigned to a transaction based on the values in the 'find' list, if found in one of the fields listed in 'field'
tagsmap = dict()

# CSV files for each provider should be placed in the input directory, in a directory with a name that matches the key of the provide in this dict. Subdirectories can be used for separate accounts.
providers = dict()

# Load config.json and providers.json
def load_configs():
    global replacepatterns
    global tagsmap
    global providers
    if not os.path.isfile('config.json'):
        print('geen config.json')
    else:
        with open('config.json', 'r') as cf:
            config = json.load(cf)
            try:
                tagsmap = config['tagsmap']
            except:
                print("Geen tagsmap in config")
            try:
                replacepatterns = config['replacepatterns']
            except:
                print("Geen replacepatterns in config")
    
    if not os.path.isfile('config.json'):
        print('geen providers.json')
        exit()
    with open('providers.json', 'r') as pf:
        providers = json.load(pf)

# Load csv files from input dir into a dataframe and return the combined dataframe.
def load_files():
    frames = []
    global data_hash
    global providers
    filenames = []

    for b,c in providers.items():
        subdirs = [x[0] for x in os.walk('input/' + b)]
        for sd in subdirs:
            if os.path.isdir(sd):
                account = sd.replace("input/","").replace("/","_")
                for csvfile in glob.glob( sd + '/*.csv'):
                    filenames.append(b + '_' + csvfile)
                    print("loading from: "+csvfile)
                    df = pandas.read_csv(csvfile, index_col=False, sep=c['sep'], parse_dates=c['date_cols'], dayfirst=True, usecols=lambda x: x in c['cols'].keys())
                    df.rename(columns = c['cols'], inplace = True)
                    for bc, cn in c['cols'].items():
                        if cn not in df.columns:
                            df[cn] = ''
                    df['bank'] = b
                    df['account'] = account
                    df['filename'] = csvfile

                    frames.append(df)
    data_hash = hashlib.md5(''.join(filenames).encode("utf-8")).hexdigest()

    parsed = pandas.concat(frames)
    parsed.fillna('', inplace=True)

    return parsed

# Process the dataframe, add tags and set/modify extra fields.
def parse_frame(df):
    global tagsmap
    df.sort_values(by=['datum'], inplace=True)
    df['mededeling'] = df.apply(set_mededeling, axis = 1)
    df['tags'] = df.apply(set_tags, axis = 1)
    df['details'] = df['details'].apply(clean_spaces)
    df = df.apply(standardize_values, axis = 1)
    return df

# Replace 'mededeling' with 'mededeling_struct' if empty.
def set_mededeling(row):
    if row['mededeling'].strip():
        return row['mededeling']
    if row['mededeling_struct'].strip():
        return row['mededeling_struct']
    return ''

# Set tags column based on the rules in tagsmap.
def set_tags(row):
    global tagsmap
    tags = []
    for t, tprops in tagsmap.items():
        for f in tprops['find']:
            for field in tprops['field']:
                if f.lower() in str(row[field]).lower():
                    tags.append(t)
    tags = list(set(tags))
    return ','.join(tags)

# Simplify fields based on replacepatterns.
def standardize_values(row):
    global replacepatterns
    # tegenpartij = row['tegenpartij']
    # details = row['details']
    for k, rp in replacepatterns.items():
        for p in rp['pattern']:
            for f in rp['field']:
                if p.lower() in str(row[f]).lower():
                    row[f] = k
    return row

# Export the dataframe to YNAB compatible csv's, per account.
def create_ynab_files(df):

    allowed_cols = ['Date', 'Payee', 'Memo', 'Amount']

    global outputdir

    if not os.path.isdir(outputdir):
        os.makedirs(outputdir)
    
    accounts = list(set(list(df['account'])))


    for acc in accounts:
        if  not acc.startswith('paypal'):
            ynab = df[df['account'] == acc].copy()
            ynab.rename(columns = {'datum':'Date','tegenpartij':'Payee' }, inplace = True)
            ynab['Memo'] = ynab.apply(create_memo, axis = 1)
            ynab['Amount'] = ynab.apply(create_amount, axis = 1)
            ynab['Payee'] = ynab.apply(fill_payee, axis = 1)

            for c in ynab.columns:
                if c not in allowed_cols:
                    del ynab[c]

            ynab.to_csv(outputdir + '/ynab_'+acc+'.csv',sep=',',index=False)

# Fill payee with relevant info if empty
def fill_payee(row):
    payee = row['Payee'].strip()
    if not payee:
        if row['tegenpartij_rek'].strip():
            return row['tegenpartij_rek']
    else:
        return payee
    return 'onbekend'

# Format amount to YNAB compatible standard.
def create_amount(row):
    return '€' + str(row['bedrag']).replace(',','.')

# Prefix details field with bank (provider) to create memo field
def create_memo(row):
    return row['bank'] + ' - ' + row['details']

# Replace multiple sequential spaces with dash.
def clean_spaces(s):
    while '   ' in s:
        s = s.replace('   ', '  ')
    s = s.replace('  ', ' - ')
    return s.strip()

# Dump the dataframe to a sqlite database for review and manual analysis.
def create_db(df):
    global outputdir

    if not os.path.isdir(outputdir):
        os.makedirs(outputdir)
    conn = sqlite3.connect(outputdir+'/budget.sqlite')
    df.to_sql('betalingen',conn,if_exists='replace')
    viewsql = "CREATE VIEW uitgave_per_tag AS "
    viewclauses = []
    tags_sorted = [t for t in  tagsmap.keys()]
    tags_sorted.sort()
    for t in tags_sorted:
        viewclauses.append("SELECT '"+t+"' as tag, SUM(bedrag) as bedrag from betalingen where tags like '%"+t+"%'")
    viewsql = viewsql + " UNION ".join(viewclauses)
    cur = conn.cursor()
    cur.execute("DROP view if exists uitgave_per_tag")
    # Create view
    cur.execute(viewsql)

load_configs()

all = load_files()

outputdir = 'output/'+data_hash

parsed  = parse_frame(all)

print(parsed)

create_db(parsed)
create_ynab_files(parsed)