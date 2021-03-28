'''
0. Initial Prep 
'''

'''
A. Required Modules
'''

import yfinance as yf
import pandas as pd
import os
import xlwings as xw
import numpy as np
import re
from pathlib import Path
from datetime import datetime

'''
B. Dictionary of sample utility companies 

The sample includes companies from FERC's CAPM estimate in Opinion 569 (Docket No. EL14-12-004).
Those companies which are commented out were included in FERC's sample, but as of March, 2021 
are no longer available on Yahoo Finance.
'''

'''
When updating the below dictionary, for QA purposes, it is required that the exact text 
for the company name as found on Yahoo Finance is specified as the dictionary value. 

For example, the following code can be used to return the company name for Xcel Energy:
    ticker = 'XEL'
    tdat = yf.Ticker(ticker)
    info = tdat.info
    longName = info.get('longName')
    print(longName)
    output: Xcel Energy Inc.
The dictionary entry for this company is thus 'XEL': 'Xcel Energy Inc.'  
'''

company_dict_input = {
 'AEE': 'Ameren Corporation',
 'AEP': 'American Electric Power Company, Inc.',
 'ALE': 'ALLETE, Inc.',
 'AVA': 'Avista Corporation',
 'BKH': 'Black Hills Corporation',
 'CMS': 'CMS Energy Corporation',
 'CNP': 'CenterPoint Energy, Inc.',
 'D': 'Dominion Energy, Inc.',
 'DTE': 'DTE Energy Company',
 'DUK': 'Duke Energy Corporation',
 'ED': 'Consolidated Edison, Inc.',
#'EDE':'Empire District Electric Co.',
#'EE':'El Paso Electric Co.'
 'EIX': 'Edison International',
 'ES': 'Eversource Energy',
 'ETR': 'Entergy Corporation',
 'EXC': 'Exelon Corporation',
 'FE': 'FirstEnergy Corp.',
#'GXP':'Great Plains Energy Inc.',
 'IDA': 'IDACORP, Inc.',
#'ITC':'ITC Holdings Corp',
 'LNT': 'Alliant Energy Corporation',
 'NEE': 'NextEra Energy, Inc.',
 'NWE': 'NorthWestern Corporation',
 'OGE': 'OGE Energy Corp.',
 'OTTR': 'Otter Tail Corporation',
 'PCG': 'PG&E Corporation',
 'PEG': 'Public Service Enterprise Group Incorporated',
 'PNM': 'PNM Resources, Inc.',
 'PNW': 'Pinnacle West Capital Corporation',
 'POR': 'Portland General Electric Company',
 'PPL': 'PPL Corporation',
#'SCG':'SCANA Corp.',
 'SO': 'The Southern Company',
 'SRE': 'Sempra Energy',
#'TE':'TECO Energy, Inc.',
#'UIL':'UIL Holdings Corp.',
#'VVC':'Vectren Corp.',
#'WR':'Westar Energy, Inc.',
 'XEL': 'Xcel Energy Inc.'
}


'''
C. Create final list of companies to include in the analysis
'''

'''
1. Create dictionary of companies to include in the sample
'''

company_dict = {}

for ticker, name in company_dict_input.items():
    try:
        tdat = yf.Ticker(ticker)
        info = tdat.info
        longName = info.get('longName')
        
        if name.strip() == longName.strip():
            company_dict.update({ticker: longName})
        else: 
            print('Expected company name for Ticker: '+ticker+' does not match downloaded ticker info')
            print('Expected Name: '+name+', Downloaded Name: '+longName)
            print("Type 'Delete' to exclude this company from analysis, otherwise press ENTER to leave in the sample")
            cofilt = input()
            print()
            
            if cofilt.upper() != 'DELETE':
                company_dict.update({ticker: longName})
            else:
                """
                Do nothing
                """
                
    except:
        print('Ticker for '+ticker+", "+name+" not found, will exclude from analysis")
        print()

'''
2. Create list of companies which are included in the sample
'''

co_list = company_dict.keys()
companies = ' '.join(co_list)

'''
I. Bring in the stock price data 
'''

'''
A. Combine Utility Company & NYSE prices
'''

'''
1. Download utility company & NYSE prices 
'''

data0 = yf.download(companies, period='max')
nya = yf.download('^NYA', period='max')


'''
2. Assign Multi-level index to NYSE table
'''

nya.columns = pd.MultiIndex.from_product([nya.columns,['NYA']])

'''
3. Combine utility company & NYSE prices: only where there is a match between dates
    which prevents mismatches between the datasets 
'''

prices = data0.merge(nya, how='inner', on='Date')

'''
B. Further prep the prices data
'''

'''
1. Keep only the Adjusted Close prices
'''

prices = prices['Adj Close']

'''
2. Take the datepart from date
'''

prices.reset_index(inplace=True)
prices['DatePart'] = prices['Date'].dt.date
del prices['Date']
prices = prices.rename(columns={'DatePart':'Date'})

'''
3. Rearrange columns
'''

#First rename NYA 
prices.rename(columns={'^NYA':'NYA'}, inplace=True)

co_list = prices.columns.tolist()
co_list.remove('Date')
co_list.remove('NYA')
col_list = ['Date','NYA'] + co_list
prices = prices[col_list]

'''
II. Assign weekday to prices data and assign imputed prices
    > Beta calcs will be based on weekly prices from each Friday
    > Imputed prices are where the daily stock price is from the
      latest available trade date
'''

'''
A. Create weekdates series
'''

'''
1. Get Min/Max date from prices data 
'''

min_date = prices['Date'].min()
max_date = prices['Date'].max()


'''
2. Create weekdates series based on prices Min/Max date
'''

dates_index = pd.date_range(start = min_date, end = max_date, freq = 'D')
dates = pd.DataFrame(index = dates_index)
dates.reset_index(inplace = True)
dates = dates.rename(columns={'index':'DatePart'})
dates['Date'] = dates['DatePart'].dt.date
#weekday assigns 0 for Monday through 6 for Sunday
dates['Weekday'] = dates['DatePart'].dt.weekday
del dates['DatePart']

'''
B. Assign imputed stock prices
'''

'''
1. Combine the dates table with the stock prices table
'''

prices = prices.assign(In_Prices = 1, Close_Price_Date = prices['Date'])
prices_table = dates.merge(prices, how='left', on='Date')

'''
2. Identify records for which will assign an imputed price
'''

prices_table.loc[prices_table['In_Prices'].isna(), 'Imputed_Price'] = 1
prices_table.loc[prices_table['In_Prices'] == 1,  'Imputed_Price'] = 0
del prices_table['In_Prices']

'''
3. Fill in the imputed prices
'''

fill_list = co_list + ['NYA', 'Close_Price_Date']
prices_table[fill_list] = prices_table[fill_list].fillna(method='ffill')

'''
C. Create the final prices tables, including only Friday prices
'''

'''
1. Rearrange columns
'''

col_list2 = ['Date','Weekday','Close_Price_Date','Imputed_Price','NYA'] + co_list
prices_table = prices_table[col_list2]

'''
2. Select Friday prices
'''

prices_table_fri0 = prices_table.loc[prices_table['Weekday'] == 4]

'''
3. Create list of the companies where have at least 261 Friday prices
   which is needed to calculate 260 return observations, i.e. 5 years of return.  
'''

'''
a. Count the number of obs in each column
'''

prices_fri_count = pd.DataFrame(prices_table_fri0.count(), columns=['Count'])
prices_fri_count.reset_index(inplace = True)
prices_fri_count = prices_fri_count.rename(columns={'index':'Column'})

'''
b. Subset to those companies where have at least 261 price obs
'''

prices_fri_count_sub = prices_fri_count[(prices_fri_count['Count']>=261) & (prices_fri_count['Column'].isin(co_list))]
prices_fri_count_sub.insert(0,'index',1,)

'''
c. Take the data wide 
'''

prices_fri_count_sub_wd = prices_fri_count_sub.pivot(index='index',columns='Column',values='Count')

'''
d. Create the list of companies with at least 260 return obs 
'''

co_list_260 = prices_fri_count_sub_wd.columns.tolist()

'''
4. Create the final prices table, selecting only those companies with at least 260 return obs
'''

ptf_columns = ['Date','Weekday','Close_Price_Date','Imputed_Price','NYA'] + co_list_260

prices_table_fri = prices_table_fri0[ptf_columns]

'''
III. Inital prep of output excel file
'''

'''
A. Create new workbook 
'''

wb = xw.Book()


'''
B. Save prices table to workbook
'''

ws_ps = wb.sheets.add('Prices')
ws_ps.range('A1').value = prices_table
ws_ps.range('A:A').api.Delete()

'''
C. Create worksheet that holds Beta calculations
'''

wb.sheets.add('Beta Calcs')
ws_bc = wb.sheets['Beta Calcs']
ws_bc.range('A1').value = prices_table_fri
ws_bc.range('A:A').api.Delete()

'''
IV. Calculate Beta and Adjusted Beta for each company in the Beta Calcs worksheet
'''

'''
A. Assign NYSE return series
'''

last_row = ws_bc.range(1,1).end('down').row

#Insert Return Series
ws_bc.range('F:F').api.Insert()
ws_bc.range('F1').value = 'NYA_Return'
ws_bc.range('F3').value = '=(E3-E2)/E2'
ws_bc.range('F3').api.AutoFill(ws_bc.range('F3:F{last_row}'.format(last_row=last_row)).api,0)

#Insert Deviation from Mean Series
ws_bc.range('G:G').api.Insert()
ws_bc.range('G1').value = 'NYA_Deviation'
ws_bc.range('G262').value = '=F262 - AVERAGE(F3:F262)'
ws_bc.range('G262').api.AutoFill(ws_bc.range('G262:G{last_row}'.format(last_row=last_row)).api,0)

'''
B. For each company, calculate Beta and Adjusted Beta 
'''

last_column_num = ws_bc.range(1,1).end('right').column
last_column_name = ws_bc.range(1,last_column_num).value

'''
\D+ matches one or more characters that is not a numeric digit from 0 to 9
'''
LettersRegex = re.compile(r'\D+')

while last_column_name != 'NYA_Deviation':
    #Series address row & column keys
    series_address = ws_bc.range(1,last_column_num).get_address(0,0)
    series_col = ''.join(LettersRegex.findall(series_address))
    series_colnum = ws_bc.range(series_address).column
    
    #Create Column Headers for the series
    ticker = ws_bc.range(1,series_colnum).value
    ws_bc.range(1,series_colnum+1).value = ticker + '_Return'
    ws_bc.range(1,series_colnum+2).value = ticker + '_Beta'
    ws_bc.range(1,series_colnum+3).value = ticker + '_Adj_Beta'
    ws_bc.range(1,series_colnum+4).value = ticker + '_Deviation'
    
    return_col = ''.join(LettersRegex.findall(ws_bc.range(1,series_colnum+1).get_address(0,0)))
    beta_col = ''.join(LettersRegex.findall(ws_bc.range(1,series_colnum+2).get_address(0,0)))
    adj_beta_col = ''.join(LettersRegex.findall(ws_bc.range(1,series_colnum+3).get_address(0,0)))   
    dev_col = ''.join(LettersRegex.findall(ws_bc.range(1,series_colnum+4).get_address(0,0)))  
    
    if ws_bc.range(2,series_colnum).value is None:
        firstobs_address = ws_bc.range(series_address).end('down')
        firstobs_row = ws_bc.range(firstobs_address).row
        
    else:
        firstobs_address = ws_bc.range(2,series_colnum)
        firstobs_row = 2
        
    #Calculate weekly return 
    return_formula = "=({col}{r1}-{col}{r0})/{col}{r0}".format(col=series_col,r1=firstobs_row+1,r0=firstobs_row)
    return_range = "{return_col}{rstart}:{return_col}{last_row}".format(return_col=return_col,rstart=firstobs_row+1,last_row=last_row) 
    ws_bc.range(firstobs_row+1,series_colnum+1).value = return_formula

    ws_bc.range(firstobs_row+1,series_colnum+1).api.AutoFill(ws_bc.range(return_range).api,0)
    
    '''
    Only perform the following calculations if there are at least 260 return observations. 
    Thinking this filter is not strictly necessary because already applied a similar filter above, 
    but it doesnt hurt to leave another level of QA. 
    '''
    if last_row - firstobs_row >= 260:
        #Calculate Beta
        beta_formula = "=SLOPE({return_col}{r1}:{return_col}{r260},F{r1}:F{r260})".format(return_col=return_col,r1=firstobs_row+1,r260=firstobs_row+260)
        beta_range = "{beta_col}{rstart}:{beta_col}{last_row}".format(beta_col=beta_col,rstart=firstobs_row+260,last_row=last_row)
        ws_bc.range(firstobs_row+260,series_colnum+2).value = beta_formula
        ws_bc.range(firstobs_row+260,series_colnum+2).api.AutoFill(ws_bc.range(beta_range).api,0)

        #Calculate Adjusted Beta 
        adj_beta_formula = "=(2/3)*{beta_col}{rstart}+1/3".format(beta_col=beta_col,rstart=firstobs_row+260)
        adj_beta_range = "{adj_beta_col}{rstart}:{adj_beta_col}{last_row}".format(adj_beta_col=adj_beta_col,rstart=firstobs_row+260,last_row=last_row)
        ws_bc.range(firstobs_row+260,series_colnum+3).value = adj_beta_formula
        ws_bc.range(firstobs_row+260,series_colnum+3).api.AutoFill(ws_bc.range(adj_beta_range).api,0)
        
        #Calculate Deviation from Mean
        dev_formula = "={return_col}{r260} - AVERAGE({return_col}{r1}:{return_col}{r260})".format(return_col=return_col,r1=firstobs_row+1,r260=firstobs_row+260)
        dev_range = "{dev_col}{rstart}:{dev_col}{last_row}".format(dev_col=dev_col,rstart=firstobs_row+260,last_row=last_row)
        ws_bc.range(firstobs_row+260,series_colnum+4).value = dev_formula
        ws_bc.range(firstobs_row+260,series_colnum+4).api.AutoFill(ws_bc.range(dev_range).api,0)
        
    #If adjacent series is not NYA, insert four blank columns
    if ws_bc.range(1,series_colnum-1).value != 'NYA_Deviation':
        for i in range(4):
            ws_bc.range("{col}:{col}".format(col=series_col)).insert(shift='right') 
            
        #Reassign last column name/number
        last_column_num = ws_bc.range(1,1).end('right').column
        last_column_name = ws_bc.range(1,last_column_num).value
        
    else:
        last_column_name = 'NYA_Deviation'
    

'''
C. Calculate average beta, adjusted beta, utility return, and deviaiton across the sample company columns
'''

'''
1. Add the three blank columns with the relevant headers to the right of NYA_Deviation
'''

#Add four blank columns 
for i in range(4):
    ws_bc.range("H:H").insert(shift='right') 

#Add column headers
ws_bc.range(1,8).value = 'Average_Beta'
ws_bc.range(1,9).value = 'Average_Adj_Beta'
ws_bc.range(1,10).value = 'Average_Utility_Return'
ws_bc.range(1,11).value = 'Average_Deviation'



'''
2. Provide the calculated columns
'''

last_column_num = ws_bc.range(1,1).end('right').column

def avgform(txt,add_c):
    '''
    Get list of the relevant columns
    '''
    cols = []
    firstrow = []
    
    for company in co_list:
        for i in range(last_column_num):
            if company + txt == ws_bc.range(1,i+1).value:
                cols.append(''.join(LettersRegex.findall(ws_bc.range(1,i+1).get_address(0,0))))
                firstrow.append(ws_bc.range(1,i+1).end('down').row)
                break
                
    '''
    Id first row on which to apply formula
    '''
    rowstart = min(firstrow)
      
    '''
    Create Formula Text
    ''' 
    crange = [i + str(rowstart) for i in cols]
    
    fmttxt = ', '.join(crange)
    avg = '=AVERAGE(' + fmttxt + ')' 
    
    '''
    Add forumla to excel sheet
    '''
    ws_bc.range(rowstart,7+add_c).value = avg
    
    '''
    Fill Down
    '''
    col = ''.join(LettersRegex.findall(ws_bc.range(1,7+add_c).get_address(0,0)))
    frange = "{col}{rowstart}:{col}{last_row}".format(col=col,rowstart=rowstart,last_row=last_row)
    ws_bc.range(rowstart,7+add_c).api.AutoFill(ws_bc.range(frange).api,0)
    
    
avgform('_Beta',1)
avgform('_Adj_Beta',2)
avgform('_Return',3)
avgform('_Deviation',4)

'''
V. Create Chart of Average Beta vs Average Adjusted Beta
'''

'''
A. Return Beta Calcs worksheet as data frame
'''

betas_df = ws_bc.range('A1').expand().options(pd.DataFrame).value

'''
B. Create dataframe for chart data
'''

chart_data_df = betas_df.loc[betas_df['Average_Beta'].notna()]
chart_data_df = chart_data_df[['Average_Beta','Average_Adj_Beta']]

'''
C. Output chart data to new worksheet
'''

#Add new worksheet
wb.sheets.add('Chart')
ws_cd = wb.sheets['Chart']

#Export the Chart Data
ws_cd.range('A1').value = chart_data_df


'''
D. Create the Chart
'''

beta_chart = ws_cd.charts.add(left=200,top=50,width=500,height=300)

last_row = ws_cd.range(1,1).end('down').row
beta_chart.set_source_data(ws_cd.range('Chart!A1:C{last_row}'.format(last_row=last_row)))

beta_chart.chart_type = 'line'
beta_chart.api[1].SetElement(2)  # Place chart title at the top
beta_chart.api[1].ChartTitle.Text = 'Average Beta vs. Average Adjusted Beta'


'''
VI. Create summary stats worksheet
'''

'''
A. Add new worksheet and columns headers
'''

#Add new worksheet
wb.sheets.add('Summary Stats')
ws_ss = wb.sheets['Summary Stats']

#Add column headers
ws_ss.range('A1').value = 'Company'
ws_ss.range('B1').value = 'Ticker'
ws_ss.range('C1').value = 'Price Series Start Date'
ws_ss.range('D1').value = 'Beta Series Start Date'
ws_ss.range('E1').value = 'Average Beta'
ws_ss.range('F1').value = 'Average Adjusted Beta'
ws_ss.range('1:1').api.Font.Bold = True


'''
B. Create the summary data
'''

#Doesnt hurt to reassign here
last_column_num = ws_bc.range(1,1).end('right').column

row = 2

company_dict2 = {ticker : (ticker in co_list_260) for ticker in company_dict}

for ticker in company_dict2:
    
    #Fill in Company Name & Ticker
    ws_ss.range(row,1).value = company_dict[ticker]
    ws_ss.range(row,2).value = ticker
    
    #If have 260 return obs, get info from Beta Calcs worksheet
    if company_dict2[ticker]==1: 
        for i in range(last_column_num):
            if ticker == ws_bc.range(1,i+1).value:

                global prices_r1, beta_r1, beta_col2, adj_beta_col2

                series_colnum = ws_bc.range(1,i+1).column
                if ws_bc.range(2,series_colnum).value is None:
                    prices_r1 = ws_bc.range(1,series_colnum).end('down').row
                else:
                    prices_r1 = 2
                beta_r1 = ws_bc.range(1,series_colnum+2).end('down').row 
                beta_col2 = ''.join(LettersRegex.findall(ws_bc.range(1,series_colnum+2).get_address(0,0)))
                adj_beta_col2 = ''.join(LettersRegex.findall(ws_bc.range(1,series_colnum+3).get_address(0,0)))
                break
    
        #Price Series Start Formula
        ws_ss.range(row,3).value = "='Beta Calcs'!C{prices_r1}".format(prices_r1=prices_r1)
    
        #Beta Series Start Formula
        ws_ss.range(row,4).value = "='Beta Calcs'!C{beta_r1}".format(beta_r1=beta_r1)
    
        #Average Beta/Adj Beta Formula
        ws_ss.range(row,5).value = "=AVERAGE('Beta Calcs'!{beta_col}:{beta_col})".format(beta_col=beta_col2)
        ws_ss.range(row,6).value = "=AVERAGE('Beta Calcs'!{adj_beta_col}:{adj_beta_col})".format(adj_beta_col=adj_beta_col2)
      
    #Otherwise indicate if dont have enough return obs
    else:
        ws_ss.range(row,3).value = "NA - less than 260 weekly return observations"
    
    row = row + 1 


'''
VII. Format the workbook
'''

'''
Auto-Fit columns & rows in each worksheet
'''

def auto_fit(ws):
    ws.autofit()
    
auto_fit(ws_bc)
auto_fit(ws_cd)
auto_fit(ws_ps)
auto_fit(ws_ss)

print('Program Finished')
