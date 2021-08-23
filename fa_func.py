import sqlite3
from sqlite3 import Error
from yahoo_fin import stock_info as si
import pandas as pd
import numpy as np
from tqdm.notebook import tqdm
import datetime
import time
import requests
from bs4 import BeautifulSoup as bs
from pandas_datareader import data as pdr


tickers = si.tickers_sp500()
sql_db_name_def = 'stocks'

#get only digits
def get_digits(word):
    if type(word) == type(123.21):
        return word
    elif type(word) == type(123):
        return word
    elif word == '':
        return 0
    only_digits = ''
    for i in range(0, len(word)):
        letter = word[i]
        if word[i].isdigit() or word[i]=='-':
            only_digits = only_digits + word[i]
            
    return int(only_digits)


#make a column in df with a rank of another column
def column_rank(df, column_name):
    max_r = np.percentile(df[column_name], 95)
    min_r = df[df[column_name] > 0][column_name].min()

    for index, row in df.iterrows():
        cur_pe = df.loc[index, column_name]
        if cur_pe > max_r:
            cur_pe = max_r
    
        if cur_pe != min_r:
            rank = (cur_pe - min_r)/(max_r-min_r)
        else:
            rank = 0
        df.loc[index, f'{column_name} Rank'] = rank

#create a new sql_db with a certain name
def create_sql_db(sql_db_name):
    def create_connection(sql_db_name):
        """ create a database connection to a SQLite database """
        try:
            conn = sqlite3.connect(sql_db_name)
            print(sqlite3.version)
        except Error as e:
            print(e)
        finally:
            if conn:
                conn.close()
    
    create_connection(f"{sql_db_name}.sqlite")
        
    create_tables(sql_db_name)

        
#create a table in a specific sqldb with certain parametres
def create_tables(sql_db_name = sql_db_name_def, const_list=False):
    
    conn = sqlite3.connect(f'{sql_db_name}.sqlite')
    cur = conn.cursor()
    
    #table Financials creating
    cur.execute(f'''DROP TABLE IF EXISTS Financials''')
    cur.execute(f'''CREATE TABLE Financials (
                id	INTEGER NOT NULL UNIQUE,
                Ticker	TEXT,
                P_Date	TEXT,
                Revenue	REAL,
                RevTTM	REAL,
                SPS	REAL,
                NetIncome	REAL,
                NetIncomeTTM	REAL,
                EPS	REAL,
                Shares	REAL,
                UNIQUE("Ticker","P_Date"),
                PRIMARY KEY("id" AUTOINCREMENT)
                )''')
    
    print('Table Financials has been created')
    
    #table Multiples creating
    cur.execute(f'''DROP TABLE IF EXISTS Multiples''')
    cur.execute(f'''CREATE TABLE Multiples (
                id	INTEGER NOT NULL UNIQUE,
                Ticker	TEXT,
                Date	TEXT,
                PE	REAL,
                PE_Rank	REAL,
                PS	REAL,
                PS_Rank	REAL,
                RevGrowthAnnual5	REAL,
                RevGrowthAnnual10	REAL,
                RevGrowthAnnual15	REAL,
                IncomeGrowthAnnual5	REAL,
                IncomeGrowthAnnual10	REAL,
                IncomeGrowthAnnual15	REAL,
                PEG5	REAL,
                PEG10	REAL,
                PEG15	REAL,
                UNIQUE("Ticker","Date"),
                PRIMARY KEY("id" AUTOINCREMENT)
                )''')
    
    print('Table Multiples has been created')
    
    #table Prices creating
    cur.execute(f'''DROP TABLE IF EXISTS Prices''')
    cur.execute(f'''CREATE TABLE Prices (
                Ticker	TEXT,
                MD_Date	TEXT,
                Open	REAL,
                High	REAL,
                Low	REAL,
                Close	REAL,
                UNIQUE("Ticker","MD_Date")
                )''')
    
    print('Table Prices has been created')
    
    conn.close()
    

#update daily stock prices
def update_prices(sql_db_name = sql_db_name_def, n=0):
    
    conn = sqlite3.connect(f'{sql_db_name}.sqlite')
    cur = conn.cursor()
    
    #import tickers
    tickers = si.tickers_sp500()
    tickers = [item.replace(".", "-") for item in tickers] # Yahoo Finance uses dashes instead of dots
    index_name = '^GSPC' # S&P 500
    end_date = datetime.date.today()
    
    #go through all tickers to get market data
    for ticker in tickers[n:]:
        #show current position
        position = tickers.index(ticker)
        print(f'Got ticker {ticker}, element {position}')
    
        #get the last saved data of this ticker
        cur.execute('SELECT * FROM Prices WHERE ticker = ? ORDER BY Prices.MD_Date DESC LIMIT 1', (ticker, ))
        query_list = cur.fetchall()
        if query_list == []:
            start_date = datetime.date.today() - datetime.timedelta(days = 25*365)
        else:
            start_date = datetime.datetime.strptime(query_list[0][1], '%Y-%m-%d').date() + datetime.timedelta(days = 1)
        if start_date == datetime.date.today():
            print('all the data has already been saved')
            continue
    
        #get the market data since the last saved date
        fh = pdr.get_data_yahoo(ticker, start_date, end_date)    
    
        #save to sql db with cool loading bar
        for i in tqdm(range(fh.shape[0])): #fh.shape[0]
            date, o, h, l, c = fh.index[i].strftime("%Y-%m-%d"), fh['Open'][i], fh['High'][i], fh['Low'][i], fh['Close'][i]
            cur.execute('''INSERT OR REPLACE INTO Prices (Ticker, MD_Date, Open, High, Low, Close)
                        VALUES (?,?,?,?,?,?)''', (ticker, date, o, h, l, c, ))
            conn.commit()
        print(f'{ticker} is done', datetime.datetime.now(), '\n')
        time.sleep(1)
    
    
    print('-------------------------')
    print('EVERYTHIN IS DONE')
    print('-------------------------')
    
    conn.close()



#update stock financials using macrotrends
def update_financials_macrotrends(sql_db_name = sql_db_name_def, n=0):
    
    conn = sqlite3.connect(f'{sql_db_name}.sqlite')
    cur = conn.cursor()
    tickers = si.tickers_sp500()
    tickers = [item.replace("-", ".") for item in tickers]
    index_name = '^GSPC' # S&P 500
    end_date = datetime.date.today()
    
    for ticker in tickers[n:]:
        try:
            #show current position
            position = tickers.index(ticker)
            print(f'Got ticker {ticker}, element {position}')
            df = pd.DataFrame(columns=['Date', 'Revenue', 'Income', 'Shares'])

            r = requests.get(f"https://www.macrotrends.net/stocks/charts/{ticker}")
            url = r.url
            time.sleep(1)

            #get the revenue data since the last saved date
            r = requests.get(f"{url}revenue")
            time.sleep(1)
            bs_r = bs(r.content, 'html.parser')
            for row in bs_r.find_all('table', attrs={'class':'historical_data_table table'})[1].find('tbody').find_all('tr'):
                date, rev = row.find_all('td')
                date, rev = date.get_text(), rev.get_text()
                new_row = pd.Series({'Date':date, "Revenue":rev,"Income":'',"Shares":''})
                df = df.append(new_row, ignore_index=True)

            #set dates as index
            df.set_index('Date', inplace=True)

            #get the income data since the last saved date
            r = requests.get(f"{url}net-income")
            time.sleep(1)
            bs_i = bs(r.content, 'html.parser')
            for row in bs_i.find_all('table', attrs={'class':'historical_data_table table'})[1].find('tbody').find_all('tr'):
                date, inc = row.find_all('td')
                date, inc = date.get_text(), inc.get_text()
                df.loc[date, 'Income'] = inc

            #get the shares data since the last saved date
            r = requests.get(f"{url}shares-outstanding")
            time.sleep(1)
            bs_s = bs(r.content, 'html.parser')
            for row in bs_s.find_all('table', attrs={'class':'historical_data_table table'})[1].find('tbody').find_all('tr'):
                date, sh = row.find_all('td')
                date, sh = date.get_text(), sh.get_text()
                df.loc[date, 'Shares'] = sh

            ticker = ticker.replace(".", "-")

            for i in tqdm(range(df.shape[0])): #fh.shape[0]
                date, r, n, s = df.index[i], get_digits(df['Revenue'][i]), get_digits(df['Income'][i]), get_digits(df['Shares'][i])
                cur.execute('''INSERT OR IGNORE INTO Financials (Ticker, P_Date, Revenue, NetIncome, Shares)
                            VALUES (?,?,?,?,?)''', (ticker, date, r, n, s, ))
                conn.commit()
            print(f'{ticker} is done', datetime.datetime.now(), '\n')
        except:
            print(f'Something went wrong with {ticker}', format)
    
    print("------------------------------------------")
    print("EVERYTHIN IS DONE")
    print("------------------------------------------")
    
    conn.close()


def update_financials_macrotrends_with_ticker(ticker, sql_db_name = sql_db_name_def, n=0):
    
    conn = sqlite3.connect(f'{sql_db_name}.sqlite')
    cur = conn.cursor()
    
    df = pd.DataFrame(columns=['Date', 'Revenue', 'Income', 'Shares'])

    r = requests.get(f"https://www.macrotrends.net/stocks/charts/{ticker}")
    url = r.url
    time.sleep(1)

    #get the revenue data since the last saved date
    r = requests.get(f"{url}revenue")
    time.sleep(1)
    bs_r = bs(r.content, 'html.parser')
    for row in bs_r.find_all('table', attrs={'class':'historical_data_table table'})[1].find('tbody').find_all('tr'):
        date, rev = row.find_all('td')
        date, rev = date.get_text(), rev.get_text()
        new_row = pd.Series({'Date':date, "Revenue":rev,"Income":'',"Shares":''})
        df = df.append(new_row, ignore_index=True)

    #set dates as index
    df.set_index('Date', inplace=True)

    #get the income data since the last saved date
    r = requests.get(f"{url}net-income")
    time.sleep(1)
    bs_i = bs(r.content, 'html.parser')
    for row in bs_i.find_all('table', attrs={'class':'historical_data_table table'})[1].find('tbody').find_all('tr'):
        date, inc = row.find_all('td')
        date, inc = date.get_text(), inc.get_text()
        df.loc[date, 'Income'] = inc

    #get the shares data since the last saved date
    r = requests.get(f"{url}shares-outstanding")
    time.sleep(1)
    bs_s = bs(r.content, 'html.parser')
    for row in bs_s.find_all('table', attrs={'class':'historical_data_table table'})[1].find('tbody').find_all('tr'):
        date, sh = row.find_all('td')
        date, sh = date.get_text(), sh.get_text()
        df.loc[date, 'Shares'] = sh

    ticker = ticker.replace(".", "-")

    for i in tqdm(range(df.shape[0])): #fh.shape[0]
        date, r, n, s = df.index[i], get_digits(df['Revenue'][i]), get_digits(df['Income'][i]), get_digits(df['Shares'][i])
        cur.execute('''update Financials set Revenue=?, NetIncome=?, Shares=? where P_Date=? and Ticker=?''', (r, n, s, date,ticker, ))
        conn.commit()
    print(f'{ticker} is done', datetime.datetime.now(), '\n')
    
    print("------------------------------------------")
    print("EVERYTHIN IS DONE")
    print("------------------------------------------")
    
    conn.close()
    
    
#return relevant stock P/E rank    
def get_stock_rank(ticker, sql_db_name = sql_db_name_def):
    
    conn = sqlite3.connect(f'{sql_db_name}.sqlite')
    cur = conn.cursor()
    
    query = cur.execute('''select MD_Date as Date, Ticker, close, ttm, shares, (ttm/Shares) as eps, (close/ttm*Shares) as pe
    from
    (select Ticker, MD_Date, max(P_Date), close, ttm, Shares
    FROM
    (select
    t1.Ticker,
    t1.MD_Date,
    t1.Close as close,
    t2.P_Date,
    sum(t2.NetIncome) over(partition by t1.Ticker, t1.MD_Date order by t2.P_Date rows BETWEEN  3 PRECEDING AND CURRENT ROW) as ttm,
    first_value(t2.Shares) OVER ( PARTITION by t1.Ticker, t1.MD_Date order by t2.P_Date DESC) as Shares
    from Prices as t1, Financials as t2
    where t1.Ticker = t2.Ticker and t2.P_Date <= t1.MD_Date and t1.Ticker = ?
    order by t1.MD_Date DESC, t2.P_Date DESC)
    group by Ticker,MD_Date)
    order by Date DESC''', (ticker,)).fetchall()

    df = pd.DataFrame(query, columns=['Date', 'Ticker', 'Close', 'TTM Income', 'Number of shares', 'EPS', 'P/E' ])
    df.set_index('Date', inplace=True)
    df = df.sort_index()
    
    conn.close()
    
    max_r = np.percentile(df['P/E'], 95)
    min_r = df['P/E'].min()

    for index, row in df.iterrows():
        cur_pe = df.loc[index, 'P/E']
        if cur_pe > max_r:
            cur_pe = max_r
    
        if cur_pe != min_r:
            rank = (cur_pe - min_r)/(max_r-min_r)
        else:
            rank = 0
        df.loc[index, 'P/E Rank'] = rank
    
    print('Current P/E Rank is', int(df.tail(1)['P/E Rank'].values[0]*100))
    
    return df['P/E Rank'].plot()
    
#return stock P/E chart    
def get_stock_pe_chart(ticker, sql_db_name = sql_db_name_def):
    
    conn = sqlite3.connect(f'{sql_db_name}.sqlite')
    cur = conn.cursor()
    
    query = cur.execute('''select MD_Date as Date, Ticker, close, ttm, shares, (ttm/Shares) as eps, (close/ttm*Shares) as pe
    from
    (select Ticker, MD_Date, max(P_Date), close, ttm, Shares
    FROM
    (select
    t1.Ticker,
    t1.MD_Date,
    t1.Close as close,
    t2.P_Date,
    sum(t2.NetIncome) over(partition by t1.Ticker, t1.MD_Date order by t2.P_Date rows BETWEEN  3 PRECEDING AND CURRENT ROW) as ttm,
    first_value(t2.Shares) OVER ( PARTITION by t1.Ticker, t1.MD_Date order by t2.P_Date DESC) as Shares
    from Prices as t1, Financials as t2
    where t1.Ticker = t2.Ticker and t2.P_Date <= t1.MD_Date and t1.Ticker = ?
    order by t1.MD_Date DESC, t2.P_Date DESC)
    group by Ticker,MD_Date)
    order by Date DESC''', (ticker,)).fetchall()

    df = pd.DataFrame(query, columns=['Date', 'Ticker', 'Close', 'TTM Income', 'Number of shares', 'EPS', 'P/E' ])
    df.set_index('Date', inplace=True)
    df = df.sort_index()
    
    conn.close()
    chart = df['P/E'].plot()
    
    return df, chart


#populate table "Multiples" with calculated multipliers
#numbers are not 100% right due to 95% decile
#force "recalculate all" to obtain the most accurate data
def populate_multiples(sql_db_name = sql_db_name_def, n=0, recalculate_all=False):
    
    conn = sqlite3.connect(f'{sql_db_name}.sqlite')
    cur = conn.cursor()
    
    for ticker in tickers[n:]:
        
        position = tickers.index(ticker)
        print(f'Got ticker {ticker}, element {position}')
        
        query = cur.execute('''select Ticker, MD_Date as Date, max(P_Date) as PeportMonth,
                            CASE WHEN SPS=0 THEN 0 ELSE Close/SPS END as PS,
                            CASE WHEN EPS=0 THEN 0 ELSE Close/EPS END as PE
                            from (select
                            t1.Ticker,
                            t1.MD_Date,
                            t1.Close,
                            t2.P_Date,
                            first_value(t2.RevTTM) over(partition by t2.P_Date order by t2.P_Date DESC) as RevTTM,
                            first_value(t2.SPS) over(partition by t2.P_Date order by t2.P_Date DESC) as SPS,
                            first_value(t2.NetIncomeTTM) over(partition by t2.P_Date order by t2.P_Date DESC) as NetIncomeTTM,
                            first_value(t2.EPS) over(partition by t2.P_Date order by t2.P_Date DESC) as EPS
                            from Prices as t1, Financials as t2
                            where t1.Ticker = t2.Ticker and t2.P_Date <= t1.MD_Date and t1.Ticker = ?
                            order by t1.MD_Date DESC, t2.P_Date DESC)
                            group by MD_Date
                            order by MD_Date desc''', (ticker,)).fetchall()
    
        if query == []:
            print(f'No data for {ticker}', '\n')
            continue
    
        df_columns = ['Ticker', 'Date', 'PeportMonth', 'P/S', 'P/E']
        df = pd.DataFrame(query, columns=df_columns)
        df.set_index('Date', inplace=True)

        column_rank(df, 'P/S')
        column_rank(df, 'P/E')
        
        switcher = "REPLACE" if recalculate_all is True else "IGNORE"
        
        for i in tqdm(range(df.shape[0])): #fh.shape[0]
            date, pe, per, ps, pes = df.index[i], df['P/E'][i], df['P/E Rank'][i], df['P/S'][i], df['P/S Rank'][i]
            cur.execute(f'''INSERT OR {switcher} INTO Multiples (Ticker, Date, PE, PE_Rank, PS, PS_Rank)
                            VALUES (?,?,?,?,?,?)''', (ticker, date, pe, per, ps, pes))
            conn.commit()
    
        print(f'{ticker} is done', datetime.datetime.now(), '\n')
    
    print("------------------------------------------")
    print("EVERYTHIN IS DONE")
    print("------------------------------------------")
    
    conn.close()
    
#populate table "Multiples" with specific ticker
def populate_multiples_with_ticker(ticker, sql_db_name = sql_db_name_def, n=0):
    
    conn = sqlite3.connect(f'{sql_db_name}.sqlite')
    cur = conn.cursor()
        
    print(f'Got ticker {ticker}')

    query = cur.execute('''select Ticker, MD_Date as Date, max(P_Date) as PeportMonth,
                        CASE WHEN SPS=0 THEN 0 WHEN SPS < 0 THEN 0 ELSE Close/SPS END as PS,
                        CASE WHEN EPS=0 THEN 0 WHEN EPS < 0 THEN 0 ELSE Close/EPS END as PE
                        from (select
                        t1.Ticker,
                        t1.MD_Date,
                        t1.Close,
                        t2.P_Date,
                        first_value(t2.RevTTM) over(partition by t2.P_Date order by t2.P_Date DESC) as RevTTM,
                        first_value(t2.SPS) over(partition by t2.P_Date order by t2.P_Date DESC) as SPS,
                        first_value(t2.NetIncomeTTM) over(partition by t2.P_Date order by t2.P_Date DESC) as NetIncomeTTM,
                        first_value(t2.EPS) over(partition by t2.P_Date order by t2.P_Date DESC) as EPS
                        from Prices as t1, Financials as t2
                        where t1.Ticker = t2.Ticker and t2.P_Date <= t1.MD_Date and t1.Ticker = ?
                        order by t1.MD_Date DESC, t2.P_Date DESC)
                        group by MD_Date
                        order by MD_Date desc''', (ticker, )).fetchall()

    if query == []:
        print(f'No data for {ticker}', '\n')
        conn.close()
        return

    df_columns = ['Ticker', 'Date', 'PeportMonth', 'P/S', 'P/E']
    df = pd.DataFrame(query, columns=df_columns)
    df.set_index('Date', inplace=True)

    column_rank(df, 'P/S')
    column_rank(df, 'P/E')

    for i in tqdm(range(df.shape[0])): #fh.shape[0]
        date, pe, per, ps, pes = df.index[i], df['P/E'][i], df['P/E Rank'][i], df['P/S'][i], df['P/S Rank'][i]
        cur.execute('''update Multiples set PS=?, PS_Rank=?, PE=?, PE_Rank=? where Ticker=? and Date=?''', (ps, pes, pe, per, ticker, date, ))
        conn.commit()

    print(f'{ticker} is done', datetime.datetime.now(), '\n')
    
    print("------------------------------------------")
    print("EVERYTHIN IS DONE")
    print("------------------------------------------")
    
    conn.close()


#populate table "Financials" with TTM values
def populate_TTM(n=0, sql_db_name = sql_db_name_def, only_null=False):
    
    conn = sqlite3.connect(f'{sql_db_name}.sqlite')
    cur = conn.cursor()
    
    for ticker in tickers:
        position = tickers.index(ticker)
        print(f'Got ticker {ticker}, element {position}')
        
        if only_null == False:
            query = cur.execute('''select id, Ticker, P_Date, Revenue, Revttm, 
                case when Revttm <= 0 then 0 else Revttm/Shares end as SPS,
                NetIncome, NIttm,
                case when NIttm <= 0 then 0 else NIttm/Shares end as EPS
                from (select
                id,
                Ticker,
                P_Date,
                Revenue,
                sum(Revenue) over(order by P_Date rows BETWEEN  3 PRECEDING AND CURRENT ROW) as Revttm,
                sum(Revenue) over(order by P_Date rows BETWEEN  3 PRECEDING AND CURRENT ROW) / Shares as SPS,
                NetIncome,
                sum(NetIncome) over(order by P_Date rows BETWEEN  3 PRECEDING AND CURRENT ROW) as NIttm,
                sum(NetIncome) over(order by P_Date rows BETWEEN  3 PRECEDING AND CURRENT ROW) / Shares as EPS,
                Shares
                from Financials
                where Ticker = ?
                order by P_Date DESC)
            ''', (ticker,)).fetchall()
        else:
            query = cur.execute('''select id, Ticker, P_Date, Revenue, Revttm,
                case when Revttm <= 0 then 0 else Revttm/Shares end as SPS, NetIncome, NIttm,
                case when NIttm <= 0 then 0 else NIttm/Shares end as EPS
                from
                (select
                id,
                Ticker,
                P_Date,
                Revenue,
                sum(Revenue) over(order by P_Date rows BETWEEN  3 PRECEDING AND CURRENT ROW) as Revttm,
                sum(Revenue) over(order by P_Date rows BETWEEN  3 PRECEDING AND CURRENT ROW) / Shares as SPS,
                NetIncome,
                sum(NetIncome) over(order by P_Date rows BETWEEN  3 PRECEDING AND CURRENT ROW) as NIttm,
                sum(NetIncome) over(order by P_Date rows BETWEEN  3 PRECEDING AND CURRENT ROW) / Shares as EPS,
                RevTTM as rev_def,
                SPS as sps_def,
                NetIncomeTTM as nittm_def,
                EPS as eps_def
                from Financials
                where Ticker = ?
                order by P_Date DESC)
                where rev_def is null or sps_def is NULL or nittm_def is null or eps_def is null
            ''', (ticker,)).fetchall()
    
        df_columns = ['id', 'Ticker', 'Date', 'Revenue', 'Revttm', 'SPS', 'NetIncome','NIttm', 'EPS']
        df = pd.DataFrame(query, columns=df_columns)
        df.set_index('Date', inplace=True)

        for i in tqdm(range(df.shape[0])): #fh.shape[0]
            date, rev_t, sps, inc_t, eps, id_r = df.index[i], df['Revttm'][i], df['SPS'][i], df['NIttm'][i], df['EPS'][i], int(df['id'][i])

            cur.execute('''UPDATE Financials SET RevTTM =:rev_t, SPS=:sps, NetIncomeTTM=:inc_t, EPS=:eps
                            WHERE id=:id_r''', dict(rev_t=rev_t, sps=sps, inc_t=inc_t, eps=eps, ticker=ticker, Date=date, id_r=id_r, ))
            conn.commit()
    
        print(f'{ticker} is done', datetime.datetime.now(), '\n')
    
    conn.close()
    

#populate table "Financials" with TTM with mistakes
def populate_TTM_with_ticker(ticker, n=0, sql_db_name = sql_db_name_def):
    
    conn = sqlite3.connect(f'{sql_db_name}.sqlite')
    cur = conn.cursor()

    query = cur.execute('''select id, Ticker, P_Date, Revenue, Revttm, 
		case when Revttm <= 0 then 0 else Revttm/Shares end as SPS,
        NetIncome, NIttm,
		case when NIttm <= 0 then 0 else NIttm/Shares end as EPS
        from (select
        id,
        Ticker,
        P_Date,
        Revenue,
        sum(Revenue) over(order by P_Date rows BETWEEN  3 PRECEDING AND CURRENT ROW) as Revttm,
        sum(Revenue) over(order by P_Date rows BETWEEN  3 PRECEDING AND CURRENT ROW) / Shares as SPS,
        NetIncome,
        sum(NetIncome) over(order by P_Date rows BETWEEN  3 PRECEDING AND CURRENT ROW) as NIttm,
        sum(NetIncome) over(order by P_Date rows BETWEEN  3 PRECEDING AND CURRENT ROW) / Shares as EPS,
		Shares
        from Financials
        where Ticker = ?
        order by P_Date DESC)''', (ticker,)).fetchall()

    df_columns = ['id', 'Ticker', 'Date', 'Revenue', 'Revttm', 'SPS', 'NetIncome','NIttm', 'EPS']
    df = pd.DataFrame(query, columns=df_columns)
    df.set_index('Date', inplace=True)

    for i in tqdm(range(df.shape[0])): #fh.shape[0]
        date, rev_t, sps, inc_t, eps, id_r = df.index[i], df['Revttm'][i], df['SPS'][i], df['NIttm'][i], df['EPS'][i], int(df['id'][i])

        cur.execute('''UPDATE Financials SET RevTTM =:rev_t, SPS=:sps, NetIncomeTTM=:inc_t, EPS=:eps
                        WHERE id=:id_r''', dict(rev_t=rev_t, sps=sps, inc_t=inc_t, eps=eps, ticker=ticker, Date=date, id_r=id_r, ))
        conn.commit()

    print(f'{ticker} is done', datetime.datetime.now(), '\n')
    
    conn.close()
        

#get all stocks with pe, pe rank, ps, ps rank
def get_mult_ranks(sql_db_name=sql_db_name_def):
    
    conn = sqlite3.connect(f'stocks.sqlite')
    cur = conn.cursor()
    
    query = cur.execute('''select * 
                        from (select
                        Ticker, 
                        first_value(Date) over(partition by Ticker order by Date DESC) as Date,
                        first_value(PS) over(partition by Ticker order by Date DESC) as PS,
                        round(first_value(PS_Rank) over(partition by Ticker order by Date DESC),4)*100 as PS_Rank,
                        first_value(PE) over(partition by Ticker order by Date DESC) as PE,
                        round(first_value(PE_Rank) over(partition by Ticker order by Date DESC),4)*100 as PE_Rank
                        from Multiples)
                        group by Ticker
                        order by PE_Rank''').fetchall()

    db_columns = ['Ticker', 'Date', 'PS', 'PS Rank', 'PE', 'PE Rank']
    db = pd.DataFrame(query)    
    db.columns = db_columns  
    
    conn.close()
    
    return db


#update all financial data
#could take some time to complete
def update_financial_data():
    
    #update stock prices
    update_prices()
    
    #update basic financial information
    update_financials_macrotrends()
    
    #update ttm fields for all new rows
    populate_TTM(only_null=True)
    
    #update pe, ps and so on
    populate_multiples()


###################################################
### ERRORS BLOCK ############
###################################################

#update all stocks with bad shares
def update_errors_shares(sql_db_name = sql_db_name_def):
    
    conn = sqlite3.connect(f'{sql_db_name}.sqlite')
    cur = conn.cursor()
    
    tickers = cur.execute('''select Ticker
                        from Financials
                        where Shares = ''
                        group by Ticker''').fetchall()
    list_of_rows = []
    for ticker in tickers:
        query = cur.execute('''select *
                from Financials
                where Ticker = ?
                order by P_Date''', (ticker[0],)).fetchall()
        for lane in query:
            if lane[9] =='':
                lane_index = query.index(lane)

                for n_lane in query[lane_index:]:
                    if n_lane[9] != '':
                        a_lane = list(lane)
                        a_lane[9] = n_lane[9]
                        break

                list_of_rows.append(a_lane)
    
    for i in tqdm(range(len(list_of_rows))):
        cur.execute('''update Financials set Shares =? where id=?''', (list_of_rows[i][9], list_of_rows[i][0],))
        conn.commit()
        
    conn.close()
    

#set all null_values to zero
def null_to_zero(table, column_name, sql_db_name = sql_db_name_def):
    
    conn = sqlite3.connect(f'{sql_db_name}.sqlite')
    cur = conn.cursor()
    
    query = cur.execute(f'''select *
                        from {table}
                        where {column_name} is NULL
                        ''').fetchall()
        
    for i in tqdm(range(len(query))):
        
        cur.execute(f'''UPDATE {table} SET {column_name}=0 WHERE id=?''', (query[i][0],))
        conn.commit()
    
    conn.close()
    
    
#update rows with null ranks
def update_errors_null_ranks(sql_db_name = sql_db_name_def):
    
    conn = sqlite3.connect(f'{sql_db_name}.sqlite')
    cur = conn.cursor()
    
    tickers_without_pe = cur.execute('''select Ticker
                        from Multiples
                        where PS_Rank is NULL OR PE_Rank is NULL
                        group by Ticker
                        order by Date''').fetchall()
    
    for ticker in tickers_without_pe:
    
        position = tickers_without_pe.index(ticker)
        print(f'Got ticker {ticker}, element {position}')
        
        query = cur.execute('''select id, Ticker, PE, PS
                from Multiples
                where Ticker=?
                order by Date''', (ticker[0], )).fetchall()
        
        df_columns = ['id', 'Ticker', 'P/S', 'P/E']
        df = pd.DataFrame(query, columns=df_columns)
        
        column_rank(df, 'P/S')
        column_rank(df, 'P/E')
        
        for i in tqdm(range(df.shape[0])): #df.shape[0]
            psrank, perank, id_r = float(df.loc[i, 'P/S Rank']), float(df.loc[i, 'P/E Rank']), int(df.loc[i, 'id'])
            cur.execute('''update Multiples set PS_Rank=?, PE_Rank=? where id=?''', (psrank, perank, id_r, ))
            conn.commit()
            
        print(f'{ticker} is done', datetime.datetime.now(), '\n')
    
    conn.close()
    

#update all bad ttm, sps, eps
def update_errors_ttm_sps_eps(sql_db_name = sql_db_name_def):
    
    conn = sqlite3.connect(f'{sql_db_name}.sqlite')
    cur = conn.cursor()
    
    tickers_wtih_mistakes = cur.execute('''select Ticker from Financials where SPS is NULL or EPS is NULL group by Ticker''').fetchall()
    
    for ticker in tickers_wtih_mistakes:
        populate_TTM_with_ticker(ticker[0])
        
    conn.close()
    
    
#update all bad PE_Rank and PS_Rank
def update_errors_PSR_PER(sql_db_name = sql_db_name_def):
    
    conn = sqlite3.connect(f'{sql_db_name}.sqlite')
    cur = conn.cursor()
    
    tickers_wtih_mistakes = cur.execute('''select Ticker from Multiples where PS_Rank is NULL or PE_Rank is NULL group by Ticker''').fetchall()
    
    for ticker in tickers_wtih_mistakes:
        populate_multiples_with_ticker(ticker[0])
        
    conn.close()
    
    
#update all rows with negative eps and sps
def update_errors_negative_eps_sps(sql_db_name = sql_db_name_def):
    
    conn = sqlite3.connect(f'{sql_db_name}.sqlite')
    cur = conn.cursor()
    
    tickers_wtih_mistakes = cur.execute('''select Ticker from Financials where SPS < 0 or EPS < 0 or SPS is null or EPS is null group by Ticker''').fetchall()
    
    for ticker in tickers_wtih_mistakes:
        
        print(f'starting work with {ticker[0]}')
        
        populate_TTM_with_ticker(ticker[0])
        populate_multiples_with_ticker(ticker[0])
        
        print(f'errors in {ticker[0]} has been elmininated')
    
    conn.close()