import sqlite3
from sqlite3 import Error
from yahoo_fin import stock_info as si
import pandas as pd
import numpy as np
from tqdm.notebook import tqdm
import datetime

tickers = si.tickers_sp500()

#get only digits
def get_digits(word):
    if type(word) == type(123.21):
        return word
    elif type(word) == type(123):
        return word
    elif word == '':
        return word
    only_digits = ''
    for i in range(0, len(word)):
        letter = word[i]
        if word[i].isdigit():
            only_digits = only_digits + word[i]
            
    return int(only_digits)


#make a column in df with a rank of another column
def column_rank(df, column_name):
    max_r = np.percentile(df[column_name], 95)
    min_r = df[column_name].min()

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
def create_tables(sql_db_name, const_list=False):
    
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
def update_prices(sql_db_name, n=0):
    
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
        cur.execute('SELECT * FROM Prices WHERE ticker = ? ORDER BY Prices.Date DESC', (ticker, ))
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
            cur.execute('''INSERT OR REPLACE INTO Prices (Ticker, Date, Open, High, Low, Close)
                        VALUES (?,?,?,?,?,?)''', (ticker, date, o, h, l, c, ))
            conn.commit()
        print(f'{ticker} is done', datetime.datetime.now(), '\n')
        time.sleep(1)
    
    
    print('-------------------------')
    print('EVERYTHIN IS DONE')
    print('-------------------------')
    
    conn.close()



#update stock financials using macrotrends
def update_financials_macrotrends(sql_db_name, n=0):
    
    conn = sqlite3.connect(f'{sql_db_name}.sqlite')
    cur = conn.cursor()
    tickers = si.tickers_sp500()
    tickers = [item.replace(".", "-") for item in tickers] # Yahoo Finance uses dashes instead of dots
    index_name = '^GSPC' # S&P 500
    end_date = datetime.date.today()
    
    for ticker in tickers[349:]:
        #show current position
        position = tickers.index(ticker)
        if ticker == 'BRK-B':
            ticker = 'BRK.B'
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
    
        if ticker == 'BRK.B':
            ticker = 'BRK-B'
    
        for i in tqdm(range(df.shape[0])): #fh.shape[0]
            date, r, n, s = df.index[i], get_digits(df['Revenue'][i]), get_digits(df['Income'][i]), get_digits(df['Shares'][i])
            cur.execute('''INSERT OR REPLACE INTO Financials (Ticker, Date, Revenue, NetIncome, Shares)
                        VALUES (?,?,?,?,?)''', (ticker, date, r, n, s, ))
            conn.commit()
        print(f'{ticker} is done', datetime.datetime.now(), '\n')
    
    print("------------------------------------------")
    print("EVERYTHIN IS DONE")
    print("------------------------------------------")
    
    conn.close()

    
#return relevant stock P/E rank    
def get_stock_rank(sql_db_name, ticker):
    
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
def get_stock_pe_chart(sql_db_name, ticker):
    
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
    
    return df['P/E'].plot()


#populate table "Multiples" with calculated multipliers
def populate_multiples(sql_db_name, n=0):
    
    conn = sqlite3.connect(f'{sql_db_name}.sqlite')
    cur = conn.cursor()
    
    for ticker in tickers[n:]:
        
        position = tickers.index(ticker)
        print(f'Got ticker {ticker}, element {position}')
    
        query = cur.execute('''select Ticker, MD_Date as Date, max(P_Date) as PeportMonth, Close/SPS as PS, Close/EPS as PE
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
            return
    
        df_columns = ['Ticker', 'Date', 'PeportMonth', 'P/S', 'P/E']
        df = pd.DataFrame(query, columns=df_columns)
        df.set_index('Date', inplace=True)

        column_rank(df, 'P/S')
        column_rank(df, 'P/E')
    
        for i in tqdm(range(df.shape[0])): #fh.shape[0]
            date, pe, per, ps, pes = df.index[i], df['P/E'][i], df['P/E Rank'][i], df['P/S'][i], df['P/S Rank'][i]
            cur.execute('''INSERT OR REPLACE INTO Multiples (Ticker, Date, PE, PE_Rank, PS, PS_Rank)
                            VALUES (?,?,?,?,?,?)''', (ticker, date, pe, per, ps, pes))
            conn.commit()
    
        print(f'{ticker} is done', datetime.datetime.now(), '\n')
    
    conn.close()


#populate table "Financials" with TTM values
def populate_TTM(sql_db_name, ticker, n=0):
    
    conn = sqlite3.connect(f'{sql_db_name}.sqlite')
    cur = conn.cursor()
    
    print(f'Got ticker {ticker}')
    
    query = cur.execute('''select
    Ticker,
    P_Date,
	Revenue,
	sum(Revenue) over(order by P_Date rows BETWEEN  3 PRECEDING AND CURRENT ROW) as Revttm,
	sum(Revenue) over(order by P_Date rows BETWEEN  3 PRECEDING AND CURRENT ROW) / Shares as SPS,
	NetIncome,
    sum(NetIncome) over(order by P_Date rows BETWEEN  3 PRECEDING AND CURRENT ROW) as NIttm,
	sum(NetIncome) over(order by P_Date rows BETWEEN  3 PRECEDING AND CURRENT ROW) / Shares as EPS
    from Financials
	where Ticker = ?
    order by P_Date DESC''', (ticker,)).fetchall()
    
    df_columns = ['Ticker', 'Date', 'Revenue', 'Revttm', 'SPS', 'NetIncome','NIttm', 'EPS']
    df = pd.DataFrame(query, columns=df_columns)
    df.set_index('Date', inplace=True)

    for i in tqdm(range(df.shape[0])): #fh.shape[0]
        date, rev_t, sps, inc_t, eps = df.index[i], df['Revttm'][i], df['SPS'][i], df['NIttm'][i], df['EPS'][i]

        cur.execute('''UPDATE Financials SET RevTTM =:rev_t, SPS=:sps, NetIncomeTTM=:inc_t, EPS=:eps
                        WHERE Ticker=:ticker AND P_Date=:Date''', dict(rev_t=rev_t, sps=sps, inc_t=inc_t, eps=eps, ticker=ticker, Date=date, ))
        conn.commit()
    
    print(f'{ticker} is done', datetime.datetime.now(), '\n')
    
    conn.close()