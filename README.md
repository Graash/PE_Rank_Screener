# PE_Rank_Screener
A try to make investing easy and straightforward. A tool that helps to choose stocks.

The main idea of this project is to give every stock a rank which helps to find the best stock to invest.

Some basic tools to create a SQLite DB, populate it with data and make tips for investing.

For begininng use create_sql_db(name) to create a new SQLite DB with a specific name.

Next, use update_prices() func with a name of your db. You can put here a second parameter 'n' to start from a certain point in the list of tickers.
Use this parameter if the previous attempt failed.

Func update_financials_macrotrends() connect to the macrotrends.com to download financial data for stocks.

Then use populate_ttm() and populate_multiples() functions to add all necessary data to your DB.

That's all, it is all complete now. Some functionts can take some time, but you can continue from the last checkpoint.

Disclaimer! This is an early stage of a big project. Hope there will be much more!
