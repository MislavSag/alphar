library(data.table)
library(duckdb)




# import distinct symbols from hour data
conn <- dbConnect(duckdb::duckdb(), ":memory:")
query <- paste("SELECT DISTINCT Symbol FROM read_csv_auto('F:/lean_root/data/all_stocks_daily.csv')")
distinct_symbols <- dbGetQuery(conn, query)
dbDisconnect(conn, )

# take sample of symbols for developing
symbols = sample(distinct_symbols[[1]], 15000)

# import daily data
conn <- dbConnect(duckdb::duckdb(), ":memory:")
symbols_string = paste(symbols, collapse = "', '")
symbols_string = paste0("'", symbols_string, "'")
query <- sprintf("
SELECT *
FROM read_csv_auto('F:/lean_root/data/all_stocks_hour.csv')
WHERE Symbol IN (%s)
", symbols_string)
dt = dbGetQuery(conn, query)
dbDisconnect(conn)
