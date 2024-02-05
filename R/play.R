library(data.table)
library(duckdb)
library(lubridate)
library(ggplot2)


# setup
PATHDAILY = "F:/lean_root/data/all_stocks_hour.csv"

# import distinct symbols from hour data
conn <- dbConnect(duckdb::duckdb(), ":memory:")
query <- sprintf("SELECT DISTINCT Symbol FROM read_csv_auto('%s')", PATHDAILY)
distinct_symbols <- dbGetQuery(conn, query)
dbDisconnect(conn, shutdown=TRUE)

# take sample of symbols for developing
symbols = sample(distinct_symbols[[1]], 5)

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
dbDisconnect(conn, shutdown=TRUE)

# convert to data.table
dt = as.data.table(dt)

# set time zone
attributes(dt$Date)
if (!("tzone" %in% names(attributes(dt$Date)))) {
  setattr(dt$Date, 'tzone', "America/New_York")
} else if (attributes(dt$Date)[["tzone"]] != "America/New_York") {
  dt[, Date := force_tz(Date, "America/New_York")]
}
attributes(dt$Date)

# keep only coluns I need
dt = dt[, .(symbol = Symbol, date = Date, close = `Adj Close`)]

# calculate returns
dt[, returns := close / shift(close) - 1]

#
dt[as.ITime(date) == as.ITime("09:00:00") & returns < -0.04, negative_open := TRUE]
dt[, d := as.Date(date)]
reverse_dates = dt[, .(intra_rev = negative_open == TRUE & tail(close, 1) < head(close, 1)), by = .(symbol, d)]

# merge dt and reverse
dt = reverse_dates[intra_rev == TRUE][dt, on = c("symbol", "d")]

# sample plot
sample_dt = dt[symbol == "bebe", .(close = tail(close, 1),
                                  intra_rev = tail(intra_rev, 1)), by = date]
ggplot(dt[symbol == "bebe"], aes(x = date, y = close)) +
  geom_line() +
  geom_point(data = sample_dt[intra_rev == TRUE], aes(x = date, y = close), color = "red")


