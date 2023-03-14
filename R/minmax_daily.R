# Title:  MinMax daily
# Author: Msilav Sagovac
# Description: MinMax strategy using daily frequency and SP500 stocks.

# packages
library(tiledb)
library(data.table)
library(checkmate)



# SET UP ------------------------------------------------------------------
# check if we have all necessary env variables
assert_choice("AWS-ACCESS-KEY", names(Sys.getenv()))
assert_choice("AWS-SECRET-KEY", names(Sys.getenv()))
assert_choice("AWS-REGION", names(Sys.getenv()))
assert_choice("BLOB-ENDPOINT", names(Sys.getenv()))
assert_choice("BLOB-KEY", names(Sys.getenv()))
assert_choice("APIKEY-FMPCLOUD", names(Sys.getenv()))
assert_choice("FRED-KEY", names(Sys.getenv()))

# set credentials
config <- tiledb_config()
config["vfs.s3.aws_access_key_id"] <- Sys.getenv("AWS-ACCESS-KEY")
config["vfs.s3.aws_secret_access_key"] <- Sys.getenv("AWS-SECRET-KEY")
config["vfs.s3.region"] <- Sys.getenv("AWS-REGION")
context_with_config <- tiledb_ctx(config)
# fredr_set_key(Sys.getenv("FRED-KEY"))

# date segments
GFC <- c("2007-01-01", "2010-01-01")
COVID <- c("2020-01-01", "2021-06-01")
AFTER_COVID <- c("2021-06-01", "2022-01-01")
NEW <- c("2022-01-01", as.character(Sys.Date()))



# MARKET DATA -------------------------------------------------------------
# import market data (choose frequency)
arr <- tiledb_array("D:/equity-usa-daily-fmp",
                    as.data.frame = TRUE,
                    query_layout = "UNORDERED"
)
system.time(prices <- arr[])
tiledb_array_close(arr)
prices <- as.data.table(prices)

# filter dates and symbols
prices_dt <- prices[symbol %in% c(unique(events), "SPY")]
prices_dt <- prices_dt[date > as.Date("2010-01-01")]
prices_dt <- unique(prices_dt, by = c("symbol", "date"))
prices_dt[symbol == "SPY"]
prices_dt[symbol == "ZYXI"]

# free resources
rm(prices)
gc()

# clean daily prices
prices_dt <- prices_dt[open > 0 & high > 0 & low > 0 & close > 0 & adjClose > 0] # remove rows with zero and negative prices
setorder(prices_dt, "symbol", "date")
prices_dt[, returns := adjClose   / data.table::shift(adjClose) - 1, by = symbol] # calculate returns
prices_dt <- prices_dt[returns < 1] # TODO:: better outlier detection mechanism. For now, remove daily returns above 100%
adjust_cols <- c("open", "high", "low")
prices_dt[, (adjust_cols) := lapply(.SD, function(x) x * (adjClose / close)), .SDcols = adjust_cols] # adjust open, high and low prices
prices_dt[, close := adjClose]
prices_dt <- na.omit(prices_dt[, .(symbol, date, open, high, low, close, volume, returns)])
prices_n <- prices_dt[, .N, by = symbol]
prices_n <- prices_n[N > 700]  # remove prices with only 700 or less observations
prices_dt <- prices_dt[symbol %in% prices_n]

# save SPY for later and keep only events symbols
spy <- prices_dt[symbol == "SPY"]
setorder(spy, date)

