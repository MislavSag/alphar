# Title:  Momentum
# Author: Mislav Sagovac
# Description: Momentum strategies with ML

# packages
library(tiledb)
library(data.table)
library(checkmate)
library(httr)
library(findata)
library(qlcal)
library(PerformanceAnalytics)



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

# set calendar
qlcal::setCalendar("UnitedStates/NYSE")


# UNIVERSE ----------------------------------------------------------------
# universe consists of US stocks
url <- modify_url("https://financialmodelingprep.com/",
                  path = "api/v3/stock/list",
                  query = list(apikey = Sys.getenv("APIKEY-FMPCLOUD")))
p <- GET(url)
res <- content(p)
securities <- rbindlist(res, fill = TRUE)
stocks_us <- securities[type == "stock" &
                          exchangeShortName %in% c("AMEX", "NASDAQ", "NYSE", "OTC")]
symbols_list <- stocks_us[, unique(symbol)]

# profiles
p <- GET("https://financialmodelingprep.com//api/v4/profile/all",
         query = list(apikey = Sys.getenv("APIKEY-FMPCLOUD")),
         write_disk("tmp/profiles.csv", overwrite = TRUE))
profiles <- fread("tmp/profiles.csv")
profiles <- profiles[country == "US"]
setnames(profiles, tolower(colnames(profiles)))

# sp500 historical universe
p <- GET("https://financialmodelingprep.com/api/v3/historical/sp500_constituent?apikey=15cd5d0adf4bc6805a724b4417bbaafc")
res <- content(p)
sp500_history <- rbindlist(res)
p <- GET("https://financialmodelingprep.com/api/v3/sp500_constituent?apikey=15cd5d0adf4bc6805a724b4417bbaafc")
res <- content(p)
sp500_current <- rbindlist(res)
sp500_symbols <- c(sp500_history[, symbol],
                   sp500_history[, removedTicker],
                   sp500_current$symbol)
sp500_symbols <- sp500_symbols[sp500_symbols != ""]
sp500_symbols <- unique(sp500_symbols)
# sp500_symbols = sp500_symbols[1:200]  # REMOVE THIS LATER



# DAILY MARKET DATA -------------------------------------------------------
# import market data (choose frequency)
arr <- tiledb_array("D:/equity-usa-daily-fmp",
                    as.data.frame = TRUE,
                    query_layout = "UNORDERED",
                    selected_ranges = list(symbol = cbind(sp500_symbols, sp500_symbols))
)
system.time(prices <- arr[])
tiledb_array_close(arr)
prices <- as.data.table(prices)

# remove duplicates
prices_dt <- unique(prices, by = c("symbol", "date"))

# change date to data.table date
prices_dt[, date := data.table::as.IDate(date)]

# keep only NYSE trading days
trading_days <- getBusinessDays(prices_dt[, min(date)], prices_dt[, max(date)])
setkey(prices_dt, date)
prices_dt <- prices_dt[.(as.IDate(trading_days))]
setkey(prices_dt, NULL)

# remove observations with measurement errors
prices_dt <- prices_dt[open > 0 & high > 0 & low > 0 & close > 0 & adjClose > 0] # remove rows with zero and negative prices

# order data
setorder(prices_dt, "symbol", "date")

# adjuset all prices, not just close
prices_dt[, returns := adjClose / shift(adjClose) - 1, by = symbol] # calculate returns
prices_dt <- prices_dt[returns < 1] # TODO:: better outlier detection mechanism. For now, remove daily returns above 100%
adjust_cols <- c("open", "high", "low")
prices_dt[, (adjust_cols) := lapply(.SD, function(x) x * (adjClose / close)), .SDcols = adjust_cols] # adjust open, high and low prices
prices_dt[, close := adjClose]

# remove missing values
prices_dt <- na.omit(prices_dt[, .(symbol, date, open, high, low, close, volume, returns)])

# remove symobls with < 252 observations
prices_n <- prices_dt[, .N, by = symbol]
prices_n <- prices_n[N > 252]  # remove prices with only 700 or less observations
prices_dt <- prices_dt[symbol %in% prices_n[, symbol]]



# PREPARE DATA  -----------------------------------------------------------
# create yearly returns
prices_dt[, mom12 := close  / shift(close, n = 252) - 1, by = c("symbol")]
prices_dt[, mom6 := close  / shift(close, n = 126) - 1, by = c("symbol")]
prices_dt[, mom3 := close  / shift(close, n = 66) - 1, by = c("symbol")]
prices_dt[, mom1 := close  / shift(close, n = 22) - 1, by = c("symbol")]

# calculate monthly returns
prices_dt[, year_month := paste0(data.table::year(date), "-",
                                 data.table::month(date))]
prices_dt[, monthly_return := tail(close, 1) / head(close, 1) - 1, by = c("symbol", "year_month")]
prices_dt[, monthly_return_roll := shift(close, -22) / close - 1]

# checks
prices_dt[symbol == "ABMD", ][1:30]
29.25000 / 17.81250 - 1

# filter 50 (10%) stocks with highest momentum on the beginning of the month
prices_dt_filter <- prices_dt[, head(.SD, 1), by = c("symbol", "year_month")]

# calculate momentum
momentum_stocks <- na.omit(prices_dt_filter, cols = "mom12")
setorderv(momentum_stocks, c("year_month", "mom12"))
momentum_stocks <- momentum_stocks[, tail(.SD, 10), by = year_month]

# crate equity curve
portfolio = momentum_stocks[, .(symbol, date, monthly_return)]
portfolio = dcast(portfolio, date~symbol, value.var = "monthly_return")
portfolio_weigts = as.xts.data.table(portfolio)
portfolio_weigts[!is.na(portfolio_weigts)] <- 0.1
portfolio_weigts[is.na(portfolio_weigts)] = 0
portfolio_ret = Return.portfolio(as.xts.data.table(portfolio),
                                 weights = portfolio_weigts,
                                 rebalance_on="months")

# vis results
charts.PerformanceSummary(portfolio_ret)
