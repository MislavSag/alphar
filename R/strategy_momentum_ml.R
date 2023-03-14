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
url <- "https://financialmodelingprep.com/api/v3/historical/sp500_constituent?apikey=15cd5d0adf4bc6805a724b4417bbaafc"
p <- GET(url)
res <- content(p)
sp500 <- rbindlist(res)
sp500_symbols <- c(sp500[, symbol], sp500[, removedTicker])
sp500_symbols <- sp500_symbols[sp500_symbols != ""]



# MARKET DATA -------------------------------------------------------------
# import market data (choose frequency)
arr <- tiledb_array("D:/equity-usa-daily-fmp",
                    as.data.frame = TRUE,
                    query_layout = "UNORDERED"
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

# clean daily prices
prices_dt <- prices_dt[open > 0 & high > 0 & low > 0 & close > 0 & adjClose > 0] # remove rows with zero and negative prices
setorder(prices_dt, "symbol", "date")
prices_dt[, returns := adjClose / shift(adjClose) - 1, by = symbol] # calculate returns
prices_dt <- prices_dt[returns < 1] # TODO:: better outlier detection mechanism. For now, remove daily returns above 100%
adjust_cols <- c("open", "high", "low")
prices_dt[, (adjust_cols) := lapply(.SD, function(x) x * (adjClose / close)), .SDcols = adjust_cols] # adjust open, high and low prices
prices_dt[, close := adjClose]
prices_dt <- na.omit(prices_dt[, .(symbol, date, open, high, low, close, volume, returns)])
prices_n <- prices_dt[, .N, by = symbol]
prices_n <- prices_n[N > 252]  # remove prices with only 700 or less observations
prices_dt <- prices_dt[symbol %in% prices_n[, symbol]]

# save SPY for later and keep only events symbols
spy <- prices_dt[symbol == "SPY"]
setorder(spy, date)




# PREPARE DATA  -----------------------------------------------------------
# monthly data


# create one month returns
prices_dt[, ret_month := shift(close)]

# calculate momentum


