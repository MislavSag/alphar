# Title:  EVT threshold
# Author: Mislav Sagovac
# Description: Estiamte systemic risk with aggreagaate EVT threshold

# packages
library(tiledb)
library(data.table)
library(checkmate)
library(httr)
library(qlcal)
library(lubridate)



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



# HOURLY MARKET DATA -------------------------------------------------------
# import market data (choose frequency)
arr <- tiledb_array("D:/equity-usa-hour-fmpcloud-adjusted",
                    as.data.frame = TRUE,
                    query_layout = "UNORDERED",
                    selected_ranges = list(symbol = cbind(sp500_symbols, sp500_symbols))
)
system.time(prices <- arr[])
tiledb_array_close(arr)
prices <- as.data.table(prices)

# remove duplicates
prices_dt <- unique(prices, by = c("symbol", "time"))

# convert timezone
prices_dt[, time := as.POSIXct(time, tz = "UTC")]
setorder(prices_dt, symbol, time)

# change timezone and keep only trading hours
prices_dt[, time := with_tz(time, tzone = "America/New_York")]
prices_dt <- prices_dt[as.ITime(time) %between% c(as.ITime("09:30:00"), as.ITime("16:00:00"))]

# keep only NYSE trading days
trading_days <- getBusinessDays(prices_dt[, min(as.IDate(time))], prices_dt[, max(as.IDate(time))])
prices_dt = prices_dt[as.IDate(time) %in% as.IDate(trading_days)]

# remove observations with measurement errors
prices_dt <- prices_dt[open > 0 & high > 0 & low > 0 & close > 0] # remove rows with zero and negative prices

# order data
setorder(prices_dt, "symbol", "time")

# calculate retutns
prices_dt[, returns := close / shift(close) - 1, by = "symbol"]

# remove missing values
prices_dt <- na.omit(prices_dt)

# remove symobls with < 2 years of observations
prices_n <- prices_dt[, .N, by = symbol]
prices_n <- prices_n[N > 252 * 2 * 8]  # remove prices with only 700 or less observations
prices_dt <- prices_dt[symbol %in% prices_n[, symbol]]

# save SPY for later and keep only events symbols
spy <- prices_dt[symbol == "SPY"]
setorder(spy, time)



# ESTIMATE THRESHOLD ------------------------------------------------------
#

library(threshr)
u_vec_gom <- quantile(gom, probs = seq(0, 0.9, by = 0.05))
gom_cv <- ithresh(data = gom, u_vec = u_vec_gom)
plot(gom_cv)
