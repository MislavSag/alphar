# Title:  Title
# Author: Name
# Description: Description

# packages
library(tiledb)
library(data.table)
library(checkmate)
library(httr)
library(qlcal)
library(PerformanceAnalytics)
library(ggplot2)
library(patchwork)




# DATA --------------------------------------------------------------------
# import market data (choose frequency)
arr <- tiledb_array("D:/equity-usa-daily-fmp",
                    as.data.frame = TRUE,
                    query_layout = "UNORDERED",
                    selected_ranges = list(date = cbind(as.Date("2017-01-01"), as.Date("2023-01-01")))
                    # selected_ranges = list(symbol = cbind(sp500_symbols, sp500_symbols))
)
system.time(prices <- arr[])
tiledb_array_close(arr)
prices <- as.data.table(prices)

# remove duplicates
prices_dt <- unique(prices, by = c("symbol", "date"))

# change date to data.table date
prices_dt[, date := data.table::as.IDate(date)]

# keep only US stocks
prices_dt = prices_dt[symbol %in% symbols_list]

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

# save SPY for later and keep only events symbols
spy <- prices_dt[symbol == "SPY"]
setorder(spy, date)

# remove prices to save RAM
rm(prices)
gc()



# TLT EXAMPLE -------------------------------------------------------------
# set up TLT data
tlt = prices_dt[symbol == "TLT"]

# create weekly return
tlt[, return_week := shift(close, 5, type = "lead") / close - 1]

# remove first and last month
tlt = tlt[date %between% c("2002-08-01", "2023-04-01")]

# create xts object
tlt_xts = as.xts.data.table(tlt[, .(date, returns)])

# returns for whole period
Return.annualized(tlt_xts)
Return.cumulative(tlt_xts)

# create group variable
tlt[, yearmonthid := round(date, digits = "month")]
tlt[, .(date, date, yearmonthid)]
# tlt[, yearmonthid := as.integer(yearmonthid)]
# tlt[, .(date, date, yearmonthid)]

# number of days before and after the end of the month
tlt_end = tlt[, tail(.SD, 3), by = yearmonthid]
tlt_start = tlt[, head(.SD, 3), by = yearmonthid]

# returns of start and end
xts_ = as.xts.data.table(tlt_end[, .(date, returns)])
Return.annualized(xts_)
Return.cumulative(xts_)
xts_ = as.xts.data.table(tlt_start[, .(date, returns)])
Return.annualized(xts_)
Return.cumulative(xts_)

# create dummies
tlt[, day_of_month := 1:.N, by = yearmonthid]
tlt[, day_of_month := as.factor(day_of_month)]
res = lm(return_week ~ day_of_month, data = as.data.frame(tlt))
summary(res)



# SEASONALITY MINING ------------------------------------------------------
# short code example
stocks = prices_dt[symbol == "TLT"]
stocks[, return_week := shift(close, 5, type = "lead") / close - 1]
stocks = stocks[date %between% c("2002-08-01", "2023-04-01")]
stocks[, yearmonthid := round(date, digits = "month")]
stocks[, day_of_month := 1:.N, by = yearmonthid]
stocks[, day_of_month := as.factor(day_of_month)]
res = lm(return_week ~ day_of_month, data = as.data.frame(stocks))
summary(res)

# prepare data
X = prices_dt[, .(symbol, date, close, volume, returns)]
X[, return_week := shift(close, 5, type = "lead") / close - 1, by = symbol]
# X = X[date %between% c("2000-02-01", "2023-04-01")]
X[, yearmonthid := round(date, digits = "month")]
X[, day_of_month := 1:.N, by = .(symbol, yearmonthid)]
X[, day_of_month := as.factor(day_of_month)]

# keep survived companies. We can't trade that companies
X[, keep := (as.IDate("2022-12-30") - max(date)) < 15, by = symbol]
X = X[keep == TRUE]

# keep only stocks with price above 2 in whole period
X[, keep := any(close > 2), by = symbol]
X = X[keep == TRUE]

# keep only stocks with median volume above 50.000$
X[, median_volume := median(volume, na.rm = TRUE), by = symbol]
X = X[median_volume > 50000]

# inspect
X[, length(unique(symbol))]
"TLT" %in% X[, unique(symbol)]
"SPY" %in% X[, unique(symbol)]
"CLM" %in% X[, unique(symbol)]
"AAPL" %in% X[, unique(symbol)]

# loop though all symbols
get_coeffs = function(df) {
  res = lm(return_week ~ day_of_month, data = as.data.frame(df))
  summary_fit = summary(res)
  as.data.table(summary_fit$coefficients,keep.rownames = TRUE)
}
X_seasons = X[, get_coeffs(.SD), by = symbol]

# inspect significatn dummies
significant_dummies = X_seasons[`Pr(>|t|)` < 0.001]
significant_dummies[, unique(symbol)]
cat("Percent of really significant dummies in all possible dummies is ",
    nrow(significant_dummies) / X[, length(unique(symbol)) * 22] * 100, "%",
    sep = "")
cat("Percent of kept symbols is ",
    significant_dummies[, length(unique(symbol))] / X_seasons[, length(unique(symbol))] * 100, "%",
    sep = "")

symbols_n = significant_dummies[, .N, by = symbol]
setorder(symbols_n, -N)
head(symbols_n, 50)
setorder(significant_dummies, `Pr(>|t|)`)

# inspect individually
significant_dummies[rn != "(Intercept)", length(unique(symbol))]
significant_dummies[rn != "(Intercept)", .N, by = symbol][order(N)]
symbol_ = "CLM"
X_seasons[symbol == symbol_]
plot(prices_dt[symbol == symbol_, close])
sample_ = X[symbol == symbol_]
data_plot = sample_[, mean(return_week), by = day_of_month]
g1 = ggplot(data_plot, aes(day_of_month, V1)) +
  geom_bar(stat = "identity") +
  ggtitle("Mean returns by month days")
data_plot = sample_[, median(return_week), by = day_of_month]
g2 = ggplot(data_plot, aes(day_of_month, V1)) +
  geom_bar(stat = "identity") +
  ggtitle("Median returns by month days")
g1 / g2
data_plot = sample_[, mean(return_week), by = .(year = data.table::year(date), day_of_month)]
g1 = ggplot(data_plot, aes(day_of_month, V1)) +
  geom_bar(stat = "identity") +
  facet_grid(. ~ year) +
  ggtitle("Mean returns by month days by year")
data_plot = sample_[, median(return_week), by = .(year = data.table::year(date), day_of_month)]
g2 = ggplot(data_plot, aes(day_of_month, V1)) +
  geom_bar(stat = "identity") +
  facet_grid(. ~ year) +
  ggtitle("Median returns by month days by year")
g1 / g2
data_plot = sample_[year(date) == 2018]
ggplot(data_plot, aes(day_of_month, return_week)) +
  geom_bar(stat = "identity") +
  facet_grid(. ~ yearmonthid)
