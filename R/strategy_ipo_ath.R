library(data.table)
library(fs)
library(arrow)
library(findata)
library(glue)
library(httr)
library(roll)
library(PerformanceAnalytics)


# SET UP ------------------------------------------------------------------
# global vars
PATH = "F:/data/equity/us"


# PRICE DATA --------------------------------------------------------------
# Import QC daily data
prices = fread("F:/lean/data/stocks_daily.csv")
setnames(prices, gsub(" ", "_", c(tolower(colnames(prices)))))

# Remove duplicates
prices = unique(prices, by = c("symbol", "date"))

# Remove duplicates - there are same for different symbols (eg. phun and phun.1)
dups = prices[, .(symbol , n = .N),
              by = .(date, open, high, low, close, volume, adj_close,
                     symbol_first = substr(symbol, 1, 1))]
dups = dups[n > 1]
dups[, symbol_short := gsub("\\.\\d$", "", symbol)]
symbols_remove = dups[, .(symbol, n = .N),
                      by = .(date, open, high, low, close, volume, adj_close,
                             symbol_short)]
symbols_remove[n >= 2, unique(symbol)]
symbols_remove = symbols_remove[n >= 2, unique(symbol)]
symbols_remove = symbols_remove[grepl("\\.", symbols_remove)]
prices = prices[symbol %notin% symbols_remove]

# Adjust all columns
prices[, adj_rate := adj_close / close]
prices[, let(
  open = open*adj_rate,
  high = high*adj_rate,
  low = low*adj_rate
)]
setnames(prices, "close", "close_raw")
setnames(prices, "adj_close", "close")
prices[, let(adj_rate = NULL)]
setcolorder(prices, c("symbol", "date", "open", "high", "low", "close", "volume"))

# Remove missing values
prices = na.omit(prices)

# Sort
setorder(prices, symbol, date)

# Create returns
prices[, returns := close / shift(close) - 1, by = symbol]

# Remove missing values
prices = na.omit(prices)

# free memory
gc()


# SIGNAL ------------------------------------------------------------------
# Keep only first 90 observations from begining of the series for all symbols
setorder(prices, symbol, date)
prices_ipo = prices[, .SD[1:90], by = symbol]

# Create signal that is equal to 1 if close at ATH
prices_ipo[, close_max := roll_max(close, width = nrow(.SD), min_obs = 2), by = symbol]
# 1)
# prices_ipo[, ath := close >= shift(close_max)]
# 2)
prices_ipo[, ath := close >= shift(close_max) & (shift(close) > 20)]

# Create strategy returns
# 1)
# prices_ipo[, strategy := (returns * shift(ath)), by = symbol]
# 2)
# prices_ipo[returns == 1]
# prices_ipo[symbol == "adil" & date %between% c(as.IDate("2018-11-27"), as.IDate("2018-11-30"))]
prices_ipo[, strategy := returns * shift(ath), by = symbol]

# Chek one symbol
prices_ipo[symbol == "aapl", .(symbol, date, close, close_raw, returns, close_max, ath, strategy)]

# Remove missing values
prices_ipo = na.omit(prices_ipo)

# keep only where ath (signal) is true
prices_ipo = prices_ipo[, .SD[shift(ath) == TRUE], by = symbol]

# Calculate portfolio return for every date
portfolio = prices_ipo[, .(strategy, weight = 1 / length(strategy)), by = date]
portfolio[, weight := ifelse(weight > 0.05, 0.05, weight)]
setorder(portfolio, date)
portfolio = portfolio[, .(strategy = sum(strategy * weight)), by = date]
portfolio = portfolio[date > as.Date("2002-01-01")] # to match QC backtest

# Equity curve
portfolio_xts = as.xts.data.table(portfolio[, .(date, strategy)])
charts.PerformanceSummary(na.omit(portfolio_xts))
charts.PerformanceSummary(na.omit(tail(portfolio_xts, 1000)))

# Test
# portfolio[date %between% c(as.IDate("2006-06-13"), as.IDate("2006-06-19"))]
# prices_ipo[date %between% c(as.IDate("2006-06-13"), as.IDate("2006-06-19"))]


# ARCHIVE -----------------------------------------------------------------
# # PROFILES DATA -----------------------------------------------------------
# # Get profiles data from FMP cloud
# api_key = Sys.getenv("APIKEY-FMPCLOUD")
# url = glue("https://financialmodelingprep.com/api/v4/profile/all?apikey={api_key}")
# file_ = "F:/profiles.csv"
# GET(url, write_disk(file_, overwrite = TRUE))
# profiles_raw = fread(file_)
# profiles_raw[Symbol == "ZBAO"]
# profiles = profiles_raw[, .(symbol = tolower(Symbol), cik, date_ipo = ipoDate,
#                             country, exchange, exchangeShortName)]
# exchanges_keep = c("NASDAQ", "NYSE", "AMEX", "ETF")
# profiles_us = profiles[exchangeShortName %in% exchanges_keep]
#
# # NASDAQ IPO DATA ---------------------------------------------------------
# # Get NASDAQ IPO data. Takes some time to webacrape this data
# nasdaq = Nasdaq$new()
# ipo_nasdaq = nasdaq$ipo_calendar()
# ipo_nasdaq_priced = copy(ipo_nasdaq$priced)
# ipo_nasdaq_priced[, let(
#   proposedSharePrice = as.numeric(proposedSharePrice),
#   sharesOffered = as.numeric(gsub(",", "", sharesOffered)),
#   date = as.Date(pricedDate, format = "%m/%d/%Y"),
#   dollarValueOfSharesOffered = as.numeric(gsub(",|\\$", "", dollarValueOfSharesOffered)),
#   dealStatus = NULL,
#   symbol = tolower(proposedTickerSymbol),
#   proposedTickerSymbol = NULL,
#   pricedDate = NULL
# )]
# ipo_nasdaq_filed = copy(ipo_nasdaq$filed)
# ipo_nasdaq_filed[, let(
#   date = as.Date(filedDate, format = "%m/%d/%Y"),
#   dollarValueOfSharesOffered = as.numeric(gsub(",|\\$", "", dollarValueOfSharesOffered)),
#   symbol = tolower(proposedTickerSymbol),
#   proposedTickerSymbol = NULL,
#   filedDate = NULL
# )]
# ipo_nasdaq_filed = ipo_nasdaq_filed[date > as.Date("2016-06-01")]
# setorder(ipo_nasdaq_filed, date)
# ipo_nasdaq_filed[date == as.Date("2024-06-03")]
# tail(ipo_nasdaq_filed, 20)
#
#
# # IPO DATA ----------------------------------------------------------------
# # Get IPO data
# ipo_dt = read_parquet(path(PATH, "fundamentals", "ipo.parquet"))
#
# # Remove columns
# ipo_dt[, all(filingDate == effectivenessDate)]
# ipo_dt[filingDate != as.Date(acceptedDate)]
# # form is constant; url not needed; effectivenessDate is the same as filingDate
# ipo_dt[, let(form = NULL, url = NULL, effectivenessDate = NULL, date = filingDate, filingDate = NULL)]
# ipo_dt[, symbol := tolower(symbol)]
# ipo_dt[, .N, by = symbol][order(N, decreasing = TRUE)]
# ipo_dt[, date_accepted := as.Date(acceptedDate)]
#
# # Merge with NASDAQ IPO data
# ipo_dt[symbol == "brth"]
# ipo_nasdaq_filed[symbol == "brth"]
#
# ipo_nasdaq_filed[ipo_dt, on = c("symbol")][!is.na(dealID)]
# ipo_nasdaq_filed[ipo_dt, on = c("symbol", "date")][!is.na(dealID)]
# ipo_nasdaq_filed[ipo_dt[, .(symbol, date = date_accepted)], on = c("symbol", "date")][!is.na(dealID)]
# ipo_nasdaq_filed[ipo_dt[, .(symbol, date = date_accepted)], on = c("symbol")][!is.na(dealID)]

# # SIGNAL ------------------------------------------------------------------
# # Sample IPO symbols
# prices_ipo = prices[symbol %in% profiles_us[, unique(symbol)]]
#
# # Merge IPO data to prices
# prices_ipo = profiles_us[, .(symbol, date_ipo)][prices_ipo, on = c("symbol")]
#
# # Keep only data after the IPO date
# prices_ipo = prices_ipo[, .SD[date >= date_ipo], by = "symbol"]
#
# # Checks
# prices_ipo[, all(date_ipo == min(date)), by = symbol][, sum(V1) / nrow(.SD) * 100]
# prices_ipo[, all(min(date) - date_ipo < 7), by = symbol][, sum(V1) / nrow(.SD) * 100]
#
# # Date can't be to far from ipo date
# symbols_keep = prices_ipo[, all(min(date) - date_ipo < 7), by = symbol][V1 == TRUE, symbol]
# prices_ipo = prices_ipo[symbol %in% symbols_keep]
#
# # Check some symbols
# prices_ipo[symbol == "zbao"]
# prices[symbol == "zbao"]
#
# # Create signal: check if symbol is on all time high
# prices_ipo[, let(
#   close_max = roll_max(close),
#   signal = close == close_max
# )]
# prices_ipo = prices_ipo[signal == TRUE]
# prices_ipo[, let(signal = NULL)]
