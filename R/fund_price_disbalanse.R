library(data.table)
library(arrow)
library(roll)
library(PerformanceAnalytics)
library(AzureStor)
library(future.apply)
library(finutils)
library(qlcal)
library(dplyr)
library(ggplot2)


# SET UP ------------------------------------------------------------------
# Global vars
PATH         = "F:/data/equity/us"

# Set NYSE calendar
setCalendar("UnitedStates/NYSE")


# EVENTS ------------------------------------------------------------------
# get events data
events = read_parquet(fs::path(PATH, "fundamentals", "earning_announcements", ext = "parquet"))
events = events[date < Sys.Date()]
events = unique(events, by = c("symbol", "date"))
investingcom_ea = read_parquet(
  fs::path(
    PATH,
    "fundamentals",
    "earning_announcements_investingcom",
    ext = "parquet"
  )
)
investingcom_ea = investingcom_ea[, .(symbol, time, eps, eps_forecast, revenue, revenue_forecast, right_time)]
investingcom_ea[, date_investingcom := as.Date(time)]
investingcom_ea = investingcom_ea[date_investingcom <= qlcal::advanceDate(Sys.Date(), 2)]
setnames(investingcom_ea,
         colnames(investingcom_ea)[2:6],
         paste0(colnames(investingcom_ea)[2:6], "_investingcom"))
setorder(investingcom_ea, date_investingcom)
investingcom_ea[date_investingcom %between% c("2024-11-01", "2024-11-08")][!is.na(eps_investingcom)]
es = read_parquet(
  fs::path(
    PATH,
    "fundamentals",
    "earning_surprises",
    ext = "parquet"
  )
)
events = merge(
  events,
  investingcom_ea,
  by.x = c("symbol", "date"),
  by.y = c("symbol", "date_investingcom"),
  all.x = TRUE,
  all.y = FALSE
)
events = merge(events, es, by = c("symbol", "date"), all = TRUE)
events = events[!is.na(date) & !is.na(as.Date(time_investingcom))]
events[!is.na(right_time) & right_time == "marketClosed ", right_time := "amc"]
events[!is.na(right_time) & right_time == "marketOpen ", right_time := "bmo"]
events[, same_announce_time := time == right_time]
events = events[events$same_announce_time == TRUE]
events = unique(events, by = c("symbol", "date"))
eps_threshold = 0.1
events_ic_similar  = events[(eps >= eps_investingcom * 1-eps_threshold) & eps <= (1 + eps_threshold)]
events_fmp_similar = events[(eps >= actualEarningResult * 1-eps_threshold) & actualEarningResult <= (1 + eps_threshold)]
events = unique(rbind(events_ic_similar,
                      events_fmp_similar,
                      events[date > (Sys.Date() - 1)]))
cols_remove = c(
  "eps_investingcom", "eps_forecast_investingcom", "revenue_investingcom",
  "revenue_forecast_investingcom", "right_time", "actualEarningResult",
  "estimatedEarning", "same_announce_time", "time_investingcom", "updatedFromDate")
events[, (cols_remove) := NULL]
events = events[symbol %notin% events[!isBusinessDay(date), symbol]]

# Plot number of rows by date
if (interactive()) {
  ggplot(events[, .N, by = date][order(date)], aes(x = date, y = N)) +
    geom_line() +
    theme_minimal()
}

# Sample results after 2015
events[, min(date)]
events = events[date > as.Date("2015-01-01")]


# FMP PRICES --------------------------------------------------------------
# Import FMP prices
prices = fread("F:/data/equity/daily_fmp_all.csv")
setkey(prices, "symbol")
events_symbols = c(events[, unique(symbol)], "SPY")
prices = prices[.(events_symbols)]
prices = prices[date > as.Date("2010-01-01")]
setorder(prices, symbol, date)
prices = unique(prices, by = c("symbol", "date"))
cols = c("open", "high", "low", "close", "adjClose")
prices[, (cols) := lapply(.SD, as.numeric), .SDcols = cols]

# Filter dates and symbols
prices_n = prices[, .N, by = symbol]
prices_n = prices_n[which(prices_n$N > 700)]  # remove prices with only 700 or less observations
prices = prices[symbol %chin% prices_n[, symbol]]

# Remove symbols that have (almost) constant close prices
prices[, sd_roll := roll::roll_sd(close, 22 * 6), by = symbol]
symbols_remove = prices[sd_roll == 0, unique(symbol)]
prices = prices[symbol %notin% symbols_remove]

# Calculate most liquid symbols
prices[, dollar_vol_rank := frankv(close * volume, order = -1L), by = date]


# FUNDAMENTAL DATA --------------------------------------------------------
# Import fundamental data
fundamentals = read_parquet("F:/data/equity/us/predictors_daily/factors/fundamental_factors.parquet")

# clean fundamentals
fundamentals = fundamentals[date > as.Date("2000-01-01")]
fundamentals[, acceptedDateTime := as.POSIXct(acceptedDate, tz = "America/New_York")]
fundamentals[, acceptedDate := as.Date(acceptedDateTime)]
fundamentals[, acceptedDateFundamentals := acceptedDate]
setnames(fundamentals, "date", "fundamental_date")
fundamentals = unique(fundamentals, by = c("symbol", "acceptedDate"))

# Checks
symbol_ = "msft"
fundamentals[symbol == "MSFT", .(symbol, acceptedDate, acceptedDateTime, acceptedDateFundamentals, EPS)]

# choose fundamental columns
# They used the following metrics:
# 1 – earnings
# 2 – return on equity
# 3 – return on assets
# 4 – accrual operating profitability to equity
# 5 - cash operating profitability to assets
# 6 – gross profit to assets
# 7 – net payout ratio
cols = colnames(fundamentals)
cols[grepl("^p", cols, ignore.case = TRUE)]
fundamentals[, .SD, .SDcols = cols[grepl("earning|eps|rev", cols, ignore.case = TRUE)]]
cols_funds = c("symbol", "acceptedDateFundamentals", "acceptedDate",
               "EPSGrowth", "EPS",
               "fiveYRevenueGrowthPerShare",
               "returnOnAssets", "returnOnEquity",
               "grossProfitMargin", "payoutRatio",
               "freeCashFlowYield",
               "threeYRevenueGrowthPerShare")
falpha = fundamentals[, ..cols_funds]
falpha = na.omit(falpha)

# We need at least 8 observations per symbol
falpha[, n_obs := .N, by = symbol]
falpha = falpha[n_obs > 8]
falpha[, n_obs := NULL]

# calculate EPSGrowth rolling standard deviation
setorder(falpha, symbol, acceptedDate)
eps_years = 1:6
cols_epsv = paste0("EPSGrowth_sd_", eps_years)
falpha[, (cols_epsv) := lapply(eps_years, function(x) roll_sd(EPSGrowth, 4*x)), by = symbol]
eps_q = 1:8
cols_epsg = paste0("EPSGrowth_ema_", eps_q)
falpha[, (cols_epsg) := lapply(eps_q, function(x) TTR::EMA(EPSGrowth, x)), by = symbol]
falpha = na.omit(falpha)


# MERGE -------------------------------------------------------------------
# merge prices and selected fundamental data
prices[, date_prices := date]
dt = falpha[prices, on = c("symbol", "acceptedDate" = "date"), roll = Inf]

# calculate month returns
setorder(dt, symbol, date_prices)
returns_months = c(1, 12, 24, 36)
cols = paste0("returns_", returns_months)
dt[, (cols) := lapply(returns_months, function(x) close / shift(close, x) - 1),
   by = symbol]

# keep columns wee need
cols_keep = c("symbol", "date_prices", "close", "adjClose", "volume",
              "EPSGrowth", "EPS", "threeYRevenueGrowthPerShare", cols_epsv,
              cols_epsg, cols)
dt = dt[, ..cols_keep]
dt = na.omit(dt, cols = c("close", "adjClose", "EPS", "volume", cols))

# Define new var that is TRUE if it is last day of the month
setnames(dt, "date_prices", "date")
dt[, month := data.table::yearmon(date)]
dt[, last_month_day := last(date) == date, by = c("symbol", "month")]
data.table::first(dt, 10)

# Create monthly volume rank
dt[, dollar_volume := close * volume]
dt[, volume := NULL]


# INSAMPLE OPTIMIZATION ---------------------------------------------------
# Optimize
universe = dt[last_month_day == TRUE]
setorder(universe, symbol, date)
universe[, target := shift(adjClose, -1, type = "shift") / adjClose - 1, by = symbol]
universe = na.omit(universe, cols = c("target"))
backtest = function(uni,
                    eps_thresh = -100,
                    close_raw_thresh = 0.01,
                    epsg_thresh = 100,
                    return_mom = "returns_6",
                    eps_n = "EPSGrowth_sd_1",
                    mom_n = 20,
                    coarse_n = 100,
                    rev_positive = TRUE,
                    ret_sharpe = TRUE) {
  # uni = copy(universe)
  # eps_thresh = -100
  # close_raw_thresh =
  # epsg_thresh = 100
  # return_mom = "returns_12"
  # eps_n = "EPSGrowth_sd_1"
  # mom_n = 20
  # coarse_n = 100
  # rev_positive = TRUE
  # ret_sharpe = TRUE

  if (rev_positive) {
    uni = uni[threeYRevenueGrowthPerShare > 0]
  }
  uni[, rank_volume := frank(-dollar_volume, ties.method = "dense"), by = month]
  uni[rank_volume < coarse_n]
  uni = uni[EPS > x, env = list(x = eps_thresh)]
  uni = uni[close > close_raw_thresh, env = list(x = close_raw_thresh)]
  uni = na.omit(uni)
  setorderv(uni, c("month", eps_n), order = c(1L, -1L))
  uni = uni[, data.table::last(.SD, x), by = month, env = list(x = epsg_thresh)]
  uni = uni[month > 2002.99]
  setorderv(uni, c("month", return_mom))
  uni = uni[, data.table::first(.SD, mom_n), by = month]
  uni[, date_month := lubridate::ceiling_date(date, "month") - 1]
  # DEBUG
  # uni[, .(symbol, date, date_month, target)][date_month == as.Date("2024-01-31")]
  if (ret_sharpe) {
    uni[, weight := 1 / length(target), by = date_month]
    uni = uni[, .(ret = sum(target * weight)), by = date_month]
    sr = PerformanceAnalytics::SharpeRatio(as.xts.data.table(uni))
    # sr = PerformanceAnalytics::SortinoRatio(as.xts.data.table(uni))
    # sr = Return.annualized(as.xts.data.table(uni))
    return(sr[1, ])
  } else {
    return(uni)
  }
}
params = expand.grid(
  eps_thresh = c(-100, 0),         # min EPS to include stock in universe. -100 means all.
  close_raw_thresh = c(1, 10, 20), # min price to include stock in universe
  epsg_thresh = c(100, 200, 500),  # how many stocks to possibly include in universe
  return_mom = cols,  # return period to calculate to identify mean reversion
  eps_n = cols_epsg[c(1, 3, 6, 8)],# number of months to calculate EPS SD
  mom_n = c(10, 20, 50),           # number of stocks to include in universe
  coarse_n = c(1000, 2000),        # number of stocks to include in coarse universe
  rev_positive = c(TRUE, FALSE),   # include only stocks with positive revenue growth
  stringsAsFactors = FALSE)
# plan("multisession", workers = 4)
results = lapply(1:nrow(params), function(i) {
  # print(i)
  # i = 945
  x = params[i, ]
  print(x)
  backtest(universe, x[1, 1], x[1, 2], x[1, 3], x[1, 4], x[1, 5], x[1, 6], x[1, 7], x[1, 8])
})
ind_ = which.max(unlist(results))
params[ind_, ]
results[ind_]

# Check best backtest
best_ = backtest(universe, params[ind_, 1], params[ind_, 2], params[ind_, 3],
                 params[ind_, 4], params[ind_, 5], params[ind_, 6],
                 params[ind_, 7], params[ind_, 8], FALSE)
best_ret = best_[, .(ret = sum(target * (1 / length(target)))), by = date_month]
best_xts = as.xts.data.table(best_ret[, .(date_month, ret)])
Return.annualized(best_xts)
charts.PerformanceSummary(best_xts)
charts.PerformanceSummary(best_xts["/2020-01-01"])
charts.PerformanceSummary(best_xts["2015-01-01/"])

# Inspect result
results_dt = data.table(params, sr = unlist(results))
setorder(results_dt, -sr)
# Xreate boxplot withh SR for some variable
ggplot(results_dt, aes(x = as.factor(eps_thresh), y = sr)) +
  geom_boxplot()
ggplot(results_dt, aes(x = as.factor(close_raw_thresh), y = sr)) +
  geom_boxplot()
ggplot(results_dt, aes(x = as.factor(return_mom), y = sr)) +
  geom_boxplot()
ggplot(results_dt, aes(x = as.factor(eps_n), y = sr)) +
  geom_boxplot()
ggplot(results_dt, aes(x = as.factor(mom_n), y = sr)) +
  geom_boxplot()
ggplot(results_dt, aes(x = as.factor(coarse_n), y = sr)) +
  geom_boxplot()
ggplot(results_dt, aes(x = as.factor(rev_positive), y = sr)) +
  geom_boxplot()
# buy 50 shares with price > 1, EPS > 0 and lowest return in a month


# # FACTORS -----------------------------------------------------------------
# # Get factors
# path_to_parquet = fs::path(PATH, "predictors_daily", "factors", "prices_factors.parquet")
# events_symbols = c(events[, unique(symbol)], "SPY")
# prices_dt = open_dataset(path_to_parquet, format = "parquet") |>
#   dplyr::filter(symbol %in% events_symbols) |>
#   dplyr::filter(date > "2010-01-01") |>
#   dplyr::arrange(symbol, date) |>
#   collect()
# setDT(prices_dt)
# gc()
#
# # Filter dates and symbols
# prices_dt = unique(prices_dt, by = c("symbol", "date"))
# prices_n = prices_dt[, .N, by = symbol]
# prices_n = prices_n[which(prices_n$N > 700)]  # remove prices with only 700 or less observations
# prices_dt = prices_dt[symbol %chin% prices_n[, symbol]]
#
# # Remove symbols that have (almost) constant close prices
# prices_dt[, sd_roll := roll::roll_sd(close, 22 * 6), by = symbol]
# symbols_remove = prices_dt[sd_roll == 0, unique(symbol)]
# prices_dt = prices_dt[symbol %notin% symbols_remove]
#
# # Set key
# setkey(prices_dt, "symbol")
# setorder(prices_dt, symbol, date)
# key(prices_dt)
#
# # Calculate most liquid symbols
# prices_dt[, dollar_vol_rank := frankv(close_raw * volume, order = -1L), by = date]
#
# # SPY data
# # spy = open_dataset("/home/sn/data/equity/daily_fmp_all.csv", format = "csv") |>
# #   dplyr::filter(symbol == "SPY") |>
# #   dplyr::select(date, adjClose) |>
# #   dplyr::rename(close = adjClose) |>
# #   collect()
# # setDT(spy)
# # spy[, returns := close / shift(close) - 1]
# # spy = na.omit(spy)
#
# # Free memory
# gc()


# DATA --------------------------------------------------------------------
# Import daily OHLCV DATA
prices = qc_daily(
  file_path = "F:/lean/data/stocks_daily.csv",
  min_obs = 252 * 2,
  price_threshold = 1e-008,
  add_dv_rank = TRUE,
  add_day_of_month = FALSE
)

# Import fundamental data
fundamentals = read_parquet("F:/data/equity/us/predictors_daily/factors/fundamental_factors.parquet")

# clean fundamentals
fundamentals[, symbol := tolower(symbol)]
fundamentals = fundamentals[date > as.Date("2000-01-01")]
fundamentals[, acceptedDateTime := as.POSIXct(acceptedDate, tz = "America/New_York")]
fundamentals[, acceptedDate := as.Date(acceptedDateTime)]
fundamentals[, acceptedDateFundamentals := acceptedDate]
data.table::setnames(fundamentals, "date", "fundamental_date")
fundamentals = unique(fundamentals, by = c("symbol", "acceptedDate"))

# Checks
symbol_ = "msft"
fundamentals[symbol == "msft", .(symbol, acceptedDate, acceptedDateTime, acceptedDateFundamentals, EPS)]

# choose fundamental columns
# They used the following metrics:
# 1 – earnings
# 2 – return on equity
# 3 – return on assets
# 4 – accrual operating profitability to equity
# 5 - cash operating profitability to assets
# 6 – gross profit to assets
# 7 – net payout ratio
cols = colnames(fundamentals)
cols[grepl("free", cols, ignore.case = TRUE)]
fundamentals[, .SD, .SDcols = cols[grepl("earning|eps|rev", cols, ignore.case = TRUE)]]
cols_funds = c("symbol", "acceptedDateFundamentals", "acceptedDate",
               "EPSGrowth", "EPS",
               "fiveYRevenueGrowthPerShare",
               "returnOnAssets", "returnOnEquity",
               "grossProfitMargin", "payoutRatio",
               "freeCashFlowYield",
               "threeYRevenueGrowthPerShare")
falpha = fundamentals[, ..cols_funds]

# Import factors
fund

# calculate EPSGrowth rolling standard deviation
setorder(falpha, symbol, acceptedDate)
eps_years = 1:6
cols_epsg = paste0("EPSGrowth_sd_", eps_years)
falpha[, (cols_epsg) := lapply(eps_years, function(x) roll_sd(EPSGrowth, 4*x)),
   by = symbol]

# merge prices and selected fundamental data
prices[, date_prices := date]
dt = falpha[prices, on = c("symbol", "acceptedDate" = "date"), roll = Inf]

# calculate month returns
setorder(dt, symbol, date_prices)
returns_months = 1:12
cols = paste0("returns_", returns_months)
dt[, (cols) := lapply(returns_months, function(x) close / shift(close, x) - 1),
   by = symbol]

# keep columns wee need
cols_keep = c("symbol", "date_prices", "close", "close_raw", "volume",
              "EPSGrowth", "EPS", "threeYRevenueGrowthPerShare", cols_epsg, cols)
dt = dt[, ..cols_keep]
dt = na.omit(dt, cols = c("close", "close_raw", "EPS", "volume", cols))

# Define new var that is TRUE if it is last day of the month
setnames(dt, "date_prices", "date")
dt[, month := data.table::yearmon(date)]
dt[, last_month_day := last(date) == date, by = c("symbol", "month")]
first(dt, 10)

# Create monthly volume rank
dt[, dollar_volume := close * volume]
dt[, volume := NULL]
# dt[, rank_dollar_volume := frank(-dollar_volume, ties.method = "dense"), by = month]
# dt[, sum(dollar_volume, na.rm = TRUE), by = month]
# data.table::rank

# Optimize
universe = dt[last_month_day == TRUE]
setorder(universe, symbol, date)
universe[, target := shift(close, -1, type = "shift") / close - 1, by = symbol]
universe = na.omit(universe, cols = c("target"))
backtest = function(uni,
                    eps_thresh = -100,
                    close_raw_thresh = 0.01,
                    epsg_thresh = 100,
                    return_mom = "returns_6",
                    eps_n = "EPSGrowth_sd_1",
                    mom_n = 20,
                    coarse_n = 100,
                    rev_positive = TRUE,
                    ret_sharpe = TRUE) {
  # uni = copy(universe)
  if (rev_positive) {
    uni = uni[threeYRevenueGrowthPerShare > 0]
  }
  uni[, rank_volume := frank(-dollar_volume, ties.method = "dense"), by = month]
  uni[rank_volume < coarse_n]
  uni = uni[EPS > x, env = list(x = eps_thresh)]
  uni = uni[close_raw > close_raw_thresh, env = list(x = close_raw_thresh)]
  uni = na.omit(uni)
  setorderv(uni, c("month", eps_n), order = c(1L, -1L))
  uni = uni[, last(.SD, x), by = month, env = list(x = epsg_thresh)]
  uni = uni[month > 2002.99]
  setorderv(uni, c("month", return_mom))
  uni = uni[, first(.SD, mom_n), by = month]
  uni[, date_month := lubridate::ceiling_date(date, "month") - 1]
  # DEBUG
  # uni[, .(symbol, date, date_month, target)][date_month == as.Date("2024-01-31")]
  if (ret_sharpe) {
    uni[, weight := 1 / length(target), by = date_month]
    uni = uni[, .(ret = sum(target * weight)), by = date_month]
    sr = PerformanceAnalytics::SharpeRatio(as.xts.data.table(uni))
    # sr = PerformanceAnalytics::SortinoRatio(as.xts.data.table(uni))
    # sr = Return.annualized(as.xts.data.table(uni))
    return(sr[1, ])
  } else {
    return(uni)
  }
}
params = expand.grid(
  eps_thresh = c(-100, 0),         # min EPS to include stock in universe. -100 means all.
  close_raw_thresh = c(1, 10, 20), # min price to include stock in universe
  epsg_thresh = c(100, 200, 500),  # how many stocks to possibly include in universe
  return_mom = cols[c(1, 6, 12)],  # return period to calculate to identify mean reversion
  eps_n = cols_epsg[c(1, 3, 6, 9)],# number of months to calculate EPS SD
  mom_n = c(10, 20, 50),           # number of stocks to include in universe
  coarse_n = c(1000, 2000),        # number of stocks to include in coarse universe
  rev_positive = c(TRUE, FALSE),   # include only stocks with positive revenue growth
  stringsAsFactors = FALSE)
# plan("multisession", workers = 4)
results = future_lapply(1:nrow(params), function(i) {
  # print(i)
  # i = 945
  x = params[i, ]
  backtest(universe, x[1, 1], x[1, 2], x[1, 3], x[1, 4], x[1, 5], x[1, 6], x[1, 7], x[1, 8])
})
ind_ = which.max(unlist(results))
params[ind_, ]
results[ind_]

# Check best backtest
best_ = backtest(universe, params[ind_, 1], params[ind_, 2], params[ind_, 3],
                  params[ind_, 4], params[ind_, 5], params[ind_, 6],
                 params[ind_, 7], params[ind_, 8], FALSE)
best_ret = best_[, .(ret = sum(target * (1 / length(target)))), by = date_month]
best_xts = as.xts.data.table(best_ret[, .(date_month, ret)])
Return.annualized(best_xts)
charts.PerformanceSummary(best_xts)
charts.PerformanceSummary(best_xts["/2020-01-01"])
charts.PerformanceSummary(best_xts["2015-01-01/"])

# Portfolio returns
# portfolio = copy(universe)
# portfolio = dcast(portfolio, date_month ~ symbol, value.var = "target")
# portfolio[1:10, 1:10]
# setnafill(portfolio, fill = 0)
# portfolio[1:10, 1:10]
# weights = portfolio[, rowSums(.SD != 0), .SDcols = -"date_month"]
# portfolio_res = Return.portfolio(portfolio)
# Return.annualized(portfolio_res)
# charts.PerformanceSummary(portfolio_res)

# Save for Quantconnect
qc_data = best_[, .(date_month, symbol)]
qc_data[, symbol := paste0(symbol, collapse = "|"), by = date_month]
qc_data = unique(qc_data)
# dates = data.table(date_month = seq.Date(qc_data[, min(date_month)], qc_data[, max(date_month)], 1))
dates = data.table(date_month = seq.Date(qc_data[, min(date_month)], qc_data[, max(date_month)], 1))
qc_data = qc_data[dates, on = "date_month", roll = Inf]
blob_key = "0M4WRlV0/1b6b3ZpFKJvevg4xbC/gaNBcdtVZW+zOZcRi0ZLfOm1v/j2FZ4v+o8lycJLu1wVE6HT+ASt0DdAPQ=="
endpoint = "https://snpmarketdata.blob.core.windows.net/"
BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)
cont = storage_container(BLOBENDPOINT, "qc-backtest")
file_name_ =  paste0("eps_price_dis.csv")
storage_write_csv(qc_data, cont, file_name_, col_names = FALSE)


# p = 0.0000001
# p_hat = 0.001
# (p_hat - p) / p


