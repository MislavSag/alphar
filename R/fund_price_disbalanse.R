library(data.table)
library(arrow)
library(roll)
library(PerformanceAnalytics)
library(AzureStor)
library(future.apply)


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

# adjust all columns
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

# Remove observations where open, high, low, close columns are below 1e-008
prices = prices[open > 1e-008 & high > 1e-008 & low > 1e-008 & close > 1e-008]

# Remove missing values
prices = na.omit(prices)

# Keep only symbol with at least 2 years of data
symbol_keep = prices[, .N, symbol][N >= 2 * 252, symbol]
prices = prices[symbol %chin% symbol_keep]

# keep 100 most liquid at every date
# 200 by volume => cca 20 mil rows
# 100 by volume => cca 13 mil rows
# 50 by volume => cca 8 mil rows
prices[, dollar_volume := close * volume]
setorder(prices, date, -dollar_volume)
liquid_symbols = prices[, .(symbol = first(symbol, 100)), by = date]
liquid_symbols = liquid_symbols[, unique(symbol)]
sprintf("We keep %f percent of data",
        length(liquid_symbols) / prices[, length(unique(symbol))] * 100)
prices = prices[symbol %chin% liquid_symbols]
prices[, dollar_volume := NULL]

# Sort
setorder(prices, symbol, date)

# free memory
gc()


# FUNDAMENTAL DATA --------------------------------------------------------
# Import fundamental data
fundamentals = read_parquet(fs::path(PATH,
                                     "predictors_daily",
                                     "factors",
                                     "fundamental_factors",
                                     ext = "parquet"))

# clean fundamentals
fundamentals[, symbol := tolower(symbol)]
fundamentals = fundamentals[date > as.Date("2000-01-01")]
fundamentals[, acceptedDateTime := as.POSIXct(acceptedDate, tz = "America/New_York")]
fundamentals[, acceptedDate := as.Date(acceptedDateTime)]
fundamentals[, acceptedDateFundamentals := acceptedDate]
data.table::setnames(fundamentals, "date", "fundamental_date")
fundamentals = unique(fundamentals, by = c("symbol", "acceptedDate"))

# choose fundamental columns
cols = colnames(fundamentals)
fundamentals[, .SD, .SDcols = cols[grepl("earning|eps", cols, ignore.case = TRUE)]]
cols_funds = c("symbol", "acceptedDateFundamentals", "acceptedDate", "EPSGrowth", "EPS")
falpha = fundamentals[, ..cols_funds]

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
              "EPSGrowth", "EPS", cols_epsg, cols)
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
                    ret_sharpe = TRUE) {
  # uni = copy(universe)
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
    uni = uni[, .(ret = sum(target * (1 / length(target)))), by = date_month]
    # sr = PerformanceAnalytics::SharpeRatio(as.xts.data.table(uni))
    sr = PerformanceAnalytics::SortinoRatio(as.xts.data.table(uni))
    # sr = Return.annualized(as.xts.data.table(uni))
    return(sr[1, ])
  } else {
    return(uni)
  }
}
params = expand.grid(
  eps_thresh = c(-100, 0),         # min EPS to include stock in universe
  close_raw_thresh = c(1, 10, 20), # min price to include stock in universe
  epsg_thresh = c(100, 200, 500),  # how many stocks to possibly include in universe
  return_mom = cols[c(1, 6, 12)],  # return period to calculate to identify mean reversion
  eps_n = cols_epsg[c(1, 3, 6)],   # number of months to calculate EPS SD
  mom_n = c(10, 20, 50),           # number of stocks to include in universe
  coarse_n = c(1000, 2000),         # number of stocks to include in coarse universe
  stringsAsFactors = FALSE)
# plan("multisession", workers = 4)
results = future_lapply(1:nrow(params), function(i) {
  # print(i)
  # i = 945
  x = params[i, ]
  backtest(universe, x[1, 1], x[1, 2], x[1, 3], x[1, 4], x[1, 5], x[1, 6], x[1, 7])
})
ind_ = which.max(unlist(results))
params[ind_, ]
results[ind_]

# Check best backtest
best_ = backtest(universe, params[ind_, 1], params[ind_, 2], params[ind_, 3],
                  params[ind_, 4], params[ind_, 5], params[ind_, 6],
                 params[ind_, 7], FALSE)
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
