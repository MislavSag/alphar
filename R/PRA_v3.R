library(data.table)
library(tiledb)
library(lubridate)
library(rvest)
library(ggplot2)
library(future.apply)
library(PerformanceAnalytics)
library(AzureStor)
library(data.table)
library(tiledb)
library(rvest)



# SET UP ------------------------------------------------------------------
# globals
DATAPATH      = "F:/lean_root/data/all_stocks_hour.csv"
URIEXUBER     = "F:/equity-usa-hour-exuber"
NASPATH       = "C:/Users/Mislav/SynologyDrive/trading_data"



# UNIVERSE ----------------------------------------------------------------
# SPY constitues
spy_const = fread(file.path(NASPATH, "spy.csv"))
symbols_spy = unique(spy_const)

# SP 500
sp500_changes = read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies") %>%
  html_elements("table") %>%
  .[[2]] %>%
  html_table()
sp500_changes = sp500_changes[-1, c(1, 2, 4)]
sp500_changes = as.data.table(sp500_changes)
sp500_changes[, Date := as.Date(Date, format = "%b %d, %Y")]
sp500_changes = sp500_changes[Date < as.Date("2009-06-28")]
sp500_changes_symbols = unlist(sp500_changes[, 2:3], use.names = FALSE)
sp500_changes_symbols = unique(sp500_changes_symbols)
sp500_changes_symbols = sp500_changes_symbols[sp500_changes_symbols != ""]

# SPY constitues + SP 500
symbols_sp500 = unique(c(symbols_spy, sp500_changes_symbols))
symbols_sp500 = tolower(symbols_sp500)
symbols_sp500 = c("spy", symbols_sp500)



# IMPORT DATA -------------------------------------------------------------
# import QC data
col = c("date", "open", "high", "low", "close", "volume", "close_adj", "symbol")
dt = fread(DATAPATH, col.names = col)

# filter symbols
# dt = dt[symbol %chin% symbols_sp500]

# set time zone
attributes(dt)
if (!("tzone" %in% names(attributes(dt)))) {
  setattr(dt, 'tzone', "America/New_York")
} else if (attributes(dt)[["tzone"]] != "America/New_York") {
  dt[, date := force_tz(date, "America/New_York")]
}
attributes(dt)

# unique
dt <- unique(dt, by = c("symbol", "date"))

# adjust all columns
unadjustd_cols = c("open", "high", "low")
adjusted_cols = paste0(unadjustd_cols, "_adj")
dt[, (adjusted_cols) := lapply(.SD, function(x) (close_adj / close) * x), .SDcols = unadjustd_cols]

# remove NA values
dt = na.omit(dt)

# order
setorder(dt, symbol, date)

# free resources
gc()





# SET UP ------------------------------------------------------------------
# globals
NAS = "C:/Users/Mislav/SynologyDrive/trading_data"

# parameters
windows = c(7, 7 * 5, 7 * 5 * 2, 7 * 22, 7 * 22 * 3, 7 * 22 * 6,  7 * 22 * 12,
            7 * 22 * 12 * 2, 7 * 22 * 12 * 4)

# date segments
GFC <- c("2007-01-01", "2010-01-01")
AFTERGFCBULL <- c("2010-01-01", "2015-01-01")
COVID <- c("2020-01-01", "2021-06-01")
AFTER_COVID <- c("2021-06-01", "2022-01-01")
CONTRACTION_2022 <- c("2022-01-01", "2022-03-15")
NEW <- c("2023-01-01", Sys.Date())



# UNIVERSES ---------------------------------------------------------------
# keep only xx symbols
qc1000 = fread(file.path(NAS, "QC1000.csv"))
setorder(qc1000, date, -`dollar volume`)
qc500 = qc1000[, head(.SD, 500), by = date]
qc500 = qc500[, sum(`dollar volume`), by = .(symbol, substr(date, 1, 6))][order(-V1)]
symbols_qc500 = qc500[, unique(symbol)]

# SPY constitues
spy_const = fread(file.path(NAS, "spy.csv"))
symbols_spy = unique(spy_const$ticker)

# SP 500
sp500_changes = read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies") %>%
  html_elements("table") %>%
  .[[2]] %>%
  html_table()
sp500_changes = sp500_changes[-1, c(1, 2, 4)]
sp500_changes$Date = as.Date(sp500_changes$Date, format = "%b %d, %Y")
sp500_changes = sp500_changes[sp500_changes$Date < as.Date("2009-06-28"), ]
sp500_changes_symbols = unlist(sp500_changes[, 2:3], use.names = FALSE)
sp500_changes_symbols = unique(sp500_changes_symbols)
sp500_changes_symbols = sp500_changes_symbols[sp500_changes_symbols != ""]

# SPY constitues + SP 500
symbols_sp500 = unique(c(symbols_spy, sp500_changes_symbols))
symbols_sp500 = tolower(symbols_sp500)
symbols_sp500 = c("spy", symbols_sp500)



# QC HOUR MARKET DATA -----------------------------------------------------
# import QC hour market data
dt = fread("F:/lean_root/data/all_stocks_hour.csv")

# change column names
old_names = c("Symbol", "Date", "Open", "High", "Low", "Close", "Volume",
              "Adj Close")
new_names = tolower(old_names)
new_names[length(new_names)] = "close_adj"
setnames(dt, old_names, new_names)

# filter SP500 symbols
dt = dt[symbol %chin% symbols_sp500]

# free memory
gc()

# set time zone
attributes(dt$date)
if (!("tzone" %in% names(attributes(dt$date)))) {
  setattr(dt$date, 'tzone', "America/New_York")
} else if (attributes(dt$date)[["tzone"]] != "America/New_York") {
  dt[, date := force_tz(date, "America/New_York")]
}
attributes(dt$date)

# unique
dt <- unique(dt, by = c("symbol", "date"))

# adjust all columns
unadjustd_cols = c("open", "high", "low")
adjusted_cols = paste0(unadjustd_cols, "_adj")
dt[, (adjusted_cols) := lapply(.SD, function(x) (close_adj / close) * x), .SDcols = unadjustd_cols]

# extract SPY
spy = dt[symbol == "spy"]



# PREDICTORS --------------------------------------------------------------
# import pra indicators
arr <- tiledb_array("F:/equity-usa-hour-pra",
                    as.data.frame = TRUE,
                    query_layout = "UNORDERED",
                    selected_ranges = list(symbol = cbind(symbols_sp500, symbols_sp500)))
pra = arr[]
pra = as.data.table(pra)

# free resources
gc()

# set timezone
setattr(pra$date, "tzone", "UTC")

# order data
setorder(pra, symbol, date)

# crate dummy variables
cols <- paste0("pra_", windows)
cols_above_999 <- paste0("pr_above_dummy_", windows)
pra[, (cols_above_999) := lapply(.SD, function(x) ifelse(x > 0.999, 1, 0)), .SDcols = cols]
cols_below_001 <- paste0("pr_below_dummy_", windows)
pra[, (cols_below_001) := lapply(.SD, function(x) ifelse(x < 0.001, 1, 0)), .SDcols = cols]
cols_net_1 <- paste0("pr_below_dummy_net_", windows)
pra[, (cols_net_1) := pra[, ..cols_above_999] - pra[, ..cols_below_001]]

cols_above_99 <- paste0("pr_above_dummy_99_", windows)
pra[, (cols_above_99) := lapply(.SD, function(x) ifelse(x > 0.99, 1, 0)), .SDcols = cols]
cols_below_01 <- paste0("pr_below_dummy_01_", windows)
pra[, (cols_below_01) := lapply(.SD, function(x) ifelse(x < 0.01, 1, 0)), .SDcols = cols]
cols_net_2 <- paste0("pr_below_dummy_net_0199", windows)
pra[, (cols_net_2) := pra[, ..cols_above_99] - pra[, ..cols_below_01]]

cols_above_97 <- paste0("pr_above_dummy_97_", windows)
pra[, (cols_above_97) := lapply(.SD, function(x) ifelse(x > 0.97, 1, 0)), .SDcols = cols]
cols_below_03 <- paste0("pr_below_dummy_03_", windows)
pra[, (cols_below_03) := lapply(.SD, function(x) ifelse(x < 0.03, 1, 0)), .SDcols = cols]
cols_net_3 <- paste0("pr_below_dummy_net_0397", windows)
pra[, (cols_net_3) := pra[, ..cols_above_97] - pra[, ..cols_below_03]]

cols_above_95 <- paste0("pr_above_dummy_95_", windows)
pra[, (cols_above_95) := lapply(.SD, function(x) ifelse(x > 0.95, 1, 0)), .SDcols = cols]
cols_below_05 <- paste0("pr_below_dummy_05_", windows)
pra[, (cols_below_05) := lapply(.SD, function(x) ifelse(x < 0.05, 1, 0)), .SDcols = cols]
cols_net_4 <- paste0("pr_below_dummy_net_0595", windows)
pra[, (cols_net_4) := pra[, ..cols_above_95] - pra[, ..cols_below_05]]

# get risk measures
indicators <- pra[symbol != "SPY", lapply(.SD, sum, na.rm = TRUE),
                  .SDcols = c(colnames(pra)[grep("pr_\\d+", colnames(pra))],
                              cols_above_999, cols_above_99, cols_below_001, cols_below_01,
                              cols_above_97, cols_below_03, cols_above_95, cols_below_05,
                              cols_net_1, cols_net_2, cols_net_3, cols_net_4),
                  by = .(date)]
indicators <- unique(indicators, by = c("date"))
setorder(indicators, "date")
file_name <- paste0(NAS,
                    "macro_predictors_pra_",
                    format(Sys.time(), format = "%Y%m%d%H%M%S"),
                    ".csv")
fwrite(indicators, file_name)

# change timezone
setattr(indicators$date, "tzone", "America/New_York")

# merge indicators and spy
attr(spy$date, "tzone") == attr(indicators$date, "tzone")
backtest_data <- spy[indicators, on = 'date']
backtest_data <- na.omit(backtest_data)
setorder(backtest_data, "date")



# VISUALIZATION -----------------------------------------------------------
# check individual stocks
sample_ <- pra[symbol == "aapl"]
sample_ = dt[symbol == "aapl"][sample_, on = "date"]
v_buy <- sample_[pr_above_dummy_95_924 == 1, date, ]
v_sell <- sample_[pr_below_dummy_05_924 == 1, date, ]
ggplot(sample_, aes(x = date)) +
  geom_line(aes(y = close_adj)) +
  geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")
ggplot(sample_[date %between% GFC], aes(x = date)) +
  geom_line(aes(y = close_adj)) +
  geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")
ggplot(sample_[date %between% AFTERGFCBULL], aes(x = date)) +
  geom_line(aes(y = close_adj)) +
  geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")
ggplot(sample_[date %between% COVID], aes(x = date)) +
  geom_line(aes(y = close_adj)) +
  geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")
ggplot(sample_[date %between% AFTER_COVID], aes(x = date)) +
  geom_line(aes(y = close_adj)) +
  geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")
ggplot(sample_[date %between% NEW], aes(x = date)) +
  geom_line(aes(y = close_adj)) +
  geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")



# BUY / SELL WF OPTIMIZATION ----------------------------------------------
# prepare data for WF optimization
backtest_data_wf2 = backtest_data[, .(date, close = close_adj, pr_above_dummy_3696)]
backtest_data_wf2[, returns := close / shift(close) - 1]
backtest_data_wf2 = na.omit(backtest_data_wf2)

# walk forward buy / sell thresholds backtest
backtest <- function(returns, indicator, threshold_buy, threshold_sell, return_cumulative = TRUE) {
  # debug
  # returns = backtest_data_wf2[, returns]
  # indicator = backtest_data_wf2[, pr_above_dummy_1848]
  # threshold_buy = 2
  # threshold_sell = 5
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1])) {
      sides[i] <- NA
    } else if (indicator[i-1] > threshold_sell) { # 3
      sides[i] <- 0
    } else if ((is.na(sides[i-1]) ||sides[i-1] == 0) && indicator[i-1] < threshold_buy) {
      sides[i] <- 1
    } else {
      sides[i] <- 1
    }
  }
  sides <- ifelse(is.na(sides), 1, sides)
  returns_strategy <- returns * sides
  if (return_cumulative) {
    return(PerformanceAnalytics::Return.cumulative(returns_strategy))
  } else {
    return(returns_strategy)
  }
}

# parameters
threshold_buy = 1:150
threshold_sell = 1:150
params = expand.grid(threshold_buy, threshold_sell, stringsAsFactors = FALSE)
colnames(params) = c("threshold_buy", "threshold_sell")

#
threshold_buy_params = params[, "threshold_buy"]
threshold_sell_params = params[, "threshold_sell"]
returns_ = backtest_data_wf2[, returns]
indicator_ = backtest_data_wf2[, 3]
plot(backtest_data_wf2[, .(date, pr_above_dummy_3696)], type = "l")
date_ = backtest_data_wf2[, date]
results = vapply(seq_along(threshold_buy_params), function(i) {
  backtest(returns_, indicator, threshold_buy_params[i], threshold_sell_params[i])
}, FUN.VALUE = numeric(1L))
result_grid = cbind(params, car = results)

# inspect results
result_grid[order(result_grid$car), ]
best_params = tail(result_grid[order(result_grid$car), ], 1)
best_backtset = backtest(returns_, indicator, best_params[1], best_params[2], FALSE)
best_backtset = data.table(cbind.data.frame(date = date_, benchmark = returns, strategy = best_backtset))
charts.PerformanceSummary(as.xts.data.table(best_backtset))


# CHAT GPT APPROACH -------------------------------------------------------
# prepare data for WF optimization
backtest_data_wf2 = backtest_data[, .(date, close = close_adj, pr_above_dummy_3696)]
backtest_data_wf2[, returns := close / shift(close) - 1]
backtest_data_wf2 = na.omit(backtest_data_wf2)

# optimize function
indicator_ = backtest_data_wf2[, pr_above_dummy_3696]
returns_   = backtest_data_wf2[, returns]
date_ = backtest_data_wf2[, date]
backtest <- function(returns, indicator, threshold_buy, threshold_sell, return_cumulative = TRUE) {
  # debug
  # returns = backtest_data_wf2[, returns]
  # indicator = backtest_data_wf2[, pr_above_dummy_1848]
  # threshold_buy = 2
  # threshold_sell = 5
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1])) {
      sides[i] <- NA
    } else if (indicator[i-1] > threshold_buy) { # 3
      sides[i] <- 1
    } else if (indicator[i-1] < threshold_sell) {
      sides[i] <- 0
    }
  }
  sides <- ifelse(is.na(sides), 1, sides)
  returns_strategy <- returns * sides
  if (return_cumulative) {
    return(PerformanceAnalytics::Return.cumulative(returns_strategy))
  } else {
    return(returns_strategy)
  }
}

backtest <- function(returns, indicator, threshold_buy, threshold_sell, return_cumulative = TRUE) {
  # debug
  # returns = backtest_data_wf2[, returns]
  # indicator = backtest_data_wf2[, pr_above_dummy_1848]
  # threshold_buy = 2
  # threshold_sell = 5
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1])) {
      sides[i] <- NA
    } else if (indicator[i-1] > threshold_sell) { # 3
      sides[i] <- 0
    } else if ((is.na(sides[i-1]) ||sides[i-1] == 0) && indicator[i-1] < threshold_buy) {
      sides[i] <- 1
    } else {
      sides[i] <- 1
    }
  }
  sides <- ifelse(is.na(sides), 1, sides)
  returns_strategy <- returns * sides
  if (return_cumulative) {
    return(PerformanceAnalytics::Return.cumulative(returns_strategy))
  } else {
    return(returns_strategy)
  }
}

optimize_strategy = function(thresholds, out = "car") {
  # debug
  # thresholds = c(48, 50)

  # Unpack the thresholds
  buy_thres <- thresholds[1]
  sell_thres <- thresholds[2]

  # # Define the strategy
  # signal <- ifelse(indicator_ > buy_thres, 1,
  #                  ifelse(indicator_ < sell_thres, 0, NA))
  # # signal <- na.locf(signal, na.rm=FALSE)
  # ret <- lag(signal) * returns_
  ret = backtest(returns_, indicator_, buy_thres, sell_thres)

  if (out == "car") {
    return(-ret)
  } else if (out == "sharpe") {
    sharpe_ratio <- SharpeRatio.annualized(R = as.xts.data.table(as.data.table(cbind.data.frame(date_, ret))),
                                           Rf = 0, scale = sqrt(252*7))
    return(-sharpe_ratio)
  } else {
    return(backtest(returns_, indicator_, buy_thres, sell_thres, FALSE))
  }
}

# Optimize the strategy on the training data
result_optim <- optim(c(20, 20), optimize_strategy, gr=NULL, method="L-BFGS-B",
                      lower=c(1, 1), upper=c(200, 200))
result_optim

# inspect results
result_optim_best = optimize_strategy(c(20, 26), "r")
best_optim_backtset = data.table(cbind.data.frame(date = date_,
                                                  benchmark = returns_,
                                                  strategy = result_optim_best))
charts.PerformanceSummary(as.xts.data.table(best_optim_backtset))

# fast optimization
# optimize function
indicator_ = backtest_data_wf2[, pr_above_dummy_3696]
returns_   = backtest_data_wf2[, returns]
date_ = backtest_data_wf2[, date]
optimize_fast = function(thresholds, out = "car") {
  # Unpack the thresholds
  buy_thres <- thresholds[1]
  sell_thres <- thresholds[2]

  # Define the strategy
  signal <- ifelse(indicator_ > buy_thres, 1,
                   ifelse(indicator_ < sell_thres, 0, NA))
  # signal <- na.locf(signal, na.rm=FALSE)
  ret <- lag(signal) * returns_

  if (out == "car") {
    return(-Return.cumulative(ret))
  } else if (out == "sharpe") {
    sharpe_ratio <- SharpeRatio.annualized(R = as.xts.data.table(as.data.table(cbind.data.frame(date_, ret))),
                                           Rf = 0, scale = sqrt(252*7))
    return(-sharpe_ratio)
  } else {
    return(ret)
  }
}

# Optimize the strategy on the training data
result <- optim(c(20, 20), optimize_fast, gr=NULL, method="L-BFGS-B", lower=c(1, 1), upper=c(200, 200))
result

# inspect results
result_best = optimize_strategy(c(50, 50), "r")
best_backtset = data.table(cbind.data.frame(date = date_, benchmark = returns, strategy = result_best))
charts.PerformanceSummary(as.xts.data.table(best_backtset))

# save data for QC
qc_data = backtest_data_wf2[, .(date, indicator = pr_above_dummy_3696)]
# cols <- setdiff(colnames(qc_data), "date")
# qc_data[, (cols) := lapply(.SD, shift), .SDcols = cols] # VERY IMPORTANT STEP !
qc_data <- na.omit(qc_data)
qc_data[, date := as.character(date)]
file_name <- paste0("pr500-", format(Sys.time(), format = "%Y%m%d%H%M%S"), ".csv")
KEY = Sys.getenv("BLOB-KEY-SNP")
ENDPOINT = Sys.getenv("BLOB-ENDPOINT-SNP")
bl_endp_key <- storage_endpoint(ENDPOINT, key=KEY)
cont <- storage_container(bl_endp_key, "qc-backtest")
storage_write_csv(qc_data, cont, file_name)
file_name


ret = backtest(returns_, indicator_, 50, 50, return_cumulative = FALSE)
best_backtset = data.table(cbind.data.frame(date = date_, benchmark = returns, strategy = ret))
charts.PerformanceSummary(as.xts.data.table(best_backtset))
