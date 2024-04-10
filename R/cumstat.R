library(data.table)
library(fs)
library(arrow)
library(duckdb)
library(finfeatures)
library(PerformanceAnalytics)
library(ggplot2)
library(runner)
library(tsDyn)
library(doParallel)
library(mlr3misc)


# Main function to construct short-both portfolio
short_both_portfolio = function(freq = c("day", "hour"),
                                symbol_long = "spxl",
                                symbol_short = "spxs",
                                symbol_benchmark = "spy") {
  # dt = copy(prices)
  # symbol_long = "spxl"
  # symbol_short = "spxs"
  # symbol_benchmark = "spy"
  # freq = "hour"

  # Import data for freq
  if (freq == "day") {
    path_to_csv = "F:/lean/data/stocks_daily.csv"
  } else {
    path_to_csv = "F:/lean/data/stocks_hour.csv"
  }
  symbols = c(symbol_long, symbol_short, symbol_benchmark)
  symbols_sql = paste(sprintf("'%s'", symbols), collapse = ", ")
  con = dbConnect(duckdb::duckdb())
  query = sprintf("
  SELECT *
  FROM read_csv_auto('%s')
  WHERE Symbol IN (%s)
", path_to_csv, symbols_sql)
  dt_ = dbGetQuery(con, query)
  duckdb::dbDisconnect(con, shutdown=TRUE)
  setDT(dt_)
  setnames(dt_, gsub(" ", "_", c(tolower(colnames(dt_)))))

  # Filter by symbols min date
  min_date = dt_[, .(date = min(date, na.rm = TRUE)), by = symbol]
  min_date = min_date[, max(date)]
  print(min_date)
  dt_ = dt_[date > min_date]

  # Remove duplicates
  dt_ = unique(dt_, by = c("symbol", "date"))

  # Adjust all columns
  dt_[, adj_rate := adj_close / close]
  dt_[, let(
    open = open*adj_rate,
    high = high*adj_rate,
    low = low*adj_rate
  )]
  setnames(dt_, "close", "close_raw")
  setnames(dt_, "adj_close", "close")
  dt_[, let(adj_rate = NULL)]
  setcolorder(dt_, c("symbol", "date", "open", "high", "low", "close", "volume"))

  # Remove missing values
  dt_ = na.omit(dt_)

  # Sort
  setorder(dt_, symbol, date)

  # Calculate daily returns
  dt_[, let(
    returns     = (close / shift(close, 1)) - 1,
    log_returns = c(NA, diff(log(close)))
  ), by = symbol]
  dt_ = na.omit(dt_)

  # Short-both strategy: Assuming equal capital allocation to both, and rebalancing daily
  dt_ = dcast(dt_, date ~ symbol, value.var = c("log_returns", "returns"))
  dt_[, let(
    strategy     = -0.5 * x1 + -0.5 * y1,
    strategy_log = -0.5 * x2 + -0.5 * y2
  ), env = list(x1 = paste0("returns_", symbol_long),
                y1 = paste0("returns_", symbol_short),
                x2 = paste0("log_returns_", symbol_long),
                y2 = paste0("log_returns_", symbol_short))]

  # Merge benchmark
  dt_ = dt_[, .(date, strategy, strategy_log, returns = x),
            env = list(x = paste0("returns_", symbol_benchmark))]
  dt_ = na.omit(dt_)

  return(dt_)
}

# Parameters
# Equites:      ["SPXL", "SPXS"] # from 2008/10
# Bonds 10y:    ["TMF", "TMV"]   # from 2009/06
# HY Bonds 10y: ["HYG", "SJB"]   # from 2011/04
# Natural Gas:  ["HYG", "SJB"]   # from 2012/03
# Nasdaq:       ["HYG", "SJB"]   # from 2010/03
params = data.table(
  symbols_long = c("spxl", "tmf", "hyg", "ugaz", "tqqq"),
  symbols_short = c("spxs", "tmv", "sjb", "dgaz", "sqqq"),
  symbols_benchmark = c("spy", "tlt", "lqd", "ung", "qqq")
)

# Get short-both portfolio for params symbols
short_both_portfolios = lapply(1:nrow(params), function(i) {
  short_both_portfolio("hour",
                       params[i, symbols_long],
                       params[i, symbols_short],
                       params[i, symbols_benchmark])
})
names(short_both_portfolios) = c("SPY", "Bonds", "HY Bonds", "Nat Gas", "Nasdaq")
short_both_portfolios = rbindlist(short_both_portfolios, idcol = "asset")

# Get portfolio results
ar = function(x) as.data.table(
  table.AnnualizedReturns(as.xts.data.table(x[, .(date, strategy, strategy_log)])),
  keep.rownames = TRUE)
sb_results = tryCatch({short_both_portfolios[, ar(.SD), by = asset]}, error = function(e) NULL)
if (is.null(sb_results)) {
  sb_results = short_both_portfolios[, prod(1 + strategy) - 1, by = asset]
} else {
  sb_results[grep("Sharpe", rn)]
}

# Rolling simple linear regression
# short_both_portfolios[, strategy_diff := strategy - shift(strategy), by = asset]
# short_both_portfolios = na.omit(short_both_portfolios)
# short_both_portfolios[, c("alpha", "beta1", "beta2") := as.data.frame(
#   roll::roll_lm(returns, cbind(strategy, strategy_log, strategy_diff), .N, min_obs = 252)$coefficients),
#                       by = asset]
# short_both_portfolios[, prediction := alpha + beta1 * returns + beta2 * returns, by = asset]
# short_both_portfolios[, signal := as.integer(shift(prediction) >= 0), by= asset]
# short_both_portfolios[, strategy_ := returns * signal]
# x = na.omit(as.xts.data.table(short_both_portfolios[asset == "SPY", .(date, strategy, returns)]))
# charts.PerformanceSummary(x)

# Rolling threshold VAR
df_runner = short_both_portfolios[asset == "SPY", .(date, strategy, returns)]
df_runner[, roll_sd := roll::roll_sd(strategy, 7 * 5)]
df_runner = df_runner[, .(date, roll_sd, returns)]
df_runner = na.omit(df_runner)
setDF(df_runner)
cl = makeCluster(8)
clusterExport(cl, "df_runner", envir = environment())
clusterEvalQ(cl, {lapply(c("tsDyn", "data.table"), require, character.only = TRUE)})
roll_preds = runner(
    x = df_runner,
    f = function(x) {
      # debug
      # x = df_runner[1:(7 * 22 * 3), ]

      # Check if already downloaded
      file_name = paste0(
        "F:/predictors/cumstat_hour/",
        strftime(max(x$date, na.rm = TRUE), "%Y%m%d%H%M%S"), ".csv"
        )
      if (file.exists(file_name)) return(NULL)

      # TVAR (1)
      tv = tryCatch(
        TVAR(
          data    = x[, 3:2], # x[, 3:2],
          lag     = 3,     # Number of lags to include in each regime
          model   = "TAR", # Whether the transition variable is taken in levels (TAR) or difference (MTAR)
          nthresh = 2,     # Number of thresholds
          thDelay = 1,     #  'time delay' for the threshold variable
          trim    = 0.05,  # trimming parameter indicating the minimal percentage of observations in each regime
          mTh     = 2,     # Combination of variables with same lag order for the transition variable. Either a single value (indicating which variable to take) or a combination
          plot    = FALSE
        ),
      error = function(e)
        NULL
      )
      if (is.null(tv)) {
        return(NA)
      } else {
        # tv1$coeffmat
        tv_pred = predict(tv)[, 1]
        names(tv_pred) = paste0("predictions_", 1:5)
        thresholds = tv$model.specific$Thresh
        names(thresholds) = paste0("threshold_", seq_along(thresholds))
        coef_1 = tv$coefficients$Bdown[1, ]
        names(coef_1) = paste0(names(coef_1), "_bdown")
        coef_2 = tv$coefficients$Bmiddle[1, ]
        names(coef_2) = paste0(names(coef_2), "_bmiddle")
        coef_3 = tv$coefficients$Bup[1, ]
        names(coef_3) = paste0(names(coef_3), "_bup")
        res_ = cbind.data.frame(as.data.frame(as.list(thresholds)),
                                as.data.frame(as.list(tv_pred)),
                                data.frame(aic = AIC(tv)),
                                data.frame(bic = BIC(tv)),
                                data.frame(loglik = logLik(tv)),
                                as.data.frame(as.list(coef_1)),
                                as.data.frame(as.list(coef_2)),
                                as.data.frame(as.list(coef_3)))
        fwrite(res_, file_name)
        return(res_)
      }
    },
    lag = 0L,
    k = 462, # 7 * 22 * 3
    cl = cl,
    simplify = FALSE,
    na_pad = TRUE
  )
stopCluster(cl)
saveRDS(roll_preds, "F:/predictors/short_both/rolling_tvar_spy_hour_sd.rds")
# saveRDS(roll_preds, "F:/predictors/short_both/rolling_tvar_spy_daily.rds")
# saveRDS(roll_preds, "F:/predictors/short_both/rolling_tvar_spy_daily_log.rds")
# saveRDS(roll_preds, "F:/predictors/short_both/rolling_tvar_spy_daily_sd.rds")
# saveRDS(roll_preds, "F:/predictors/short_both/rolling_tvar_spy_daily_sd_strategy.rds")
# saveRDS(roll_preds, "F:/predictors/short_both/rolling_tvar_tlt_daily_sd.rds")
# roll_preds = readRDS("F:/predictors/short_both/rolling_tvar_spy_daily_sd.rds")

# list.files("F:/predictors/short_both")
length(roll_preds)
lengths(roll_preds)
roll_preds[[1000]]
roll_preds_dt = lapply(roll_preds, as.data.table)
roll_preds_dt = rbindlist(roll_preds_dt, fill = TRUE)
roll_preds_dt = cbind(as.data.table(df_runner), roll_preds_dt)
roll_preds_dt[, let(V1 = NULL)]

roll_preds_dt[, signal := as.integer(roll_sd <= threshold_1 | predictions_1 > 0)]
roll_preds_dt[, strategy_tvar := returns * shift(signal)]
roll_preds_dt = na.omit(roll_preds_dt)
roll_preds_xts = na.omit(as.xts.data.table(roll_preds_dt[, .(date, strategy_tvar, returns)]))
charts.PerformanceSummary(roll_preds_xts)
table.AnnualizedReturns(roll_preds_xts)

# Save for QC
qc_data = lapply(roll_preds, as.data.table)
qc_data = rbindlist(qc_data, fill = TRUE)
qc_data = cbind(as.data.table(df_runner)[252:nrow(df_runner)], qc_data)
qc_data = qc_data[, .(date, roll_sd, threshold_2, predictions_1)]
cols_shift = c("roll_sd", "threshold_2", "predictions_1")
qc_data = qc_data[, (cols_shift) := shift(.SD), .SDcols = cols_shift]
qc_data = na.omit(qc_data)
qc_data[, let(date = as.character(date))]
blob_key = "0M4WRlV0/1b6b3ZpFKJvevg4xbC/gaNBcdtVZW+zOZcRi0ZLfOm1v/j2FZ4v+o8lycJLu1wVE6HT+ASt0DdAPQ=="
endpoint = "https://snpmarketdata.blob.core.windows.net/"
BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)
cont = storage_container(BLOBENDPOINT, "qc-backtest")
storage_write_csv(qc_data, cont, "short_both.csv")


# Simple threshold backtest
x = na.omit(short_both_portfolios[asset == "SPY", .(asset, date, strategy, returns)])
charts.PerformanceSummary(as.xts.data.table(x[, .(date, strategy)]))
x[, strategy_ma := TTR::EMA(strategy, 5)]
x[, strategy_sd := roll::roll_sd(strategy, 10)]
plot(as.xts.data.table(x[, .(date, strategy_ma)]))
# x[, strategy_lag := shift(strategy)]
# x[, strategy_sma_lag := shift(strategy_sma)]
# x[, strategy_lag_diff := c(NA, diff(strategy_lag))]
x[, signal := as.integer(shift(strategy_ma) < .00005)]
x[, let(strategy = returns * signal)]
x = na.omit(as.xts.data.table(x[, .(date, strategy, returns)]))
charts.PerformanceSummary(x)
table.AnnualizedReturns(x)

# Filter symbols SPXL (3x leveraged SPY) and SPXS (3x inverse SPY)
# Equites:      ["SPXL", "SPXS"] # from 2008/10
# Bonds 10y:    ["TMF", "TMV"]   # from 2009/06
# HY Bonds 10y: ["HYG", "SJB"]   # from 2011/04
symbols = c("spxl", "spxs", "spy")
all(prices[, symbols %in% unique(symbol)])
dt = prices[symbol %in% symbols]
dt[, min(date, na.rm = TRUE), by = symbol]

# Remove duplicates
dt = unique(dt, by = c("symbol", "date"))

# Adjust all columns
dt[, adj_rate := adj_close / close]
dt[, let(
  open = open*adj_rate,
  high = high*adj_rate,
  low = low*adj_rate
)]
setnames(dt, "close", "close_raw")
setnames(dt, "adj_close", "close")
dt[, let(adj_rate = NULL)]
setcolorder(dt, c("symbol", "date", "open", "high", "low", "close", "volume"))

# Remove missing values
dt = na.omit(dt)

# Sort
setorder(dt, symbol, date)

# Plot etf dt
plot(as.xts.data.table(dcast(dt, date ~ symbol, value.var = "close")))
plot(as.xts.data.table(dt[symbol == "tmv", .(date, close)]))
plot(as.xts.data.table(dt[symbol == "tmf", .(date, close)]))
plot(as.xts.data.table(dt[symbol == "tlt", .(date, close)]))

# Calculate daily returns
dt[, let(
  returns     = (close / shift(close, 1)) - 1,
  log_returns = c(NA, diff(log(close)))
), by = symbol]
dt = na.omit(dt)

# Short-both strategy: Assuming equal capital allocation to both, and rebalancing daily
strategyReturns = dcast(dt, date ~ symbol, value.var = c("log_returns", "returns"))
strategyReturns[, let(
  strategy     = 0.5 * -returns_tmv + 0.5 * -returns_tmf,
  strategy_log = 0.5 * -log_returns_tmv + 0.5 * -log_returns_tmf
)]
plot(strategyReturns[, .(date, strategy)])

# Performance analysis
strategy_xts = as.xts.data.table(strategyReturns[, .(date, strategy, strategy_log)])
chart.CumReturns(strategy_xts, main = "Cumulative Returns of Short-Both Strategy")
chart.CumReturns(strategy_xts[, 1], main = "Cumulative Returns of Short-Both Strategy")
chart.CumReturns(strategy_xts[, 2], main = "Cumulative Returns of Short-Both Strategy")
performance = table.AnnualizedReturns(strategy_xts)
print(performance)

# Simple backtest
backtest_data = strategyReturns[, .(date, strategy_log)][dt[symbol == "tlt"], on = "date"]
backtest_data = backtest_data[, .(date, returns, indicator = strategy_log)]
backtest_data = na.omit(backtest_data)
backtest_data[, indicator_sma := TTR::SMA(indicator, 10)]
plot(backtest_data[, .(date, indicator_sma)])
plot(backtest_data[, .(date, indicator)])
backtest_data[, indicator_lag := shift(indicator)]
backtest_data[, indicator_lag_diff := c(NA, diff(indicator_lag))]
# backtest_data[, signal := as.integer(shift(indicator) < 0.001 | shift(indicator) > 0.02)]
# backtest_data[, signal := as.integer(shift(indicator) < 0.001 | diff(shift(indicator)) < 0)]
backtest_data[, signal := as.integer(indicator_lag < 0.0005)]
# backtest_data[, signal := as.integer(indicator_lag_diff <= 0)]
#backtest_data[signal == 0, signal := 0]
backtest_data[, let(strategy = returns * signal)]
x = na.omit(as.xts.data.table(backtest_data[, .(date, strategy, returns)]))
charts.PerformanceSummary(x)

# Scaterplot
ggplot(backtest_data, aes(x = returns, y = shift(indicator_sma, 1))) +
  geom_point() +
  geom_smooth(method = "lm") +
  labs(title = "Scatterplot of Returns vs. SMA Indicator", x = "Returns", y = "SMA Indicator")

# Scaterplot without overlaps
ggplot(backtest_data[seq(1, nrow(backtest_data), 10)], aes(x = returns, y = shift(indicator_sma, 1))) +
  geom_point() +
  geom_smooth(method = "lm") +
  labs(title = "Scatterplot of Returns vs. SMA Indicator", x = "Returns", y = "SMA Indicator")

# Bars of returns across indicator
backtest_data[, indicator_deciles := cut(indicator_sma, breaks = quantile(indicator_sma, probs = seq(0, 1, 0.1), na.rm = TRUE))]
na.omit(backtest_data) |>
  ggplot(aes(x = shift(indicator_deciles), y = returns)) +
    geom_bar(stat = "identity") +
    labs(title = "Bars of Returns across SMA Indicator", x = "SMA Indicator", y = "Returns")

na.omit(backtest_data) |>
  _[, .(mr = median(returns, na.rm = TRUE)), by = indicator_deciles] |>
  ggplot(aes(x = shift(indicator_deciles), y = mr)) +
  geom_bar(stat = "identity") +
  labs(title = "Bars of Returns across SMA Indicator", x = "SMA Indicator", y = "Returns")




# FINNTS ------------------------------------------------------------------
# library(finnts)
#
# # prepare historical data
# hist_data <- timetk::m4_monthly %>%
#   dplyr::rename(Date = date) %>%
#   dplyr::mutate(id = as.character(id))
#
# # call main finnts modeling function
# finn_output <- forecast_time_series(
#   input_data = hist_data,
#   combo_variables = c("id"),
#   target_variable = "value",
#   date_type = "month",
#   forecast_horizon = 3,
#   back_test_scenarios = 6,
#   models_to_run = c("arima", "ets"),
#   run_global_models = FALSE,
#   run_model_parallel = FALSE
# )

