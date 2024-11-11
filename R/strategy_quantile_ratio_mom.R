library(data.table)
library(RollingWindow)
library(DescTools)
library(TTR)
library(janitor)
library(PerformanceAnalytics)
library(erf)
library(foreach)
library(parallel)
library(doParallel)


# # SET UP ------------------------------------------------------------------
# # global vars
# PATH = "F:/data/equity/us"


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

# Remove observations where open, high, low, close columns are below 1e-008
# This step is opional, we need it if we will use finfeatures package
prices = prices[open > 1e-008 & high > 1e-008 & low > 1e-008 & close > 1e-008]

# Sort
setorder(prices, symbol, date)

# Calculate returns
prices[, returns := close / shift(close, 1) - 1]

# Remove missing values
prices = na.omit(prices)

# Set SPY returns as market returns
spy_ret = na.omit(prices[symbol == "spy", .(date, market_ret = returns)])
prices = spy_ret[prices, on = "date"]

# Minimal observations per symbol is 253 days
remove_symbols = prices[, .(symbol, n = .N), by = symbol][n < 253, symbol]
prices = prices[symbol %notin% remove_symbols]

# Free memory
gc()


# FILTERING ---------------------------------------------------------------
# Add label for most liquid asssets
prices[, dollar_volume := close_raw * volume]

# Create list of symbols arranged by dollar liquidity from 2020 to today
symbols_liquid = prices[date >= as.Date("2020-01-01"),
                        .(dollar_volume_sum = sum(dollar_volume, na.rm = TRUE)),
                        by = symbol][order(-dollar_volume_sum)]

# Remove columns we don't need
prices[, dollar_volume := NULL]


# PREDICTORS --------------------------------------------------------------
# Rolling beta
setorder(prices, symbol, date)
prices = prices[, beta := RollingBeta(market_ret, returns, 252, na_method = "ignore"),
                by = symbol]

# Highest beta by date - 5% of symbols by highest beta
prices[, beta_rank := frank(abs(beta), ties.method = "dense", na.last = "keep"), by = date]
prices[, beta_rank_pct := beta_rank / max(beta_rank, na.rm = TRUE), by = date]
prices[, beta_rank_largest_99 := 0, by = date]
prices[beta_rank_pct > 0.99, beta_rank_largest_99 := 1, by = date]
prices[, beta_rank_largest_95 := 0, by = date]
prices[beta_rank_pct > 0.95, beta_rank_largest_95 := 1, by = date]
prices[, beta_rank_largest_90 := 0, by = date]
prices[beta_rank_pct > 0.90, beta_rank_largest_90 := 1, by = date]
setorder(prices, symbol, date)

# Momentum predictors
months_size = c(3, 6, 9, 12)
mom_vars = paste0("momentum_", months_size)
f_ = function(x, n) {
  shift(x, 21) / shift(x, n * 21) - 1
}
prices[, (mom_vars) := lapply(months_size, function(x) f_(close, x)), by = symbol]

# Momentum ensambles
weights_ = c(12, 6, 3, 1) / sum(c(12, 6, 3, 1))
prices[, momentum_average := momentum_3 * weights_[1] +
         momentum_6 * weights_[2] +
         momentum_9 * weights_[3] +
         momentum_12 * weights_[4]]

# Dolar volume z-score
dv_cols = paste0("dollar_volume_zscore_winsorized", months_size)
f_ = function(x, y, z) RollingZscore(as.numeric(x * y), z, na_method = "ignore")
prices[, (dv_cols) := lapply(months_size, function(s)  as.vector(f_(close_raw, volume, s * 21))),
       by = symbol]
prices[, (dv_cols) := lapply(.SD, function(x) Winsorize(x, val = c(-5, 5))),
       .SDcols = dv_cols]

# Tecnical indicators
prices[, rsi := RSI(close, n = 14), by = symbol]

# Remove columns we don't need
prices[, c("beta_rank", "beta_rank_pct", "open", "high", "low", "volume") := NULL]


# ESTIMATION --------------------------------------------------------------
# Keep data we need
dt = na.omit(prices[symbol %in% symbols_liquid[, head(symbol, 200)]])

# Create target variable
setorder(dt, symbol, date)
dt[, target := shift(close, 1, type = "lead") / close - 1, by = symbol]
dt = na.omit(dt)

# Shift predictors
nonx_cols = c("symbol", "date", "close", "close_raw", "returns", "target")
predictors = dt[, colnames(.SD), .SDcols = -nonx_cols]

# Select some date interval
dates = seq.Date(from = as.Date("2019-01-01"), to = Sys.Date(), by = "day")
dates = as.Date(intersect(dates, dt[, unique(date)]))

# Loop over date and symbols and train erf model extract predictions
quantile_levels = c(0.01, 0.05, 0.95, 0.99)
setorder(dt, symbol ,date)
for (s in dt[, unique(symbol)]) {
  # debug
  print(s)

  # Check if already estimated
  file_name = paste0("F:/strategies/evtq/", s, ".csv")
  if (file.exists(file_name)) next

  # sample data
  dt_ = dt[symbol == s]

  # estimation
  s_ = Sys.time()
  cl = makeCluster(4L)
  registerDoParallel(cl)
  # l = foreach(i = 1:100) %do% {
  l = foreach(i = 1:length(dates),
              .packages = c("data.table", "erf", "janitor"),
              .export = c("dt_", "s")) %dopar% {
    d = dates[i]
    # for (d in dates[1:20]) {
    # d = dates[1]

    # Train data
    dtd = dt_[date < d]
    if (nrow(dtd) < 252) return(NULL)
    if (as.Date(d) - dt_[, as.Date(max(date))] > 2) return(NULL)

    # Test data
    test_data = dt_[date == d]
    if (nrow(test_data) == 0) return(NULL)

    # Fit model for upper
    train_data_upper = dtd[returns > 0]
    erf_model_upper = erf(
      X = as.matrix(train_data_upper[, .SD, .SDcols = predictors]),
      Y = train_data_upper[, target],
      min.node.size = 5,
      lambda = 0.001,
      intermediate_quantile = 0.8
    )

    # Fit model for lower
    train_data_lower = dt_[returns < 0]
    erf_model_lower = erf(
      X = as.matrix(train_data_lower[, .SD, .SDcols = predictors]),
      Y = -train_data_lower[, target],
      min.node.size = 5,
      lambda = 0.001,
      intermediate_quantile = 0.8
    )

    # Predict
    erf_predictions_upper = predict(
      erf_model_upper,
      as.matrix(test_data[, .SD, .SDcols = predictors]),
      quantiles = quantile_levels
    )
    erf_predictions_lower = predict(
      erf_model_lower,
      as.matrix(test_data[, .SD, .SDcols = predictors]),
      quantiles = quantile_levels
    )
    erf_predictions_upper = clean_names(as.data.frame(erf_predictions_upper))
    colnames(erf_predictions_upper) = paste0("upper_", colnames(erf_predictions_upper))
    erf_predictions_lower = clean_names(as.data.frame(erf_predictions_lower))
    colnames(erf_predictions_lower) = paste0("lower_", colnames(erf_predictions_lower))
    cbind(symbol = s, date = d, erf_predictions_upper, erf_predictions_lower,
          targetr = test_data[, target])
  }
  stopCluster(cl)

  # Check time
  e = Sys.time()
  print(e-s_)

  # Clean and save
  x = rbindlist(l)
  fwrite(x, file_name)
}
