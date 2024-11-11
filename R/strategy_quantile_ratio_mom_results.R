library(arrow)
library(dplyr)
library(data.table)
library(ggplot2)
library(TTR)
library(AzureStor)
library(janitor)
library(PerformanceAnalytics)


# DATA --------------------------------------------------------------------
# Set up
PATH = "F:/strategies/evtq"
blobendpoint = storage_endpoint(Sys.getenv("BLOB-ENDPOINT-SNP"),
                                key=Sys.getenv("BLOB-KEY-SNP"))
cont = storage_container(blobendpoint, "qc-backtest")

# Import results
files = list.files(PATH, full.names = TRUE)
erf_predictions = lapply(files, fread)
erf_predictions = erf_predictions[lengths(erf_predictions) > 0]

# Combine all results
erf_predictions = rbindlist(erf_predictions)

# # Import prices
# prices = fread("F:/lean/data/stocks_daily.csv")
# setnames(prices, gsub(" ", "_", c(tolower(colnames(prices)))))
#
# # Remove duplicates
# prices = unique(prices, by = c("symbol", "date"))
#
# # Adjust all columns
# prices[, adj_rate := adj_close / close]
# prices[, let(
#   open = open*adj_rate,
#   high = high*adj_rate,
#   low = low*adj_rate
# )]
# setnames(prices, "close", "close_raw")
# setnames(prices, "adj_close", "close")
# prices[, let(adj_rate = NULL)]
# setcolorder(prices, c("symbol", "date", "open", "high", "low", "close", "volume"))
#
# # Remove observations where open, high, low, close columns are below 1e-008
# # This step is opional, we need it if we will use finfeatures package
# prices = prices[open > 1e-008 & high > 1e-008 & low > 1e-008 & close > 1e-008]
#
# # Sort
# setorder(prices, symbol, date)
#
# # Calculate returns
# prices[, returns := close / shift(close, 1) - 1]
#
# # Remove missing values
# prices = na.omit(prices)
#
# # Set SPY returns as market returns
# spy_ret = na.omit(prices[symbol == "spy", .(date, market_ret = returns)])
# prices = spy_ret[prices, on = "date"]
#
# # Minimal observations per symbol is 253 days
# remove_symbols = prices[, .(symbol, n = .N), by = symbol][n < 253, symbol]
# prices = prices[symbol %notin% remove_symbols]
#
# # Free memory
# gc()


# PREPARE -----------------------------------------------------------------
# Define quantile ratios
erf_predictions[, qr_99 := upper_quantile_0_99 / lower_quantile_0_99]
erf_predictions[, qr_95 := upper_quantile_0_95 / lower_quantile_0_95]

# # Merge with prices
# dt = prices[, .(date, symbol, returns)][erf_predictions, on = c("symbol", "date")]

# Generate signals
generate_signal = function(dt, threshold, n = 0, q = c("99", "95", "both")) {
  dt_ = copy(dt)
  dt_[, signal := 0]
  if (q == "99") {
    dt_[qr_99 > threshold, signal := 1]
  } else if (q == "95") {
    dt_[qr_95 > threshold, signal := 1]
  } else {
    dt_[qr_99 > threshold & qr_95 > threshold, signal := 1]
  }
  dt_[, signal := shift(signal, n), by = symbol]
  dt_[, strategy_return := targetr * signal]
  return(na.omit(dt_))
}


# INDIVIDUAL ASSETS -------------------------------------------------------
# Sample
symbol_ = erf_predictions[, sample(unique(symbol), 1)]
# symbol_ = "spy"
dt_ = erf_predictions[symbol == symbol_]

# Plot quantiles and returns
cols = c("date", "upper_quantile_0_99", "lower_quantile_0_99",
         "upper_quantile_0_95", "lower_quantile_0_95", "targetr")
ggplot(dt_[, ..cols], aes(x = date)) +
  geom_line(aes(y = upper_quantile_0_99, color = "upper_quantile_0_99")) +
  geom_line(aes(y = -lower_quantile_0_99, color = "lower_quantile_0_99")) +
  geom_line(aes(y = upper_quantile_0_95, color = "upper_quantile_0_95")) +
  geom_line(aes(y = -lower_quantile_0_95, color = "lower_quantile_0_95")) +
  geom_line(aes(y = targetr, color = "targetr")) +
  scale_color_manual(values = c("upper_quantile_0_99" = "red",
                                "lower_quantile_0_99" = "red",
                                "upper_quantile_0_95" = "blue",
                                "lower_quantile_0_95" = "blue",
                                "targetr" = "black")) +
  scale_x_date(date_breaks = "1 year", date_labels = "%Y") +
  theme_minimal()
n_ = 22
ggplot(dt_[, ..cols], aes(x = date)) +
  geom_line(aes(y = EMA(upper_quantile_0_99, n_), color = "upper_quantile_0_99")) +
  geom_line(aes(y = EMA(-lower_quantile_0_99, n_), color = "lower_quantile_0_99")) +
  geom_line(aes(y = EMA(upper_quantile_0_95, n_), color = "upper_quantile_0_95")) +
  geom_line(aes(y = EMA(-lower_quantile_0_95, n_), color = "lower_quantile_0_95")) +
  geom_line(aes(y = EMA(targetr, n_), color = "targetr")) +
  scale_color_manual(values = c("upper_quantile_0_99" = "red",
                                "lower_quantile_0_99" = "red",
                                "upper_quantile_0_95" = "blue",
                                "lower_quantile_0_95" = "blue",
                                "targetr" = "black")) +
  scale_x_date(date_breaks = "1 year", date_labels = "%Y") +
  theme_minimal()

# Scatterplot between signal and returns
dt_ = generate_signal(dt_, 0.8, 1)
ggplot(dt_, aes(x = qr_99, y = targetr)) +
  geom_point() +
  geom_smooth(method = "lm") +
  theme_minimal()
ggplot(dt_, aes(x = qr_95, y = targetr)) +
  geom_point() +
  geom_smooth(method = "lm") +
  theme_minimal()
ggplot(dt_[qr_99 > 1], aes(x = qr_99, y = targetr)) +
  geom_point() +
  geom_smooth(method = "lm") +
  theme_minimal()
ggplot(dt_[qr_95 > 0.9], aes(x = qr_95, y = targetr)) +
  geom_point() +
  geom_smooth(method = "lm") +
  theme_minimal()

# Plot cumulative returns
dt_ = generate_signal(dt_, 1, 1, "95")
back_xts = as.xts.data.table(dt_[, .(date, strategy_return, targetr)])
table.AnnualizedReturns(back_xts)
maxDrawdown(back_xts)
charts.PerformanceSummary(
  back_xts[],
  legend.loc = "topleft",
  # main = paste(symbol, "Strategy Performance vs. Benchmark"),
  colorset = c("blue", "red"),
  wealth.index = TRUE
)

# Add to QC to test the strategy\
qc_data = dt_[, .(date, qr_99, qr_95)]
qc_data[, date := paste0(as.character(date), " 16:00:00")]
storage_write_csv(qc_data, cont, paste0("erf_", dt_[1, symbol], ".csv"))


# OPTIMIZATION ALL --------------------------------------------------------
# Parameters
params = expand.grid(s = erf_predictions[, unique(symbol)],
                     q = c("99", "95", "both"),
                     t = seq(0.5, 1.5, 0.1),
                     stringsAsFactors = FALSE)

# Calculate portfolio performance for all parameters
results = list()
for (i in 1:nrow(params)) {
  dt_ = erf_predictions[symbol == params$s[i]]
  dt_ = generate_signal(copy(dt_), params$t[i], 0, params$q[i])
  back_xts = as.xts.data.table(dt_[, .(date, strategy_return, targetr)])
  x = table.AnnualizedReturns(back_xts)
  x = as.data.table(x, keep.rownames = "var")
  x = data.table::transpose(x[, 1:2], make.names = "var")
  x = clean_names(x)
  results[[i]] = cbind(s = params$s[i], q = params$q[i], t = params$t[i], x)
}
results_df = rbindlist(results)
setnames(results_df, c("symbol", "q", "threshold", "cagr", "std", "SR"))

# list best
setorder(results_df, -SR)
head(na.omit(results_df), 10)

# list best across symbols
setorder(results_df, symbol, -SR)
na.omit(results_df)[, head(.SD), by = symbol]


# PORTFOLIO ---------------------------------------------------------------
# Portfolio returns
portfolio = erf_predictions[, .(symbol, date, qr_95, targetr)]
portfolio[, signal := 0]
portfolio[qr_95 > 1, signal := 1]
# portfolio[, signal := shift(signal, 1), by = symbol]
portfolio[, weights := signal / nrow(.SD[signal == 1]), by = date]
setorder(portfolio, date)
portfolio_ret = portfolio[, .(returns = sum(targetr * weights, na.rm = TRUE)), by = date]
portfolio_ret = as.xts.data.table(portfolio_ret)
charts.PerformanceSummary(portfolio_ret)
table.AnnualizedReturns(portfolio_ret)
maxDrawdown(portfolio_ret)

# Add all to QC
qc_data = erf_predictions[, .(symbol, date, qr_99, qr_95)]
qc_data[, date := paste0(as.character(date), " 16:00:00")]
qc_data = qc_data[, .(
  symbol = paste0(symbol, collapse = "|"),
  qr_99 = paste0(qr_99, collapse = "|"),
  qr_95 = paste0(qr_95, collapse = "|")),
  by = date]
storage_write_csv(qc_data, cont, paste0("erf.csv"))


# SYSTEMIC RISK -----------------------------------------------------------
# SPY
spy = open_dataset("F:/lean/data/stocks_daily.csv", format = "csv") |>
  filter(Symbol == "spy") |>
  select(Date, `Adj Close`) |>
  dplyr::rename(date = Date, close = `Adj Close`) |>
  collect()
setDT(spy)
spy[, returns := close / shift(close) - 1]

# Simple aggregation
sr = copy(portfolio)
setorder(sr, symbol, date)
sr[, stand_q95 := roll::roll_scale(qr_95, nrow(.SD), min_obs = 44), by = symbol]
sr = na.omit(sr)
sr = sr[, .(mean = mean(stand_q95),
            median = median(stand_q95),
            sd = sd(stand_q95)),
        by = date]
plot(as.xts.data.table(sr),
     main = "Systemic Risk",
     ylab = "Standardized Quantile 95",
     col = c("blue", "red", "green"),
     lwd = 2,
     legend.loc = "topleft")

# Backtest
back = spy[, .(date, returns)][sr, on = "date"]
setorder(back, date)
predictors = colnames(back)[3:ncol(back)]
back[, (predictors) := lapply(.SD, shift, n = 1), .SDcols = predictors]
back[, strategy := ifelse(mean > 0, returns, 0)]
back_xts = as.xts.data.table(na.omit(back)[, .(date, strategy, returns)])
charts.PerformanceSummary(back_xts)
table.AnnualizedReturns(back_xts)
maxDrawdown(back_xts)

# Add to Quantconnect
qc_data = spy[, .(date, returns)][sr, on = "date"]
setorder(qc_data, date)
qc_data = qc_data[, .(date, mean)]
qc_data[, date := paste0(as.character(date), " 16:00:00")]
storage_write_csv(qc_data, cont, paste0("erf_risk.csv"))
