library(data.table)
library(vse4ts)
library(duckdb)
library(lubridate)
library(ggplot2)
library(PerformanceAnalytics)
library(TTR)


# SET UP ------------------------------------------------------------------
# Globals
QCDATA  = "F:/lean/data"
DATA    = "F:/data"
RESULTS = "F:/strategies/vse4ts"


# DATA --------------------------------------------------------------------
# Import hour data for specific symbols
con = dbConnect(duckdb::duckdb())
path_ = file.path(QCDATA, "stocks_hour.csv")
symbols = c("spy", "aapl")
symbols_string = paste(sprintf("'%s'", symbols), collapse=", ")
query = sprintf("
  SELECT *
  FROM '%s'
  WHERE Symbol IN (%s)
", path_, symbols_string)
ohlcvh = dbGetQuery(con, query)
dbDisconnect(con, shutdown = TRUE)

# Clean hour data
setDT(ohlcvh)
col = c("date", "open", "high", "low", "close", "volume", "close_adj", "symbol")
setnames(ohlcvh, col)
ohlcvh = unique(ohlcvh, by = c("symbol", "date"))
unadjustd_cols = c("open", "high", "low")
ohlcvh[, (unadjustd_cols) := lapply(.SD, function(x) (close_adj / close) * x),
       .SDcols = unadjustd_cols]
ohlcvh = na.omit(ohlcvh)
setorder(ohlcvh, symbol, date)
setnames(ohlcvh, c("close", "close_adj"), c("close_raw", "close"))
ohlcvh = ohlcvh[open > 0.00003 & high > 0.00003 & low > 0.00003 & close > 0.00003]
ohlcvh[, dollar_volume := close_raw * volume]
ohlcvh[, date := force_tz(date, "America/New_York")]

# Calculate returns
ohlcvh[, returns := close / shift(close) - 1, by = .(symbol)]

# Remove NA values
ohlcvh = na.omit(ohlcvh)


# ANALYSIS ----------------------------------------------------------------
# Calculate indicators
ohlcvh[, vse := frollapply(returns, 66 * 7, vse), by = symbol]
ohlcvh[, wnoise := frollapply(returns, 66 * 7, function(x) Wnoise.test(x)[["p.value"]]),
       by = symbol]
ohlcvh[, slm := frollapply(returns, 66 * 7, function(x) SLmemory.test(x)[["p.value"]]),
       by = symbol]

# Summary of indicators
sample_symbol = "spy"
plot(ohlcvh[symbol == sample_symbol, vse])
plot(ohlcvh[symbol == sample_symbol, wnoise])
plot(ohlcvh[symbol == sample_symbol, slm])
ohlcvh[, .(
  min_vse = min(vse, na.rm = TRUE),
  max_vse = max(vse, na.rm = TRUE),
  mean_vse = mean(vse, na.rm = TRUE),
  sd_vse = sd(vse, na.rm = TRUE)
)]
ohlcvh[, .(
  min_vse = min(wnoise, na.rm = TRUE),
  max_vse = max(wnoise, na.rm = TRUE),
  mean_vse = mean(wnoise, na.rm = TRUE),
  sd_vse = sd(wnoise, na.rm = TRUE)
)]
ohlcvh[, .(
  min_vse = min(slm, na.rm = TRUE),
  max_vse = max(slm, na.rm = TRUE),
  mean_vse = mean(slm, na.rm = TRUE),
  sd_vse = sd(slm, na.rm = TRUE)
)]
plot(ohlcvh[symbol == sample_symbol, SMA(vse, 7)])
plot(ohlcvh[symbol == sample_symbol, SMA(wnoise, 7)])
plot(ohlcvh[symbol == sample_symbol, SMA(slm, 7)])

# Save to Azure blob to backtest in Quantconnect for one symbol
symbol_ = "spy"
qc_data = ohlcvh[, .(date, vse)]
qc_data[, date := as.character(date)]
bl_endp_key = storage_endpoint(Sys.getenv("BLOB-ENDPOINT-SNP"), Sys.getenv("BLOB-KEY-SNP"))
cont = storage_container(bl_endp_key, "qc-backtest")
storage_write_csv(qc_data, cont, "vse_spy.csv", col_names = FALSE)

# Make lag vse
ohlcvh[, vse_lag_1 := shift(vse, 1, type = "lag"), by = .(symbol)]
ohlcvh[, vse_lag_2 := shift(vse, 2, type = "lag"), by = .(symbol)]
ohlcvh[, wnoise_lag_1 := shift(wnoise, 1, type = "lag"), by = .(symbol)]
ohlcvh[, wnoise_lag_2 := shift(wnoise, 2, type = "lag"), by = .(symbol)]
ohlcvh[, wnoise_lag_1 := shift(slm, 1, type = "lag"), by = .(symbol)]
ohlcvh[, wnoise_lag_2 := shift(slm, 2, type = "lag"), by = .(symbol)]

# Create target variable
ohlcvh[, target_hour := shift(close, 1, type = "lead") / close - 1, by = .(symbol)]
ohlcvh[, target_day := shift(close, 8, type = "lead") / close - 1, by = .(symbol)]
ohlcvh[, target_week := shift(close, 35, type = "lead") / close - 1, by = .(symbol)]
ohlcvh[, target_month := shift(close, 150, type = "lead") / close - 1, by = .(symbol)]

# Sample data for one symbol
symbol_ = "spy"
dts = ohlcvh[symbol == symbol_]

# Scaterplots for x = vse and y = target
ggplot(na.omit(dts), aes(x = vse, y = target_hour)) +
  geom_point() +
  geom_smooth(method = "lm", se = FALSE)
ggplot(na.omit(dts), aes(x = vse, y = target_day)) +
  geom_point() +
  geom_smooth(method = "lm", se = FALSE)
ggplot(na.omit(dts), aes(x = vse, y = target_week)) +
  geom_point() +
  geom_smooth(method = "lm", se = FALSE)
ggplot(na.omit(dts), aes(x = vse, y = target_month)) +
  geom_point() +
  geom_smooth(method = "lm", se = FALSE)
ggplot(na.omit(dts), aes(x = wnoise, y = target_hour)) +
  geom_point() +
  geom_smooth(method = "lm", se = FALSE)
ggplot(na.omit(dts), aes(x = wnoise, y = target_day)) +
  geom_point() +
  geom_smooth(method = "lm", se = FALSE)
ggplot(na.omit(dts), aes(x = wnoise, y = target_week)) +
  geom_point() +
  geom_smooth(method = "lm", se = FALSE)
ggplot(na.omit(dts), aes(x = wnoise, y = target_month)) +
  geom_point() +
  geom_smooth(method = "lm", se = FALSE)
ggplot(na.omit(dts), aes(x = slm, y = target_hour)) +
  geom_point() +
  geom_smooth(method = "lm", se = FALSE)
ggplot(na.omit(dts), aes(x = slm, y = target_day)) +
  geom_point() +
  geom_smooth(method = "lm", se = FALSE)
ggplot(na.omit(dts), aes(x = slm, y = target_week)) +
  geom_point() +
  geom_smooth(method = "lm", se = FALSE)
ggplot(na.omit(dts), aes(x = slm, y = target_month)) +
  geom_point() +
  geom_smooth(method = "lm", se = FALSE)

# Scaterplots for x = vse and y = target for bins of vse
bin_mean_returns = function(dt,
                            number_of_bins = 20,
                            target_var = "target_week",
                            vse_var = "vse",
                            plot = TRUE) {
  labels = paste0("vse_", 1:number_of_bins)
  dt[, vse_bin := cut(vse,
                      breaks = number_of_bins,
                      labels = labels,
                      include.lowest = TRUE)]
  dts_binds_results = dt[, .(mean_target = mean(get(target_var), na.rm = TRUE) * 100),
                        by = .(vse_bin)]
  dts_binds_results[, vse_bin_int := as.integer(gsub("vse_", "", vse_bin))]
  setorder(dts_binds_results, vse_bin_int)

  if (isTRUE(plot)) {
    p = ggplot(dts_binds_results, aes(x = vse_bin_int, y = mean_target)) +
      geom_point() +
      geom_smooth(method = "lm", se = FALSE)
    return(p)
  } else {
    return(dts_binds_results)
  }
}

# Sample plots
bin_mean_returns(dts, 40, "target_hour")
bin_mean_returns(dts, 40, "target_day")
bin_mean_returns(dts, 40, "target_week")
bin_mean_returns(dts, 40, "target_month")
bin_mean_returns(dts, 40, "target_hour", vse_var = "wnoise")
bin_mean_returns(dts, 40, "target_day", vse_var = "wnoise")
bin_mean_returns(dts, 40, "target_week", vse_var = "wnoise")
bin_mean_returns(dts, 40, "target_month", vse_var = "wnoise")
bin_mean_returns(dts, 40, "target_hour", vse_var = "slm")
bin_mean_returns(dts, 40, "target_day", vse_var = "slm")
bin_mean_returns(dts, 40, "target_week", vse_var = "slm")
bin_mean_returns(dts, 40, "target_month", vse_var = "slm")

# Simple dirty backtest
number_of_bins = 40
labels = paste0("vse_", 1:number_of_bins)
dts[, vse_bin := cut(slm,
                     breaks = number_of_bins,
                     labels = labels,
                     include.lowest = TRUE)]
dts[, vse_bin_char := as.character(vse_bin)]
dts[, unique(vse_bin_char)]
dts[, .N, by = vse_bin_char]
vse_bin_buy = paste0("vse_", 1:3)
dts[, signal := 0]
dts[vse_bin_char %in% vse_bin_buy, signal := 1]
dts[, strategy := signal * returns]
r = as.xts.data.table(na.omit(dts[, .(date, strategy, benchmark = returns)]))
Return.cumulative(r)
SharpeRatio(r)
charts.PerformanceSummary(r)
