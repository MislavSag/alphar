library(data.table)
library(ggplot2)
library(xts)
library(PerformanceAnalytics)
library(TTR)
library(tidyr)
library(leanr)
library(patchwork)
library(future.apply)
library(kerasgenerator)
library(equityData)
library(rdrop2)
drop_auth()


# exuber paths
exuber_path <- "D:/risks/radf-hour"
exuber_paths <- list.files(exuber_path, full.names = TRUE)
exuber_paths <- "D:/risks/radf-hour/1-600-1"
symbols_saved <- gsub('.csv', '', list.files(exuber_paths))

# price data
sp500_stocks <- import_lean("D:/market_data/equity/usa/hour/trades_adjusted")
sp500_stocks[, returns := (close / shift(close)) - 1, by = .(symbol)]
spy <- sp500_stocks[symbol == "SPY", .(datetime, close, returns)]
sp500_stocks <- sp500_stocks[symbol %in% symbols_saved]
sp500_stocks[, cum_returns := frollsum(returns, 10 * 8), by = .(symbol)]
sp500_stocks[, cum_returns := shift(cum_returns, type = "lag"), by = .(symbol)]
sp500_stocks <- na.omit(sp500_stocks)
sp500_stocks <- sp500_stocks[, .(symbol, datetime, returns, cum_returns)]

# function for calculating exuber aggregate values based on file which contains radf values
exuber_agg <- function(path) {

  # import exuber data
  exuber_file <- list.files(path, full.names = TRUE)
  if (length(exuber_file) < 500) {
    print("number of files in the folder is lower than 500.")
    return(NULL)
  }
  exuber_dt <- lapply(exuber_file, function(x) {tryCatch(fread(x), error = function(e) NULL)})
  exuber_dt <- rbindlist(exuber_dt)[, id := gsub(".*/", "", path)]
  exuber_dt[, radf_sum := adf + sadf + gsadf + badf + bsadf]
  exuber_dt <- exuber_dt[, .(id, symbol, datetime, adf, sadf, gsadf, badf, bsadf, radf_sum)]
  # attributes(exuber_dt$datetime)$tzone <- "EST" ############################### VIDJETI STO S OVIM ######################

  # merge exuber and stocks
  exuber_dt <- merge(exuber_dt, sp500_stocks, by = c("symbol", "datetime"), all.x = TRUE, a.y = FALSE)
  setorderv(exuber_dt, c("id", "symbol", "datetime"))

  # define indicators based on exuber
  radf_vars <- colnames(exuber_dt)[4:ncol(exuber_dt)]
  indicators_median <- exuber_dt[, lapply(.SD, median, na.rm = TRUE), by = c('id', 'datetime'), .SDcols = radf_vars]
  colnames(indicators_median)[3:ncol(indicators_median)] <- paste0("median_", colnames(indicators_median)[3:ncol(indicators_median)])
  indicators_sd <- exuber_dt[, lapply(.SD, sd, na.rm = TRUE), by = c('id', 'datetime'), .SDcols = radf_vars]
  colnames(indicators_sd)[3:ncol(indicators_sd)] <- paste0("sd_", colnames(indicators_sd)[3:ncol(indicators_sd)])
  indicators_mean <- exuber_dt[, lapply(.SD, mean, na.rm = TRUE), by = c('id', 'datetime'), .SDcols = radf_vars]
  colnames(indicators_mean)[3:ncol(indicators_mean)] <- paste0("mean_", colnames(indicators_mean)[3:ncol(indicators_mean)])
  indicators_sum <- exuber_dt[, lapply(.SD, sum, na.rm = TRUE), by = c('id', 'datetime'), .SDcols = radf_vars]
  colnames(indicators_sum)[3:ncol(indicators_sum)] <- paste0("sum_", colnames(indicators_sum)[3:ncol(indicators_sum)])

  # merge indicators
  indicators <- merge(indicators_sd, indicators_median, by = c("id", "datetime"))
  indicators <- merge(indicators, indicators_mean, by = c("id", "datetime"))
  indicators <- merge(indicators, indicators_sum, by = c("id", "datetime"))
  setorderv(indicators, c("id", "datetime"))
  indicators <- na.omit(indicators)

  return(indicators)
}

# get exuber agg for all files
exuber_list <- lapply(exuber_paths, exuber_agg)
exuber_list <- exuber_list[lengths(exuber_list) != 0]
exuber <- rbindlist(exuber_list)
exuber_indicators <- exuber[spy, on = "datetime"]
exuber_indicators <- na.omit(exuber_indicators)
setorderv(exuber_indicators, c("id", "datetime"))
exuber_indicators <- exuber_indicators[id %like% "-600-"] # filter,, comment if want to uise all ids

# save to local path and dropbox
data_save <- lapply(exuber_list, function(x) {
  attr(x$datetime, "tzone") <- "EST"
  x
})
lapply(data_save, function(x) fwrite(x[, 2:8], paste0("D:/risks/radf-indicators/", x$id[1], ".csv"), col.names = FALSE, dateTimeAs = "write.csv"))
lapply(list.files("D:/risks/radf-indicators", full.names = TRUE), function(x) drop_upload(file = x, path = "exuber"))
head(exuber_list[[1]])


# plots
variable_name <- "sd_radf_sum"
variables_ <- c("id", "datetime", variable_name)
data_plot <- melt(exuber_indicators[, ..variables_], id.vars = c("id", "datetime"), measure.vars = variable_name)
sma_width <- 4
g1 <- ggplot(data_plot, aes(x = datetime, y = value, color = id)) +
  geom_line()
g2 <- ggplot(data_plot[datetime %between% c("2015-01-01", "2016-01-01")], aes(x = datetime, y = SMA(value, sma_width), color = id)) +
  geom_line()
g3 <- ggplot(data_plot[datetime %between% c("2008-01-01", "2010-03-01")], aes(x = datetime, y = SMA(value, sma_width), color = id)) +
  geom_line()
g4 <- ggplot(data_plot[datetime %between% c("2020-01-01", "2021-01-01")], aes(x = datetime, y = SMA(value, sma_width), color = id)) +
  geom_line()
g5 <- ggplot(data_plot[datetime %between% c("2021-01-01", "2021-05-10")], aes(x = datetime, y = SMA(value, sma_width), color = id)) +
  geom_line()
g2 / g3 / g4 / g5
data_plot <- exuber_indicators[, .(id, datetime, sd_radf_sum)]
# data_plot <- data_plot[grep("100-2|200-2|400-2|600-2", id)] # uncomment this if use all variables
ggplot(data_plot, aes(datetime, sd_radf_sum)) +
  geom_line() +
  ggplot2::facet_grid(id ~ .)
ggplot(data_plot[datetime %between% c("2020-01-01", "2021-01-01")], aes(datetime, sd_radf_sum)) +
  geom_line() +
  ggplot2::facet_grid(id ~ .)

# backtst function
backtest <- function(returns, indicator, threshold, return_cumulative = TRUE) {
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1])) {
      sides[i] <- NA
    } else if (indicator[i-1] > threshold) {
      sides[i] <- 0
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

# backtest percentiles
backtest_percentiles <- function(returns, indicator,
                                 indicator_percentil, return_cumulative = TRUE) {
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1]) || is.na(indicator_percentil[i-1])) {
      sides[i] <- NA
    } else if (indicator[i-1] > indicator_percentil[i-1]) {
      sides[i] <- 0
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

# backtest performance
Performance <- function(x) {
  cumRetx = Return.cumulative(x)
  annRetx = Return.annualized(x, scale=252)
  sharpex = SharpeRatio.annualized(x, scale=252)
  winpctx = length(x[x > 0])/length(x[x != 0])
  annSDx = sd.annualized(x, scale=252)

  DDs <- findDrawdowns(x)
  maxDDx = min(DDs$return)
  maxLx = max(DDs$length)

  Perf = c(cumRetx, annRetx, sharpex, winpctx, annSDx, maxDDx, maxLx)
  names(Perf) = c("Cumulative Return", "Annual Return","Annualized Sharpe Ratio",
                  "Win %", "Annualized Volatility", "Maximum Drawdown", "Max Length Drawdown")
  return(Perf)
}


# LEVEL TRADING RULE ------------------------------------------------------

# optimizations loop
thresholds <- c(seq(0.5, 4, 0.05))
variables <- colnames(exuber_indicators)[3:(ncol(exuber_indicators))]
params <- expand.grid(thresholds, variables, stringsAsFactors = FALSE)
returns_strategies <- list()
unique_ids <- unique(exuber_indicators$id)
for (j in seq_along(unique_ids)) {
  print(unique_ids[j])
  sample_ <- exuber_indicators[id == unique_ids[j]]
  sample_ <- sample_[order(datetime)]
  sample_ <- unique(sample_)
  x <- vapply(1:nrow(params), function(i) backtest(sample_$returns,
                                                   SMA(sample_[, get(params[i, 2])], 2),
                                                   params[i, 1]),
              numeric(1))
  returns_strategies[[j]] <- cbind(radf_id = unique_ids[j], params, x)
}
optimization_results_levels <- rbindlist(returns_strategies)
head(optimization_results_levels[order(optimization_results_levels$x, decreasing = TRUE), ], 20)

# optimization summary
opt_summary <- optimization_results_levels[, median(x), by = .(Var2)]
opt_summary[order(V1, decreasing = TRUE)]
ggplot(optimization_results_levels, aes(Var1, Var2, fill= x)) +
  geom_tile()
tail(optimization_results_levels[Var2 == "sd_bsadf"][order(x)], 20)
tail(optimization_results_levels[Var2 == "sd_radf_sum"][order(x)], 20)

# backtest individual
backtest_data <- exuber_indicators[id == "1-600-1"]
strategy_returns <- backtest(backtest_data$sd_bsadf, backtest_data$sd_bsadf, 0.7, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(backtest_data$returns, strategy_returns), order.by = backtest_data$datetime))



# LEVEL TRADING RULE WITH SD ---------------------------------------------------------

# optimization params
thresholds <- c(seq(2.5, 4, 0.05))
variables <- colnames(exuber_indicators)[3:8]
sma_window <- c(1, 2, 4, 8, 16, 32)
params <- expand.grid(thresholds, variables, sma_window, stringsAsFactors = FALSE)
colnames(params) <- c("thresholds", "variables", "sma_window")

# optimizations loop
plan(multicore(workers = 8L))
returns_strategies <- list()
unique_ids <- unique(exuber_indicators$id)
for (j in seq_along(unique_ids)) {
  print(unique_ids[j])
  sample_ <- exuber_indicators[id == unique_ids[j]]
  sample_ <- sample_[order(datetime)]
  sample_ <- unique(sample_)
  x <- future_vapply(1:nrow(params), function(i) backtest(sample_$returns,
                                                          SMA(sample_[, get(params[i, 2])], params[i, 3]),
                                                          params[i, 1]),
                     numeric(1))

  returns_strategies[[j]] <- cbind(radf_id = unique_ids[j], params, x)
}
optimization_results_level_sd <- rbindlist(returns_strategies)
optimization_results_level_sd_order <- optimization_results_level_sd[order(x, decreasing = TRUE)]
head(optimization_results_level_sd[order(optimization_results_level_sd$x, decreasing = TRUE), ], 70)
nrow(optimization_results_level_sd)
optimization_results_level_sd[thresholds == 3.40][order(x)]
optimization_results_level_sd[radf_id == "1-600-1"][order(x)]

# optimization summary
ggplot(optimization_results_level_sd, aes(x, fill = radf_id)) +
  geom_histogram(position = "identity", alpha = 0.6) +
  geom_vline(xintercept = 4.2)
ggplot(optimization_results_level_sd, aes(thresholds, sma_window, fill= x)) +
  geom_tile()
ggplot(optimization_results_level_sd[radf_id == "1-100-1"], aes(thresholds, sma_window, fill= x)) +
  geom_tile()
ggplot(optimization_results_level_sd[radf_id == "1-100-5"], aes(thresholds, sma_window, fill= x)) +
  geom_tile()

# backtest individual
threshold <- 3.4
backtest_data <- exuber_indicators[id == "1-100-1"]
strategy_returns <- backtest(backtest_data$returns, backtest_data$sd_radf_sum, threshold, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(backtest_data$returns, strategy_returns), order.by = backtest_data$datetime))
Performance(xts(strategy_returns, order.by = backtest_data$datetime))
Performance(xts(backtest_data$returns, order.by = backtest_data$datetime))

backtest_data <- spy[.id == "1-100-5" & datetime %between% c("2008-01-01", "2010-01-01")]
strategy_returns <- backtest(backtest_data$returns, backtest_data$sd_radf_sum, threshold, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(backtest_data$returns, strategy_returns), order.by = backtest_data$datetime))

backtest_data <- spy[.id == "1-100-5" & datetime %between% c("2020-01-01", "2021-01-01")]
strategy_returns <- backtest(backtest_data$returns, backtest_data$sd_radf_sum, threshold, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(backtest_data$returns, strategy_returns), order.by = backtest_data$datetime))

backtest_data <- spy[.id == "1-100-5" & datetime %between% c("2015-01-01", "2017-01-01")]
strategy_returns <- backtest(backtest_data$returns, backtest_data$sd_radf_sum, threshold, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(backtest_data$returns, strategy_returns), order.by = backtest_data$datetime))

backtest_data <- spy[.id == "1-100-5" & datetime %between% c("2016-01-01", "2017-01-01")]
strategy_returns <- backtest(backtest_data$returns, backtest_data$sd_radf_sum, threshold, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(backtest_data$returns, strategy_returns), order.by = backtest_data$datetime))

backtest_data <- spy[.id == "1-100-5" & datetime %between% c("2021-01-01", "2021-07-01")]
strategy_returns <- backtest(backtest_data$returns, backtest_data$sd_radf_sum, threshold, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(backtest_data$returns, strategy_returns), order.by = backtest_data$datetime))



# PERCENTILES TRADING RULE ------------------------------------------------

# calculate percentiles
backtest_data <- copy(exuber_indicators)

# optimization loop
p <- seq(0.8, 0.99, 0.01)
roll_width <- c(8 * 22 * seq(1, 12, 1), 8 * 22 * 12 * seq(2, 6, 1))
params <- expand.grid(p, roll_width, stringsAsFactors = FALSE)
plan(multicore(workers = 8))
returns_strategies <- list()
unique_ids <- unique(backtest_data$id)
for (j in seq_along(unique_ids)) {
  print(unique_ids[j])
  sample_ <- backtest_data[id == unique_ids[j]]
  sample_ <- sample_[order(datetime)]
  sample_ <- unique(sample_)

  x <- future_lapply(1:nrow(params), function(i) {

    # calculate percentiles
    cols <- colnames(sample_)[3:(ncol(sample_))]
    cols_new <- paste0('p_', cols)
    sample_[, (cols_new) := lapply(.SD, function(x) roll::roll_quantile(x, params[i, 2], p = params[i, 1])),
            .SDcols = cols]
    backtests_z <- list()
    for (z in 1:length(cols)) {
      col_ <- cols[z]
      col_new_ <- cols_new[z]
      backtets <- backtest_percentiles(sample_$returns,
                                       unlist(sample_[, ..col_]),
                                       unlist(sample_[, ..col_new_]))
      backtests_z[[z]] <- cbind.data.frame(col_, backtets)
    }
    backtests_p <- rbindlist(backtests_z)
    backtests_p <- cbind(backtests_p, params[i, ])
    # backtests_p <- data.table::transpose(backtests_p, make.names = TRUE)
    # backtests_p[, col_ := paste(col_, paste0(params[i, ] * 100, collapse = "_"), sep = "_")]
    # setnames(backtests_p, colnames(backtests_p), paste(colnames(backtests_p), paste0(params[i, ] * 10, collapse = "_"), sep = "_"))

    # remove percentiles from sample_
    remove_cols <- which(colnames(sample_) %in% cols_new)
    sample_[, (remove_cols) := NULL]

    # return
    backtests_p
  })
  returns_strategies[[j]] <- cbind(id = unique_ids[j], rbindlist(x))
}
optimization_results <- rbindlist(returns_strategies)
fwrite(optimization_results, paste0("D:/risks/radfagg_optimization/optimization_results-", Sys.Date(), ".csv"))

# optimization summary
vars_ <- unique(optimization_results$col_)
vars_ <- vars_[!grepl("p_|returns", vars_)]
optimization_results_summary <- optimization_results[col_ %in% vars_]
head(optimization_results_summary[order(optimization_results_summary$backtets, decreasing = TRUE), ], 30)
opt_summary <- optimization_results_summary[, .(median_score = median(backtets),
                                                mean_score = mean(backtets)), by = .(id)]
opt_summary[order(median_score, decreasing = TRUE)]

# results by variable
ggplot(optimization_results_summary, aes(x = backtets)) + geom_histogram() +
  geom_vline(xintercept = 4.1, color = "red") + facet_grid(. ~ col_)
ggplot(optimization_results[col_ == "sd_radf_sum"], aes(Var1, Var2, fill= backtets)) +
  geom_tile()
ggplot(optimization_results[col_ == "sd_radf_sum" & Var2 < 1500], aes(Var1, Var2, fill= backtets)) +
  geom_tile()
ggplot(optimization_results[col_ == "sd_radf_sum" & Var2 < 1500], aes(Var1, id, fill= backtets)) +
  geom_tile()
ggplot(optimization_results_summary[grep("1-200-2", id)], aes(x = backtets)) + geom_histogram() +
  geom_vline(xintercept = 4.1, color = "red") + facet_grid(. ~ id) +
  theme( axis.text = element_text( size = 6 ),
         axis.text.x = element_text( size = 6 ),
         axis.title = element_text( size = 6, face = "bold" ),
         legend.position="none",
         strip.text = element_text(size = 6))

# heatmaps
ggplot(optimization_results[id == "1-200-2"], aes(Var1, Var2, fill= backtets)) +
  geom_tile()
# heatmaps for best variable
optimization_results_best <- optimization_results[col_ == "sd_radf_sum"]
ggplot(optimization_results_best, aes(Var1, Var2, fill= backtets)) +
  geom_tile()
ggplot(optimization_results_best[grep("200", id)], aes(Var1, id, fill= backtets)) +
  geom_tile()

# backtest individual percentiles
sample_ <- backtest_data[id == "1-400-10"] #  & datetime %between% c('2020-01-01', '2021-01-01')]
cols <- colnames(sample_)[4:(ncol(sample_))]
cols_new <- paste0('p_', cols)
sample_[, (cols_new) := lapply(.SD, function(x) roll::roll_quantile(x, 1000, p = 0.90)), .SDcols = cols, by = id]
strategy_returns <- backtest_percentiles(sample_$returns, sample_$sd_radf_sum, sample_$p_sd_radf_sum, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(sample_$returns, strategy_returns), order.by = sample_$datetime))
Performance(xts(strategy_returns, order.by = sample_$datetime))
Performance(xts(sample_$returns, order.by = sample_$datetime))

ind <- 1:15000
charts.PerformanceSummary(xts(cbind(sample_$returns[ind], strategy_returns[ind]), order.by = sample_$datetime[ind]))
ind <- 15000:20000
charts.PerformanceSummary(xts(cbind(sample_$returns[ind], strategy_returns[ind]), order.by = sample_$datetime[ind]))
ind <- 20000:25000
charts.PerformanceSummary(xts(cbind(sample_$returns[ind], strategy_returns[ind]), order.by = sample_$datetime[ind]))




# CROSS MA ----------------------------------------------------------------


# backtst function
backtest_cross <- function(returns, indicator_short, indicator_long, return_cumulative = TRUE) {
  sides <- vector("integer", length(indicator_short))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator_long[i-1])) {
      sides[i] <- NA
    } else if ((indicator_short[i-1] > indicator_long[i-1]) & indicator_short[i-1] > 3.4) {
      sides[i] <- 0
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

# backtest individual
backtest_data <- exuber_indicators[id == "1-100-5"]
strategy_returns <- backtest_cross(backtest_data$returns, SMA(backtest_data$sd_radf_sum, 10), SMA(backtest_data$sd_radf_sum, 50), return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(backtest_data$returns, strategy_returns), order.by = backtest_data$datetime))

# optimizations loop
sma_short <- c(seq(5,25, 5))
sma_long <- c(seq(50, 90, 10))
variables <- colnames(exuber_indicators)[3:(ncol(exuber_indicators))]
params <- expand.grid(sma_short, sma_long, variables, stringsAsFactors = FALSE)
returns_strategies <- list()
unique_ids <- unique(exuber_indicators$id)
for (j in seq_along(unique_ids)) {
  print(unique_ids[j])
  sample_ <- exuber_indicators[id == unique_ids[j]]
  sample_ <- sample_[order(datetime)]
  sample_ <- unique(sample_)
  x <- vapply(1:nrow(params), function(i) backtest_cross(sample_$returns,
                                                         SMA(sample_[, get(params[i, 2])], 2),
                                                         sample_[, get(params[i, 2])],
                                                         params[i, 1]),
              numeric(1))
  returns_strategies[[j]] <- cbind(radf_id = unique_ids[j], params, x)
}
optimization_results_levels <- rbindlist(returns_strategies)
head(optimization_results_levels[order(optimization_results_levels$x, decreasing = TRUE), ], 20)

# optimization summary
opt_summary <- optimization_results_levels[, median(x), by = .(Var2)]
opt_summary[order(V1, decreasing = TRUE)]
ggplot(optimization_results_levels, aes(Var1, Var2, fill= x)) +
  geom_tile()
tail(optimization_results_levels[Var2 == "sd_bsadf"][order(x)], 20)
tail(optimization_results_levels[Var2 == "sd_radf_sum"][order(x)], 20)

# backtest individual
backtest_data <- exuber_indicators[id == "1-100-1"]
strategy_returns <- backtest(backtest_data$returns, backtest_data$sd_bsadf, 0.9, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(backtest_data$returns, strategy_returns), order.by = backtest_data$datetime))

# ML PREDICTION -----------------------------------------------------------

# import python modules
library(reticulate)
use_python("C:/ProgramData/Anaconda3/python.exe")
sktime = import("sktime")
sklearn = import("sklearn")

# parameters for all ML models
sktime_tsforest <- function() {
  classifier = sktime$classification$interval_based$TimeSeriesForestClassifier()
  classifier$fit(test[[1]], test[[3]])
}

# create X
unique(exuber_indicators$id)
mldata <- exuber_indicators[id == "1-600-1"]
mldata[, returns_week := close / shift(close, 1) - 1]
mldata <- mldata[, sign := ifelse(returns_week > 0, 1, 0)]
X <- mldata[, .(sd_bsadf, sign)]
X$sd_bsadf <- SMA(X$sd_bsadf, 8)

# generate 3d data dim (WRONG OUTPUT FOR Y)
timestep_length = 10
data_gen <- flow_series_from_dataframe(
  data = as.data.frame(X),
  x = "sd_bsadf",
  y = "sign",
  length_out = 1,
  stride = 1,
  lookback = 1,
  timesteps = timestep_length,
  batch_size = nrow(X),
  mode = "training"
)
Xy <- data_gen()
x <- Xy[[1]]
dim(x)
x <- array(x, dim = c(dim(x)[1], 1, timestep_length))
x <- sktime$utils$data_processing$from_3d_numpy_to_nested(x)
y <- as.matrix(X[(timestep_length+1):nrow(X), 2])
test = sktime$forecasting$model_selection$temporal_train_test_split(x, y, test_size = 0.8)

# Time series forest
classifier = sktime$classification$interval_based$TimeSeriesForestClassifier()
classifier$fit(test[[1]], test[[3]])
y_pred_prob = classifier$predict_proba(test[[2]])
y_pred = ifelse(y_pred_prob[, 1] > 0.55, "0", "1")
table(y_pred)
sklearn$metrics$accuracy_score(test[[4]], as.integer(y_pred))

# backtest
returns <- exuber_indicators[id == "1-600-1", .(datetime, returns)]
returns <- as.xts.data.table(na.omit(returns))
returns_strategy <- returns[(length(returns) - length(y_pred) + 1):nrow(returns)] * as.integer(y_pred)
charts.PerformanceSummary(cbind(returns[(nrow(returns) - length(y_pred) + 1):nrow(returns)], returns_strategy))
