library(data.table)
library(leanr)
library(PerformanceAnalytics)
library(TTR)
library(fasttime)
library(lubridate)
library(ggplot2)
library(future.apply)
library(kerasgenerator)
library(reticulate)


# SETUP -------------------------------------------------------------------

# import python modules
use_python("C:/ProgramData/Anaconda3/python.exe")
sktime <- reticulate::import("sktime")
sklearn <- reticulate::import("sklearn")
pd <- import("pandas")

# parameters
data_freq = "hour"
tickers = "SPY" # if all tickers set to NA



# IMPORT DATA -------------------------------------------------------------

# import spy market_data
radf_data_path <- paste0("D:/risks/radf-", data_freq)
if (data_freq == "hour") {
  spy <- import_lean('D:/market_data/equity/usa/hour/trades_adjusted', tickers)
  spy[, returns := close / shift(close) - 1]
} else if (data_freq == "minute") {
  spy <- get_market_equities_minutes('D:/market_data/equity/usa/minute', tickers)
  spy[, time := format.POSIXct(datetime, "%H:%M:%S")]
  spy <- spy[time %between% c("08:00:00", "16:00:00")]
}

# import radf data
radf_data_files <- list.files(radf_data_path, full.names = TRUE)
radf_data <- lapply(radf_data_files, function(x) {
  spy_file <- list.files(x, pattern = "SPY", full.names = TRUE)
  if (length(spy_file) != 0) {
    y <- fread(spy_file)
    y[, id := gsub(".*/|\\.csv", "", x)]
  } else {
   y <- NULL
  }
  y
})
radf_data <- rbindlist(radf_data)
radf_data <- merge(radf_data, spy[, .(datetime, close)], by = "datetime", all.x = TRUE, all.y = FALSE)
setorderv(radf_data, c("id", "datetime"))
radf_data[, returns := close / shift(close) - 1, by = .(id)]
radf_data[, returns_day := close / shift(close, 8) - 1, by = .(id)]
radf_data[, returns_week := close / shift(close, 8 * 5) - 1, by = .(id)]
setorderv(radf_data, c("id", "datetime"))
radf_data_ml <- na.omit(radf_data[, .(id, datetime, returns, returns_week, returns_day, adf, sadf, gsadf, badf, bsadf)])
radf_data <- na.omit(radf_data[, .(id, datetime, returns, returns_day, adf, sadf, gsadf, badf, bsadf)])
radf_data <- radf_data[, radf_sum := adf + sadf + gsadf + badf + bsadf]
unique(radf_data$id)
# plot(radf_data[id == "1-100-1", .(radf_sum)][[1]], type = "l")

# backtest function
backtest_return <- function(returns, indicator, threshold, return_cumulative = TRUE) {
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


# backtest function
backtest_return <- function(returns, returns_day, indicator, threshold, return_cumulative = TRUE) {
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1])) {
      sides[i] <- NA
    } else if ((indicator[i-1] > threshold & (returns_day[i-1] < 0)) | (indicator[i-1] > 2)) {
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

# backtest SMA
backtest_sma_cross <- function(returns, indicator, threshold,
                               return_cumulative = TRUE) {

  # sma series
  sma_short <- SMA(indicator, 8)
  sma_long <- SMA(indicator, 22*3)
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(sma_long[i-1])) {
      sides[i] <- NA
    } else if (sma_short[i-1] > threshold & (sma_short[i-1] > sma_long[i-1])) {
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


# performance function
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

# optimization
thresholds <- seq(0.6, 4, 0.05)
# sma_width <- c(8, 8 * 22)
variables <- colnames(radf_data)[5:(ncol(radf_data))]
# params <- expand.grid(thresholds, variables, sma_width, stringsAsFactors = FALSE)
params <- expand.grid(thresholds, variables, stringsAsFactors = FALSE)

# optimizations loop
returns_strategies <- list()
unique_ids <- unique(radf_data$id)
for (j in seq_along(unique_ids)) {
  print(unique_ids[j])
  sample_ <- radf_data[id == unique_ids[j]]
  x <- vapply(1:nrow(params), function(i) backtest_sma_cross(sample_$returns,
                                                             # EMA(sample_[, get(params[i, 2])], n = params[i, 3]),
                                                             sample_[, get(params[i, 2])],
                                                             params[i, 1]),
              numeric(1))
  returns_strategies[[j]] <- cbind(radf_id = unique_ids[j], params, x)
}
optimization_results <- rbindlist(returns_strategies)
head(optimization_results[order(optimization_results$x, decreasing = TRUE), ], 50)
unique(optimization_results$radf_id)

# optimization summary
opt_summary <- optimization_results[, median(x), by = .(Var2)]
opt_summary[order(V1, decreasing = TRUE)]
optimization_results[Var2 == "sadf" & Var1 > 3]
ggplot(optimization_results, aes(x, fill = radf_id)) +
  geom_histogram(position = "identity", alpha = 0.6) +
  geom_vline(xintercept = 4.5)
ggplot(optimization_results, aes(x = x)) + geom_histogram() +
  geom_vline(xintercept = 4.5, color = "red") + facet_grid(. ~ radf_id)
ggplot(optimization_results, aes(x = x)) + geom_histogram() +
  geom_vline(xintercept = 4.5, color = "red") + facet_grid(. ~ Var2)
ggplot(optimization_results, aes(Var1, Var2, fill= x)) +
  geom_tile()
ggplot(optimization_results, aes(radf_id, Var2, fill= x)) +
  geom_tile()
ggplot(optimization_results, aes(radf_id, Var1, fill= x)) +
  geom_tile()

# backtest individual
backtest_data <- radf_data[id == "1-200-2"]
# strategy_returns <- backtest(backtest_data$returns, backtest_data$returns_day, backtest_data$bsadf, 0.65, return_cumulative = FALSE)
strategy_returns <- backtest_sma_cross(backtest_data$returns, backtest_data$gsadf, 2.3, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(backtest_data$returns, strategy_returns), order.by = backtest_data$datetime))
Performance(xts(strategy_returns, order.by = backtest_data$datetime))
Performance(xts(backtest_data$returns, order.by = backtest_data$datetime))


# PERCENTILES TRADING RULE ------------------------------------------------

# calculate percentiles
backtest_data <- copy(radf_data)

# plot percentiles
# backtest_data_plot <- copy(backtest_data)
# cols <- colnames(backtest_data_plot)[4:(ncol(backtest_data_plot))]
# cols_new <- paste0('p_', cols)
# backtest_data_plot[, (cols_new) := lapply(.SD, function(x) roll::roll_quantile(x, 8*30, p = 0.90)),
#               .SDcols = cols, by = id]
# ggplot(backtest_data_plot, aes(x = datetime)) +
#   geom_line(aes(y = gsadf)) +
#   facet_grid(id ~ .)
# ggplot(backtest_data_plot, aes(x = datetime)) +
#   geom_line(aes(y = gsadf)) +
#   geom_line(aes(y = p_gsadf), color = 'red') +
#   facet_grid(id ~ .)
# ggplot(backtest_data[.id == "SPY_990_EWMA_age_100_150"], aes(x = datetime)) +
#   geom_line(aes(y = es_std)) +
#   geom_line(aes(y = p_es_std), color = 'red') +
#   facet_grid(.id ~ .)

# optimization params
p <- seq(0.8, 0.99, 0.01)
roll_width <- c(8 * 22 * seq(1, 12, 1), 8 * 22 * 12 * seq(2, 6, 1))
params <- expand.grid(p, roll_width, stringsAsFactors = FALSE)

# optimization loop
plan(multicore(workers = 8))
returns_strategies <- list()
unique_ids <- unique(backtest_data$id)
for (j in seq_along(unique_ids)) {
  print(unique_ids[j])
  sample_ <- backtest_data[id == unique_ids[j]]

  x <- future_lapply(1:nrow(params), function(i) {

    # calculate percentiles
    cols <- colnames(sample_)[4:(ncol(sample_))]
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
ggplot(optimization_results[col_ == "gsadf"], aes(Var1, Var2, fill= backtets)) +
  geom_tile()
ggplot(optimization_results[col_ == "gsadf" & Var2 < 1500], aes(Var1, Var2, fill= backtets)) +
  geom_tile()
ggplot(optimization_results[col_ == "gsadf" & Var2 < 1500], aes(Var1, id, fill= backtets)) +
  geom_tile()

ggplot(optimization_results_summary[grep("1-600-1", id)], aes(x = backtets)) + geom_histogram() +
  geom_vline(xintercept = 4.1, color = "red") + facet_grid(. ~ id)
ggplot(optimization_results_summary[grep("1-600-1", id)], aes(x = backtets)) + geom_histogram() +
  geom_vline(xintercept = 4.1, color = "red") + facet_grid(. ~ id) +
  theme( axis.text = element_text( size = 6 ),
         axis.text.x = element_text( size = 6 ),
         axis.title = element_text( size = 6, face = "bold" ),
         legend.position="none",
         strip.text = element_text(size = 6))
ggplot(optimization_results_summary[grep("1-600-1", id)], aes(x = backtets)) + geom_histogram() +
  geom_vline(xintercept = 4.1, color = "red") + facet_grid(. ~ id) +
  theme( axis.text = element_text( size = 6 ),
         axis.text.x = element_text( size = 6 ),
         axis.title = element_text( size = 6, face = "bold" ),
         legend.position="none",
         strip.text = element_text(size = 6))
ggplot(optimization_results_summary[grep("GARCH_plain", id)], aes(x = backtets)) + geom_histogram() +
  geom_vline(xintercept = 4.1, color = "red") + facet_grid(. ~ id) +
  theme( axis.text = element_text( size = 6 ),
         axis.text.x = element_text( size = 6 ),
         axis.title = element_text( size = 6, face = "bold" ),
         legend.position="none",
         strip.text = element_text(size = 6))

# heatmaps
ggplot(optimization_results[id == "1-100-1"], aes(Var1, Var2, fill= backtets)) +
  geom_tile()
# heatmaps for best variable
optimization_results_best <- optimization_results[col_ == "es_std"]
ggplot(optimization_results_best, aes(Var1, Var2, fill= backtets)) +
  geom_tile()
ggplot(optimization_results_best[grep("age_100|plain_100", id)], aes(Var1, id, fill= backtets)) +
  geom_tile()

# backtest individual percentiles
sample_ <- backtest_data[id == "1-400-1"] #  & datetime %between% c('2020-01-01', '2021-01-01')]
cols <- colnames(sample_)[4:(ncol(sample_))]
cols_new <- paste0('p_', cols)
sample_[, (cols_new) := lapply(.SD, function(x) roll::roll_quantile(x, 1500, p = 0.84)), .SDcols = cols, by = id]
strategy_returns <- backtest_percentiles(sample_$returns, sample_$gsadf, sample_$p_gsadf, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(sample_$returns, strategy_returns), order.by = sample_$datetime))
Performance(xts(strategy_returns, order.by = sample_$datetime))
Performance(xts(sample_$returns, order.by = sample_$datetime))

ind <- 1:15000
charts.PerformanceSummary(xts(cbind(sample_$returns[ind], strategy_returns[ind]), order.by = sample_$datetime[ind]))
ind <- 15000:20000
charts.PerformanceSummary(xts(cbind(sample_$returns[ind], strategy_returns[ind]), order.by = sample_$datetime[ind]))
ind <- 20000:25000
charts.PerformanceSummary(xts(cbind(sample_$returns[ind], strategy_returns[ind]), order.by = sample_$datetime[ind]))



# ML MODEL ----------------------------------------------------------------

# parameters for all ML models
sktime_tsforest <- function() {
  classifier = sktime$classification$interval_based$TimeSeriesForestClassifier()
  classifier$fit(test[[1]], test[[3]])
}

# create X
unique(radf_data_ml$id)
mldata <- radf_data_ml[id == "1-100-3"]
mldata <- mldata[, sign := ifelse(returns_week > 0, 1, 0)]
X <- mldata[, .(sadf, sign)]
X$sadf <- SMA(X$sadf, 80)

# generate 3d data dim (WRONG OUTPUT FOR Y)
timestep_length = 10
data_gen <- flow_series_from_dataframe(
  data = as.data.frame(X),
  x = "sadf",
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
test = sktime$forecasting$model_selection$temporal_train_test_split(x, y, test_size = 0.2)

# Time series forest
classifier = sktime$classification$interval_based$TimeSeriesForestClassifier()
classifier$fit(test[[1]], test[[3]])
y_pred_prob = classifier$predict_proba(test[[2]])
y_pred = ifelse(y_pred_prob[, 1] > 0.60, "0", "1")
table(y_pred)
sklearn$metrics$accuracy_score(test[[4]], as.integer(y_pred))

# backtest
returns <- radf_data_ml[id == "1-100-3", .(datetime, returns)]
sadf <- radf_data_ml[id == "1-100-3", .(datetime, sadf)]
plot(sadf, type = "l")
returns <- as.xts.data.table(na.omit(returns))
sign_1 <- as.integer(y_pred)
sign_2 <- as.integer(sadf[(length(returns) - length(y_pred) + 1):nrow(returns), sadf]) < 1
# returns_strategy <- returns[(length(returns) - length(y_pred) + 1):nrow(returns)] * ifelse(sign_1 == 0 & shift(sign_2, 1) == 0, 0, 1)
returns_strategy <- returns[(length(returns) - length(y_pred) + 1):nrow(returns)] * sign_1
charts.PerformanceSummary(cbind(returns[(nrow(returns) - length(y_pred) + 1):nrow(returns)], returns_strategy))
