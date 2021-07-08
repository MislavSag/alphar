library(data.table)
library(leanr)
library(PerformanceAnalytics)
library(TTR)
library(fasttime)
library(lubridate)
library(ggplot2)
library(roll)
library(future.apply)


# parameters
ticker = 'SPY'
backtest_path <- paste0("D:/risks/risk-factors/hour/", ticker)

# import spy radf data
spy <- import_lean('D:/market_data/equity/usa/hour/trades_adjusted', ticker)
spy[, returns := close / shift(close) - 1]
bactest_files <- list.files(backtest_path, full.names = TRUE)
backtest_data <- lapply(bactest_files, fread)
names(backtest_data) <- gsub(".*/|\\.csv", "", bactest_files)
backtest_data <- rbindlist(backtest_data, idcol = TRUE)
backtest_data[, datetime := as.POSIXct(datetime, tz = "EST")]
backtest_data <- merge(backtest_data, spy[, .(datetime, returns)], by = "datetime", all.x = TRUE, all.y = FALSE)
setorderv(backtest_data, c(".id", "datetime"))
# backtest_data <- na.omit(backtest_data[, .(.id, datetime, returns, adf, sadf, gsadf, badf, bsadf)])

# calculate percentiles
cols <- colnames(backtest_data)[3:(ncol(backtest_data)-1)]
cols_new <- paste0('p_', cols)
backtest_data[, (cols_new) := lapply(.SD, function(x) roll_quantile(x, 900, p = 0.92)),
              .SDcols = cols, by = .id]

# plots
ggplot(backtest_data, aes(x = datetime)) +
  geom_line(aes(y = es_std)) +
  facet_grid(.id ~ .)
ggplot(backtest_data, aes(x = datetime)) +
  geom_line(aes(y = es_std)) +
  geom_line(aes(y = p_es_std), color = 'red') +
  facet_grid(.id ~ .)
ggplot(backtest_data[.id == "SPY_990_EWMA_age_100_150"], aes(x = datetime)) +
  geom_line(aes(y = es_std)) +
  geom_line(aes(y = p_es_std), color = 'red') +
  facet_grid(.id ~ .)



# LEVEL TRADING RULE ------------------------------------------------------

# optimization params
thresholds <- seq(0, 0.1, 0.001)
variables <- colnames(backtest_data)[3:(ncol(backtest_data)-1)]
params <- expand.grid(thresholds, variables, stringsAsFactors = FALSE)

# backtest function
backtest <- function(returns, indicator, threshold, return_cumulative = TRUE) {
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i])) {
      sides[i] <- NA
    } else if (indicator[i] > threshold) {
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

# optimizations loop
returns_strategies <- list()
unique_ids <- unique(backtest_data$.id)
for (j in seq_along(unique_ids)) {
  print(unique_ids[j])
  sample_ <- backtest_data[.id == unique_ids[j]]
  x <- vapply(1:nrow(params), function(i) backtest(sample_$returns,
                                                   sample_[, get(params[i, 2])], # sample_[, get(params[i, 2])]
                                                   params[i, 1]),
              numeric(1))
  returns_strategies[[j]] <- cbind(radf_id = unique_ids[j], params, x)
}
optimization_results <- rbindlist(returns_strategies)
head(optimization_results[order(optimization_results$x, decreasing = TRUE), ], 30)

# backtest individual
sample_ <- backtest_data[.id == "GOOG_950_EWMA_plain_100_150"] #  & datetime %between% c('2020-01-01', '2021-01-01')]
strategy_returns <- backtest(sample_$returns, sample_$es_std, 0.01, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(sample_$returns, strategy_returns), order.by = sample_$datetime))


# PERCENTILES TRADING RULE ------------------------------------------------

# backtest percentiles
backtest_percentiles <- function(returns, indicator,
                                 indicator_percentil, return_cumulative = TRUE) {
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i]) || is.na(indicator_percentil[i])) {
      sides[i] <- NA
    } else if (indicator[i] > indicator_percentil[i]) {
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

# optimization params
p <- seq(0.7, 0.99, 0.01)
roll_width <- 6 * seq(10, 300, 10)
params <- expand.grid(p, roll_width, stringsAsFactors = FALSE)
# params <- params[1:2, ]

# optimization loop
plan(multicore(workers = 8))
returns_strategies <- list()
unique_ids <- unique(backtest_data$.id)
for (j in seq_along(unique_ids)) {
  print(unique_ids[j])
  sample_ <- backtest_data[.id == unique_ids[j]]

  x <- future_lapply(1:nrow(params), function(i) {

    # calculate percentiles
    cols <- colnames(sample_)[3:(ncol(sample_)-1)]
    cols_new <- paste0('p_', cols)
    sample_[, (cols_new) := lapply(.SD, function(x) roll_quantile(x, params[i, 2], p = params[i, 1])),
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
ggplot(optimization_results_summary[grep("GARCH_age", id)], aes(x = backtets)) + geom_histogram() +
  geom_vline(xintercept = 4.1, color = "red") + facet_grid(. ~ id)
ggplot(optimization_results_summary[grep("EWMA_age", id)], aes(x = backtets)) + geom_histogram() +
  geom_vline(xintercept = 4.1, color = "red") + facet_grid(. ~ id) +
  theme( axis.text = element_text( size = 6 ),
         axis.text.x = element_text( size = 6 ),
         axis.title = element_text( size = 6, face = "bold" ),
         legend.position="none",
         strip.text = element_text(size = 6))
ggplot(optimization_results_summary[grep("EWMA_plain", id)], aes(x = backtets)) + geom_histogram() +
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
ggplot(optimization_results_summary[grep("age_100|plain_100", id)], aes(x = backtets)) + geom_histogram() +
  geom_vline(xintercept = 4.1, color = "red") + facet_grid(. ~ id) +
  theme( axis.text = element_text( size = 6 ),
         axis.text.x = element_text( size = 6 ),
         axis.title = element_text( size = 6, face = "bold" ),
         legend.position="none",
         strip.text = element_text(size = 6))
ggplot(optimization_results_summary[grep("age_100|plain_100", id)], aes(x = backtets)) + geom_histogram() +
  geom_vline(xintercept = 4.1, color = "red") + facet_grid(. ~ id) +
  theme( axis.text = element_text( size = 6 ),
         axis.text.x = element_text( size = 6 ),
         axis.title = element_text( size = 6, face = "bold" ),
         legend.position="none",
         strip.text = element_text(size = 6))

# heatmaps
ggplot(optimization_results[id == "SPY_975_EWMA_plain_100_150"], aes(Var1, Var2, fill= backtets)) +
  geom_tile()
# heatmaps for best variable
optimization_results_best <- optimization_results[col_ == "es_std"]
ggplot(optimization_results_best, aes(Var1, Var2, fill= backtets)) +
  geom_tile()
ggplot(optimization_results_best[grep("age_100|plain_100", id)], aes(Var1, id, fill= backtets)) +
  geom_tile()

# backtest individual percentiles
sample_ <- backtest_data[.id == paste0(ticker, "_950_EWMA_plain_100_150")] #  & datetime %between% c('2020-01-01', '2021-01-01')]
cols <- colnames(sample_)[3:(ncol(sample_)-1)]
cols_new <- paste0('p_', cols)
sample_[, (cols_new) := lapply(.SD, function(x) roll_quantile(x, 700, p = 0.89)), .SDcols = cols, by = .id]
strategy_returns <- backtest_percentiles(sample_$returns, sample_$es_std, sample_$p_es_std, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(sample_$returns, strategy_returns), order.by = sample_$datetime))
ind <- 1:15000
charts.PerformanceSummary(xts(cbind(sample_$returns[ind], strategy_returns[ind]), order.by = sample_$datetime[ind]))
ind <- 15000:20000
charts.PerformanceSummary(xts(cbind(sample_$returns[ind], strategy_returns[ind]), order.by = sample_$datetime[ind]))
ind <- 20000:25000
charts.PerformanceSummary(xts(cbind(sample_$returns[ind], strategy_returns[ind]), order.by = sample_$datetime[ind]))
