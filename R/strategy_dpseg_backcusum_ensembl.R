library(data.table)
library(Rcpp)
library(purrr)
library(ggplot2)
library(highfrequency)
library(quantmod)
library(dpseg)
library(future.apply)
library(anytime)
library(PerformanceAnalytics)
library(roll)
library(TTR)
library(simfinapi)
source('C:/Users/Mislav/Documents/GitHub/alphar/R/parallel_functions.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/outliers.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/import_data.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/execution.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/features.R')

# performance
plan(multiprocess(workers = availableCores() - 8))  # multiprocess(workers = availableCores() - 8)



# PARAMETERS --------------------------------------------------------------

# before backcusum function
contract = 'SPY5'
upsample = FALSE
# dpseg
dpseg_p = 0.1
window_dpseg = 500
quantiles_dpseg = 0.96
quantiles_dpseg_width = 5*12*8*1000  # 1000 days (3 years)
# file name prefix
now <- gsub(' |:', '-', as.character(Sys.time()))
file_name <- paste0(contract, '_', now)



# IMPORT DATA -------------------------------------------------------------


# HFD
market_data <- import_mysql(
  contract = contract,
  save_path = 'D:/market_data/usa/ohlcv',
  trading_days = TRUE,
  upsample = upsample,
  RMySQL::MySQL(),
  dbname = 'odvjet12_market_data_usa',
  username = 'odvjet12_mislav',
  password = 'Theanswer0207',
  host = '91.234.46.219'
)
vix <- import_mysql(
  contract = 'VIX5',
  save_path = 'D:/market_data/usa/ohlcv',
  trading_days = TRUE,
  upsample = upsample,
  RMySQL::MySQL(),
  dbname = 'odvjet12_market_data_usa',
  username = 'odvjet12_mislav',
  password = 'Theanswer0207',
  host = '91.234.46.219'
)

# FUNDAMENTALS
fundamentals <- simfinapi::sfa_get_statement(Ticker = gsub('\\d+', '', contract),
                                             statement = 'all',
                                             period = 'quarters',
                                             ttm = TRUE,
                                             shares = TRUE,
                                             api_key = '8qk9Xc9scFc0Rbpfrx6PLdaiomvi7Dxc')
if (!is.null(fundamentals)) {
  fundamentals <- janitor::clean_names(fundamentals)
  fundamentals <- fundamentals[, .(publish_date, shares, sales_per_share)]
  fundamentals <- xts::xts(fundamentals[, 2:ncol(fundamentals)], order.by = as.POSIXct(fundamentals$publish_date))
  market_data <- merge(market_data, fundamentals, all = TRUE)
  market_data$sales_per_share <- zoo::na.locf(market_data$sales_per_share)
  market_data <- market_data[!is.na(market_data$close), ]
  market_data$ps <- market_data$close / market_data$sales_per_share
}



# PREPROCESSING -----------------------------------------------------------


# Remove outliers
market_data <- remove_outlier_median(market_data, median_scaler = 25)

# merge market data and VIX
market_data <- merge(market_data, vix[, 'close'], join = 'left')
colnames(market_data)[ncol(market_data)] <- 'vix'
market_data <- na.omit(market_data)

# Add features
market_data <- add_features(market_data)

# lags
market_data$returns_lag <- data.table::shift(market_data$returns)
market_data$vix_lag <- data.table::shift(market_data$vix)

# Remove NA values
market_data <- na.omit(market_data)



# DYNAMIC PROGRAMING SEGMENTS STRATEGY ------------------------------------


dpseg_roll <- function(data, var = 'close', window_size = 100) {
  columns <- c('index', var)
  DT <- as.data.table(data)[, columns, with = FALSE]
  colnames(DT) <- c('time', 'price')
  DT <- DT[, time := as.numeric(time)]
  dpseg_last <- slider_parallel(
    .x = DT,
    .f =   ~ {
      library(dpseg)
      segs <- dpseg(.x$time, .x$price, jumps=FALSE, verb=FALSE, P = 0.1)
      slope_last <- segs$segments$slope[length(segs$segments$slope)]
      r2 <- segs$segments$r2[length(segs$segments$r2)]
      cbind(slope_last = slope_last, r2 = r2)
    },
    .before = window_size - 1,
    .after = 0L,
    .complete = TRUE,
    n_cores = -1
  )
  dpseg_last <- do.call(rbind, dpseg_last)
  dpseg_last <- rbind(matrix(NA, nrow = window_size - 1, ncol = 2), dpseg_last)
  return(dpseg_last)
}
results_dpseg <- dpseg_roll(market_data, 'close', window_dpseg)

# estimate quantiles
quantile_roll <- roll::roll_quantile(results_dpseg[, 'slope_last'], width = quantiles_dpseg_width, p = quantiles_dpseg, min_obs = 50)

# merge all
results <- merge.xts(market_data, results_dpseg = results_dpseg[, 1], quantile_roll = quantile_roll, r2 = results_dpseg[, 'r2'])
results <- na.omit(results)

# backtest
side <- vector(mode = 'integer', length = nrow(results))
slopes <- as.vector(zoo::coredata(results$results_dpseg))
qrolls <- as.vector(zoo::coredata(results$quantile_roll))
r2 <- as.vector(zoo::coredata(results$r2))
for (i in 1:nrow(results)) {
  if (i == 1) {
    side[i] <- NA
  } else if (slopes[i-1] < -0.000009 & r2[i - 1] > 0.8) {
    side[i] <- 0
  } else {
    side[i] <- 1
  }
}
table(side)
results$side <- side
results$returns_strategy <- results$returns * results$side

# save graph with cumulative returns and drowdown
p <- charts.PerformanceSummary(results[, c('returns', "returns_strategy")], plot.engine = 'ggplot2')
ggplot2::ggsave(paste0(file_name, '-dpseg.png'), p)

# save xcalars
strategy_reteurn <- cumprod(1 + na.omit(as.vector(results$returns_strategy)))
cat(paste0('cumulative_return: ', round(tail(strategy_reteurn, 1), 2)))

############### PLAYGROUND
test <- market_data['2008-06-05/2008-06-10']
plot(test$close)
test_dpeg <- dpseg(as.numeric(zoo::index(test)), as.vector(test$close), P = 0.1)
plot(test_dpeg)
test_dpeg$segments
############### PLAYGROUND


# # (BACK)CUSUM ---------------------------------------------------------------
#
#
# # backcusum roll
# back_cusum_test <- function(data, col_name, window_size = 105, side = c('greter', 'less')) {
#   bc <- slider_parallel(
#     .x = as.data.table(data),
#     .f =   ~ {
#       formula_ <- paste0('.$', col_name, ' ~ 1')
#       backCUSUM::SBQ.test(stats::as.formula(formula_), alternative = side)[['statistic']]
#     },
#     .before = window_size - 1,
#     .after = 0L,
#     .complete = TRUE,
#     n_cores = -1
#   )
#   bc <- unlist(bc)
#   bc <- c(rep(NA, window_size - 1), bc)
#   return(bc)
# }
# market_data$standardized_returns_less <- back_cusum_test(market_data, 'standardized_returns', 80, side = 'less')
# market_data$standardized_returns_greater <- back_cusum_test(market_data, 'standardized_returns', 80, side = 'greater')
# market_data$standardized_returns_greater_sma <- TTR::SMA(market_data$standardized_returns_greater, 200)
# market_data$standardized_returns_less_sma <- TTR::SMA(market_data$standardized_returns_less, 200)
# market_data <- na.omit(market_data)
#
# # Log statistics
# quantile(market_data$standardized_returns_less, seq(0, 1, 0.05), na.rm = TRUE)
# quantile(market_data$standardized_returns_greater, seq(0, 1, 0.05), na.rm = TRUE)
#
# # generate sides
# side <- vector(mode = 'integer', length = nrow(market_data))
# backcusum_var <- as.vector(zoo::coredata(market_data$standardized_returns_greater_sma))
# quantile(backcusum_var, seq(0, 1, 0.05), na.rm = TRUE)
# for (i in 1:nrow(market_data)) {
#   if (i == 1) {
#     side[i] <- NA
#   } else if (backcusum_var[i -1] > 0.9) {
#     side[i] <- 0L
#   } else {
#     side[i] <- 1L
#   }
# }
# market_data$side_backcusum <- side
# table(market_data$side_backcusum)
# market_data$returns_strategy_backcusum <- market_data$returns * market_data$side_backcusum
# PerformanceAnalytics::charts.PerformanceSummary(market_data[, c('returns', "returns_strategy_backcusum")], plot.engine = 'ggplot2')
#
#
#
# # MERGE BACKCUSUM AND DSEG ------------------------------------------------
#
#
# results_ensemmbl <- merge(market_data, results[, c('side')])
# results_ensemmbl <- na.omit(results_ensemmbl)
# results_ensemmbl$side_ensembl <- ifelse(results_ensemmbl$side == 0, results_ensemmbl$side, results_ensemmbl$side_backcusum)
# results_ensemmbl$returns_strategy_ensembl <- results_ensemmbl$returns * results_ensemmbl$side_ensembl
# results_xts <- results_ensemmbl[, c('returns', 'returns_strategy_ensembl')]
# results_xts <- na.omit(results_xts)
# PerformanceAnalytics::charts.PerformanceSummary(results_xts, plot.engine = 'ggplot2')
