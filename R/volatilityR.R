library(data.table)
library(Rcpp)
library(purrr)
library(ggplot2)
library(mlflow)
library(highfrequency)
library(quantmod)
library(backCUSUM)
library(future.apply)
library(anytime)
library(PerformanceAnalytics)
library(roll)
source('C:/Users/Mislav/Documents/GitHub/alphar/R/parallel_functions.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/outliers.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/import_data.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/execution.R')

# performance
plan(multiprocess)  # for multithreading  workers = availableCores() - 8)



# PARAMETERS --------------------------------------------------------------

# before backcusum function
contract = 'SPY5'
upsample = FALSE
std_window = 50
backcusum_rolling_window = 100
# after bsckcusum function
sum_returns_param <- c(1:5)
# file name prefix
file_name <- paste0(contract, '-', upsample, '-', std_window, '-',
                    backcusum_rolling_window, '-')



# IMPORT DATA -------------------------------------------------------------


# Import data
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

# market_data <- import_ib(
#   contract = 'SPY',
#   frequencies = '1 hour',
#   duration = '15 Y'
# )

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



# PREPROCESSING -----------------------------------------------------------


# Remove outliers
market_data <- remove_outlier_median(market_data, median_scaler = 25)

# merge market data and VIX
market_data <- merge(market_data, vix[, 'close'], join = 'left')
colnames(market_data)[ncol(market_data)] <- 'vix'
market_data <- na.omit(market_data)



# VOLATILITY  --------------------------------------------------

# Spot volatility
# spot_vol <- spotVol(market_data$close, method = 'RM', lookBackPeriod = 3, on = 'minutes', k = 5, marketOpen = "09:30:00", marketClose = "16:00:00")
# market_data <- merge.xts(market_data_low, spot_vol$spot, all = c(TRUE, FALSE))[zoo::index(market_data_low)]


# spot_vol <- slider_parallel(
#   .x = as.data.table(market_data),
#   .f =   ~ {
#     library(data.table)
#     library(highfrequency)
#     cols <- c('index', 'close')
#     data_ <- .x[, ..cols]
#     colnames(data_) <- c('DT', 'PRICE')
#     vol <- tryCatch({
#         vol <- spotVol(data_, method = 'detPer', dailyvol = 'bipower',
#                        on = 'minutes', k = 60, marketOpen = '09:30:00',
#                        marketClose = '16:00:00', tz = 'America/New_York')
#         vol <- vol$spot
#         vol <- tail(vol, 1)
#       },
#       error = function(e) NA
#     )
#     return(vol)
#   },
#   .before = 5000,
#   .after = 0L,
#   .complete = TRUE,
#   n_cores = -1
# )
# vol_clean <- purrr::flatten(vol)
# vol_clean <- unlist(vol_clean)
# errors <- lapply(vol_clean, is.na)
# sum(unlist(errors))
# vol_clean <- purrr::compact(vol_clean)
#
# length(vol_clean)
# nrow(market_data)
#
# market_data_vol <- cbind.data.frame(market_data[(5000+1):nrow(market_data),], vol_clean)
# market_data_vol <- na.omit(market_data_vol)
# market_data_vol <- xts::xts(market_data_vol,
#                             order.by = as.POSIXct(rownames(market_data_vol), tz = 'America/New_York'))
# head(market_data_vol, 50)
# tail(market_data_vol, 20)

# roll std volatility
market_data$std <- roll::roll_sd(Cl(market_data), width = std_window)
market_data$std_lag <- data.table::shift(market_data$std)

# Close-to-Close Volatility
obs_per_year <- 60 / 30 * 8.5 * 255
n_ <- 5
market_data$ctc <- volatility(OHLC(market_data), n = n_, calc = "close", N = obs_per_year)
market_data$parkinson <- volatility(OHLC(market_data), n = n_, calc = "parkinson", N = obs_per_year)
market_data$rogers <- volatility(OHLC(market_data), n = n_, calc = "rogers.satchell", N = obs_per_year)
market_data$gkyz <- volatility(OHLC(market_data), n = n_, calc = "gk.yz", N = obs_per_year)
market_data$yz <- volatility(OHLC(market_data), n = n_, calc = "yang.zhang", N = obs_per_year)

# price
market_data$returns <- PerformanceAnalytics::Return.calculate(Cl(market_data))
market_data$returns_squared <- market_data$returns^2
market_data$returns_squared_lag <- data.table::shift(market_data$returns_squared)
market_data$stand_price <- Cl(market_data) / market_data$std
market_data$returns_std <- PerformanceAnalytics::Return.calculate(market_data$stand_price)
market_data$returns_squared_stand <- (market_data$returns)^2

# market_data$std_2 <- TTR::volatility(quantmod::OHLC(market_data), n = 30, calc = 'yang.zhang', N = 26010)

# parm_bipower <- spotVol(market_data$close, RM = "bipower", lookback = 50,
#                         on = 'minutes', k = 10, marketOpen = '09:30:00',
#                         marketClose = '16:00:00', tz = 'America/New_York')
# plot(rv_bipower)
# tail(rv_bipower$spot, 50)
#
#
# vol <- slider_parallel(
#   .x = as.data.table(market_data),
#   .f =   ~ {
#     library(data.table)
#     library(highfrequency)
#     cols <- c('index', 'close')
#     data_ <- .x[, ..cols]
#     colnames(data_) <- c('DT', 'PRICE')
#     vol <- tryCatch({
#       vol <- spotVol(data_, method = 'detPer', dailyvol = 'bipower',
#                      on = 'minutes', k = 10, marketOpen = '09:30:00',
#                      marketClose = '16:00:00', tz = 'America/New_York')
#       vol <- vol$spot
#       vol <- tail(vol, 1)
#     },
#     error = function(e) NA
#     )
#     return(vol)
#   },
#   .before = 5000,
#   .after = 0L,
#   .complete = TRUE,
#   n_cores = -1
# )

# remove na values
market_data <- na.omit(market_data)



# (BACK)CUSUM ---------------------------------------------------------------


# backcusum roll
bc_greater_return <- slider_parallel(
  .x = as.data.table(market_data),
  .f =   ~ {
    backCUSUM::BQ.test(.$returns_squared_stand ~ 1, alternative = "greater")
  },
  .before = backcusum_rolling_window - 1,
  .after = 0L,
  .complete = TRUE,
  n_cores = -1
)
bc_less_return <- slider_parallel(
  .x = as.data.table(market_data),
  .f =   ~ {
    backCUSUM::BQ.test(.$returns_squared_stand ~ 1, alternative = "less")
  },
  .before = backcusum_rolling_window - 1,
  .after = 0L,
  .complete = TRUE,
  n_cores = -1
)

bc_greater_std <- slider_parallel(
  .x = as.data.table(market_data),
  .f =   ~ {
    backCUSUM::BQ.test(.$std ~ 1 + .$std_lag, alternative = "greater")
  },
  .before = backcusum_rolling_window - 1,
  .after = 0L,
  .complete = TRUE,
  n_cores = -1
)
# bc_less_std <- slider_parallel(
#   .x = as.data.table(market_data),
#   .f =   ~ {
#     backCUSUM::BQ.test(.$yz ~ 1, alternative = "less")
#   },
#   .before = backcusum_rolling_window - 1,
#   .after = 0L,
#   .complete = TRUE,
#   n_cores = -1
# )




# EXECUTION AND BACKTEST --------------------------------------------------


# Extract value
bc_greater_rejection_return <- unlist(lapply(bc_greater_return, function(x) x[['statistic']]))
bc_less_rejection_return <- unlist(lapply(bc_less_return, function(x) x[['statistic']]))

# rejection test
bc_less_rejection_return <- bc_less_rejection_return > quantile(bc_less_rejection_return, seq(0, 1, by = 0.05))[20]
bc_greater_rejection_return <- bc_greater_rejection_return > quantile(bc_greater_rejection_return, seq(0, 1, by = 0.05))[20]

# Merge market data and backcusum results
market_data_sample <- market_data[(nrow(market_data)-length(bc_greater_rejection_return)+1):nrow(market_data),]
results <- cbind.data.frame(
  market_data_sample,
  bc_greater_rejection_return = bc_greater_rejection_return,
  bc_less_rejection_return = bc_less_rejection_return)
results <- na.omit(results)


# Backtest
Rcpp::cppFunction('NumericVector create_signs_test(NumericVector greater) {
    int n = greater.size();
    NumericVector sign(n);

    for(int i = 1; i < n; ++i) {
      if (i == 1) sign[i] = R_NaN;
      else if(greater[i - 1] > 0) sign[i] = 0;
      else sign[i] = 1;
    }
    return sign;
}')

backtest <- function(df, greater) {
  df$side <- create_signs_test(greater)
  df$returns_strategy <- df$returns * df$side
  results_xts <- df[, c('returns', 'returns_strategy')]
  results_xts <- na.omit(results_xts)
  p <- charts.PerformanceSummary(results_xts, plot.engine = 'ggplot2')
  return(list(returns = results_xts, plot = p))
}

# test
test <- backtest(results, results$returns)



# Backtest results
backtest_results <- lapply(
  results[, grepl('bc_', colnames(results))],
  function(x) {
    lapply(results[, sum_returns_colnames],
           function(y) {
             backtest(results, x, y)
           })
  })
backtest_results <- purrr::pluck(purrr::flatten(purrr::flatten(backtest_results)))
backtest_charts <- backtest_results[seq(2, length(backtest_results), by = 2)]
backtest_returns <- backtest_results[seq(1, length(backtest_results), by = 2)]

# save charts
backtest_charts_names <- lapply(
  colnames(results)[grepl('bc_', colnames(results))],
  function(x) {
    lapply(sum_returns_colnames,
      function(y) {
        paste0(file_name, x, y, '.png')
      })
  })
backtest_charts_names <- purrr::flatten(backtest_charts_names)
purrr::map2(backtest_charts_names, backtest_charts, ~ ggsave(filename = .x, plot = .y))

# scalars
lapply(backtest_returns, function(x) {
  daily_returns <- apply.monthly(x, Return.cumulative)
  # sharpe ratio
  sr <- PerformanceAnalytics::SharpeRatio.annualized(daily_returns)
  cat("sharpe_ratio:", sr[1, 2], "\n")
  # cumulative return
  cum_return <- PerformanceAnalytics::Return.cumulative(daily_returns)
  cat("cumulative_return:", cum_return[1, 2], "\n")
})
