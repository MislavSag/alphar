library(evir)
library(data.table)
library(quantmod)
library(purrr)
library(ggplot2)
library(patchwork)
library(PerformanceAnalytics)
library(roll)
library(TTR)
library(simfinapi)
library(parallel)         # for the GASS package
library(BatchGetSymbols)  # import data
source('C:/Users/Mislav/Documents/GitHub/alphar/R/parallel_functions.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/outliers.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/import_data.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/features.R')

# performance
plan(multiprocess(workers = availableCores() - 8))  # multiprocess(workers = availableCores() - 8)




# PARAMETERS --------------------------------------------------------------

# before backcusum function
contract = 'SPY'
freq = 5  # 1 is one minute, 60 is hour, 480 is day etc
upsample = FALSE
add_fundamentals = FALSE
add_vix = TRUE
# model parameters
roll_window = 2000
threshold = 0.001
# backtest
sma_long = 400
sma_short = 50



# IMPORT DATA -------------------------------------------------------------

if (freq < 60) {
  paste0(contract, freq)
  # HFD
  market_data <- import_mysql(
    contract = paste0(contract, freq),
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
    contract = paste0('VIX', freq),
    save_path = 'D:/market_data/usa/ohlcv',
    trading_days = TRUE,
    upsample = upsample,
    RMySQL::MySQL(),
    dbname = 'odvjet12_market_data_usa',
    username = 'odvjet12_mislav',
    password = 'Theanswer0207',
    host = '91.234.46.219'
  )
} else if (freq == 480) {
  market_data <- BatchGetSymbols(tickers = contract, first.date = '2000-01-01')[[2]]
  market_data <- xts::xts(market_data[, c(1:6, 9, 10)], order.by = market_data$ref.date)
  market_data$returns <- market_data$ret.adjusted.prices
} else {
  # LFD
  market_data <- import_ib(
    contract = contract,
    frequencies = ifelse(freq == 60, '1 hour', '1 day'),
    duration = '30 Y',
    type = 'equity',
    trading_days = FALSE,
    what_to_show = 'TRADES'
  )
  colnames(market_data) <- c('open', 'high', 'low', 'close', 'volume', 'WAP', 'hasGaps', 'count')
  vix <- import_ib(
    contract = 'VIX',
    frequencies = ifelse(freq == 60, '1 hour', '1 day'),
    duration = '30 Y',
    type = 'index',
    trading_days = FALSE
  )
  colnames(vix) <- c('open', 'high', 'low', 'close', 'volume', 'WAP', 'hasGaps', 'count')
}

# FUNDAMENTALS
if (add_fundamentals) {
  fundamentals <- simfinapi::sfa_get_statement(Ticker = contract,
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
}



# PREPROCESSING -----------------------------------------------------------

# Remove outliers
market_data <- remove_outlier_median(market_data, median_scaler = 25)

# merge market data and VIX
if (add_vix) {
  market_data <- merge(market_data, vix[, 'close'], join = 'left')
  colnames(market_data)[ncol(market_data)] <- 'vix'
  market_data <- na.omit(market_data)
}

# Add features
market_data <- add_features(market_data)

# Remove NA values
market_data <- na.omit(market_data)
head(market_data)



# ROLL GPD ----------------------------------------------------------------

# roll gpd
gpd_roll <- frollapply_parallel(
y = market_data$returns * -1,
n_cores = -1,
roll_window = roll_window,
FUN = function(x) {
  es <- evir::gpd(x, threshold = threshold, method = 'ml', information = 'expected')
  es <- evir::riskmeasures(es, c(0.999))[, 3]
}
)
gpd_roll <- lapply(gpd_roll, function(x) x[!is.na(x)])
gpd_roll <- unlist(purrr::flatten(gpd_roll))
gpd_roll <- c(rep(NA, roll_window-1), gpd_roll)
gpd_roll <- cbind(market_data, es = gpd_roll)
gpd_roll <- na.omit(gpd_roll)

# plot
plot(gpd_roll[, c('es')])
data_plot <- as.data.table(gpd_roll[, c('close', 'es', 'vix')])
ggplot(data_plot, aes(x = index)) +
geom_line(aes(y = es * 1000), colour = 'blue') +
# geom_line(aes(y = TTR::SMA(es * 1000, 5000)), colour = 'green') +
geom_line(aes(y = vix), colour = 'black')
#geom_line(aes(y = TTR::SMA(vix, 5000)), colour = 'orange')
ggplot(data_plot, aes(x = index)) +
geom_line(aes(y = TTR::SMA(es * 1000, 1000)), colour = 'green') +
geom_line(aes(y = TTR::SMA(vix, 1000)), colour = 'red')
ggplot(data_plot, aes(x = index, y = close, colour = es > 0.015)) +
geom_line(aes(group = 1), size= 1.1)
ggplot(data_plot, aes(x = index, y = close, colour = es > 0.015 & TTR::SMA(es, 50) > TTR::SMA(es, 500))) +
geom_line(aes(group = 1), size= 1.1)
head(data_plot)



# CUSUM -------------------------------------------------------------------

# backcusum roll
# back_cusum_test <- function(data, col_name, window_size = 100, side = c('greater', 'less')) {
#   bc <- slider_parallel(
#     .x = as.data.table(data),
#     .f =   ~ {
#       formula_ <- paste0('.$', col_name, ' ~ 1')
#       backCUSUM::SBQ.test(stats::as.formula(formula_), alternative = side)[['statistic']]
#     },
#     .before = window_size - 1,
#     .complete = TRUE,
#     n_cores = -1
#   )
#   bc <- unlist(bc)
#   bc <- c(rep(NA, window_size - 1), bc)
#   return(bc)
# }
#
# # backcusum roll for chooosen variables
# results$es_greater <- back_cusum_test(results, 'es', 150, side = 'greater')
# results$es_greater_sma <- TTR::SMA(results$es_greater, 100)
# results$es_greater_sma_q <- roll::roll_quantile(
#   results$es_greater_sma, width = 12*8*200, p = 0.95, min_obs = 12*8*30)
# plot(results$es_greater)
# plot(results$es_greater_sma)
# quantile(results$es_greater, seq(0, 1, 0.05), na.rm = TRUE)
# quantile(results$es_greater_sma, seq(0, 1, 0.05), na.rm = TRUE)



# BACKTEST ----------------------------------------------------------------

# backtet functoin
backtest <- function(data, sma_long, sma_short) {

# trading rule
indicator <- as.vector(zoo::coredata(data$es))
# indicator_q <- roll::roll_quantile(indicator, 12*8*255*5, p = 0.90, min_obs = 12*8*255)
indicator_sma_long <- as.vector(zoo::coredata(TTR::SMA(indicator, sma_long)))
indicator_sma_short <- as.vector(zoo::coredata(TTR::SMA(indicator, sma_short)))
side <- vector(mode = 'integer', length = length(indicator))
for (i in 1:length(indicator)) {
  if (i == 1 || is.na(indicator_sma_long[i-1])) {
    side[i] <- NA
  } else if (indicator[i-1] > 0.010 & (indicator_sma_short[i-1] > indicator_sma_long[i-1])) {
    side[i] <- 0
  } else {
    side[i] <- 1
  }
}

# merge
returns_strategy <- xts::xts(data$returns * side, order.by = zoo::index(data))
perf <- na.omit(merge(data[, 'returns'], returns_strategy = returns_strategy))
colnames(perf)[ncol(perf)] <- 'returns_strategy'
return(perf)
}
backtest_results <- backtest(gpd_roll, 2000, 40)
charts.PerformanceSummary(backtest_results, plot.engine = "ggplot2")

# perfrmance
cumreturns <- t(Return.cumulative(backtests_all))
cumreturns <- data.table(model = row.names(cumreturns), cumreturns = cumreturns)
cumreturns <- cumreturns[, c('type', 'contract', 'window', 'p', 'sma_long', 'sma_short') := data.table::tstrsplit(cumreturns$model, split = '_')]
colnames(cumreturns)[2] <- 'return'
cumreturns[, mean(return), by = .(window)]
cumreturns[, mean(return), by = .(p)]
cumreturns[, mean(return), by = .(sma_long)]
cumreturns[, mean(return), by = .(sma_short)]
cumreturns[, mean(return), by = .(sma_long, sma_short)]
cumreturns[, median(return), by = .(sma_long, sma_short)]
# tet <- table.Drawdowns(backtests_all[, 10])
# plots <- charts.PerformanceSummary(backtests[, c('returns', 'returns_strategy')], plot.engine = 'ggplot2')

