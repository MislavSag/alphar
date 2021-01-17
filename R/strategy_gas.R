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
library(gridExtra)
library(forecast)
library(tseries)
library(rugarch)
library(GAS)
library(parallel)         # for the GASS package
library(BatchGetSymbols)  # import data
source('C:/Users/Mislav/Documents/GitHub/alphar/R/parallel_functions.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/outliers.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/import_data.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/execution.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/features.R')

# performance
plan(multiprocess(workers = availableCores() - 4))  # multiprocess(workers = availableCores() - 8)




# PARAMETERS --------------------------------------------------------------

# before backcusum function
contract = 'AMZN'
freq = 480  # 1 is one minute, 60 is hour, 480 is day etc
upsample = FALSE
add_fundamentals = FALSE
add_vix = FALSE
# model parameters
roll_window = 150
moving = 'none'  # none, sma, ema
moving_window = 20
var_threshold = -0.002
# file name prefix
now <- gsub(' |:', '-', as.character(Sys.time()))
file_name <- paste0('var-', contract, '_', now)



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
# market_data <- remove_outlier_median(market_data, median_scaler = 25)

# merge market data and VIX
if (add_vix) {
  market_data <- merge(market_data, vix[, 'close'], join = 'left')
  colnames(market_data)[ncol(market_data)] <- 'vix'
  market_data <- na.omit(market_data)
}

# Add features
# market_data$returns <- Return.calculate(market_data$close, method = 'log')
# market_data <- add_features(market_data)

# Remove NA values
market_data <- na.omit(market_data)
head(market_data)


# DESCRIPTIVES ------------------------------------------------------------

# # plot prices
# qplot(x = zoo::index(market_data) , y = market_data$close, geom = 'line') + geom_line(color = 'darkblue') +
#   labs(x = '' , y = 'Price' , title = contract) + geom_hline(yintercept = mean(market_data$close) , color = 'red')
#
# # returns and distribution
# p1 = qplot(x = zoo::index(market_data) , y = market_data$returns , geom = 'line') + geom_line(color = 'darkblue') +
#   geom_hline(yintercept = mean(market_data$returns) , color = 'red' , size = 1) +
#   labs(x = '' , y = 'Daily Returns')
# p2 = qplot(market_data$returns , geom = 'density') + coord_flip() + geom_vline(xintercept = mean(market_data$returns) , color = 'red' , size = 1) +
#   geom_density(fill = 'lightblue' , alpha = 0.4) + labs(x = '')
# grid.arrange(p1 , p2 , ncol = 2)
#
# # adf test
# adf.test(market_data$returns)
#
# # arima
# model.arima = auto.arima(market_data$returns , max.order = c(3 , 0 ,3) , stationary = TRUE , trace = T , ic = 'aicc')
# model.arima
# model.arima$residuals %>% ggtsdisplay(plot.type = 'hist' , lag.max = 14)
# ar.res = model.arima$residuals
# Box.test(model.arima$residuals , lag = 14 , fitdf = 2 , type = 'Ljung-Box')
# tsdisplay(ar.res^2 , main = 'Squared Residuals')
#
# # below quantile returns
# qplot(market_data$returns , geom = 'histogram') + geom_histogram(fill = 'lightblue' , bins = 30) +
#   geom_histogram(aes(market_data$returns[market_data$returns < quantile(market_data$returns , 0.05)]) , fill = 'red' , bins = 30) +
#   labs(x = 'Daily Returns')
#
# # t distribution
# fitdist(distribution = 'std' , x = market_data$returns)$pars
# cat("For a = 0.05 the quantile value of normal distribution is: " ,
#     qnorm(p = 0.05) , "\n" ,
#     "For a = 0.05 the quantile value of t-distribution is: " ,
#     qdist(distribution = 'std' , shape = 3.7545967917 , p = 0.05) , "\n" , "\n" ,
#     'For a = 0.01 the quantile value of normal distribution is: ' ,
#     qnorm(p = 0.01) , "\n" ,
#     "For a = 0.01 the quantile value of t-distribution is: " ,
#     qdist(distribution = 'std' , shape = 3.7545967917 , p = 0.01) , sep = "")



# GARCH -------------------------------------------------------------------

# # Model specification
# model.spec = ugarchspec(variance.model = list(model = 'sGARCH' , garchOrder = c(1 , 1)) ,
#                         mean.model = list(armaOrder = c(0 , 0)))
# model.fit = ugarchfit(spec = model.spec , data = ar.res , solver = 'solnp')
# model.fit@fit$matcoef




# GAS RISK MEASURES --------------------------------------------------------------------

# specification of general autoregresive scoring model
GASSpec = UniGASSpec(
  Dist = "sstd",
  ScalingType = "Identity",
  GASPar = list(location = TRUE, scale = TRUE, skewness = TRUE, shape = TRUE))  # only volatility is time varying

# Perform 1-step ahead rolling forecast with refit
insample_length <- 500
cluster <- makeCluster(30)
Roll <- UniGASRoll(
  market_data$ret.adjusted.prices,
  GASSpec,
  Nstart = insample_length,
  RefitEvery = 5,
  RefitWindow = c("moving"),
  cluster = cluster)
stopCluster(cluster)
rm("cluster")

# forecasts_gas <- getForecast(Roll)
alphas <- c(0.01, 0.05)
var_gas <- quantile(Roll, probs = alphas)  # VaR
colnames(var_gas) <- paste0('var_gas_', as.character(alphas * 100))
es_gas <- GAS::ES(Roll, probs = alphas)
colnames(es_gas) <- paste0('es_gas_', as.character(alphas * 100))
forecasts_gas <- GAS::getForecast(Roll)
colnames(forecasts_gas) <- paste0('forecasts_gas_', colnames(forecasts_gas))

# merge to market_data
market_data <- merge(market_data, rbind(matrix(NA, insample_length, length(alphas)), var_gas))
market_data <- merge(market_data, rbind(matrix(NA, insample_length, length(alphas)), es_gas))
market_data <- merge(market_data, rbind(matrix(NA, insample_length, ncol(forecasts_gas)), zoo::coredata(forecasts_gas)))

# plots
# autoplot(na.omit(market_data[, grep('var_gas', colnames(market_data))]), facet = TRUE)
# autoplot(na.omit(market_data[, grep('es_gas', colnames(market_data))]), facet = TRUE)
# autoplot(market_data$forecasts_gas_location)
# autoplot(market_data$forecasts_gas_scale)
# autoplot(market_data$forecasts_gas_skewness)
# autoplot(market_data$forecasts_gas_shape)
# autoplot(market_data$forecasts_gas_location / market_data$forecasts_gas_scale)
# autoplot(TTR::SMA(market_data$es_gas_1 / market_data$forecasts_gas_scale))
# autoplot(market_data$es_gas_1 / TTR::SMA(market_data$forecasts_gas_scale))


# CF VAR ------------------------------------------------------------------

# # backcusum roll
# cfvar_roll <- function(returns, window_size = 100, p_ = 0.95) {
#   returns <- zoo::coredata(returns)
#   cfvar <- slider_parallel(
#     .x = returns,
#     .f =   ~ {
#       PerformanceAnalytics::VaR(.x, p = p_, method = 'modified')
#     },
#     .before = window_size - 1,
#     .complete = TRUE,
#     n_cores = -1
#   )
#   cfvar <- unlist(cfvar)
#   cfvar <- c(rep(NA, window_size - 1), cfvar)
#   return(cfvar)
# }
#
# # estimate CF VAR and merge to market data
# cfvar_p <- c(0.95, 0.99)
# cfvar_rolls <- sapply(cfvar_p, function(x) {
#   x <- cfvar_roll(market_data$returns, 3, x)
#   na.locf(x, na.rm = FALSE)
#   })
# colnames(cfvar_rolls) <- paste0('cfvar_', as.character(cfvar_p * 100))
# cfvar_rolls <- xts::xts(cfvar_rolls, order.by = zoo::index(market_data))
# market_data <- merge(market_data, cfvar_rolls)
#
# # market_data$standardized_returns_greater_sma <- TTR::SMA(market_data$standardized_returns_greater, 150)
# # market_data$standardized_returns_greater_sma_q <- roll::roll_quantile(
# #   market_data$standardized_returns_greater_sma,
# #   width = 12*8*500, p = 0.90, min_obs = 12*8*30)
#
# # plots
# autoplot(na.omit(market_data[, grep('cfvar_', colnames(market_data))]), facet = TRUE)
# autoplot(market_data$cfvar_99 - market_data$cfvar_95)



# BACKTEST ----------------------------------------------------------------

# trading rule
backtest_data <- na.omit(market_data)
indicator <- as.vector(zoo::coredata(backtest_data$es_gas_1))

# indicator <- as.vector(zoo::coredata(TTR::SMA(backtest_data$es_gas_5 / backtest_data$forecasts_gas_scale), 10))

indicator_quantile <- roll::roll_quantile(indicator, width = 255*4, p = 0.05, min_obs = 10)
indicator_2 <- as.vector(zoo::coredata(backtest_data$forecasts_gas_skewness))
side <- vector(mode = 'integer', length = length(indicator))
for (i in 1:length(indicator)) {
  if (i == 1 || is.na(indicator_quantile[i-1])) {
    side[i] <- NA
  } else if (indicator[i-1] < indicator_quantile[i]) {
    side[i] <- 0
  } else {
    side[i] <- 1
  }
}
table(side, useNA = 'ifany')
returns_strategy <- xts::xts(backtest_data$returns * side, order.by = zoo::index(backtest_data))
perf <- na.omit(merge(market_data[, c('returns')], returns_strategy = returns_strategy))
charts.PerformanceSummary(perf, plot.engine = 'ggplot2')



# GOOD STRATEGIES FOR PLUMBER --------------------------------------------

# GAS
GASSpec = UniGASSpec(
  Dist = "sstd",
  ScalingType = "Identity",
  GASPar = list(location = TRUE, scale = TRUE, skewness = TRUE, shape = TRUE))
Fit = UniGASFit(GASSpec, market_data$returns[1:1000,], Compute.SE = FALSE)
Forecast = UniGASFor(Fit, H = 2, ReturnDraws = TRUE)
forecasts <- GAS::quantile(Forecast)
es <- GAS::ES(Forecast)

