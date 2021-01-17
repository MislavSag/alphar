library(data.table)
library(Rcpp)
library(purrr)
library(ggplot2)
library(highfrequency)
library(quantmod)
library(future.apply)
library(anytime)
library(PerformanceAnalytics)
library(roll)
library(strucchange)
source('C:/Users/Mislav/Documents/GitHub/alphar/R/parallel_functions.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/outliers.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/import_data.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/execution.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/features.R')

# performance
plan(multiprocess)  # for multithreading  workers = availableCores() - 8)



# PARAMETERS --------------------------------------------------------------

# import data
contract = 'SPY5'
upsample = FALSE
# file name prefix
file_name <- paste0(contract, '-', upsample, '-')



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

# LFD
market_data <- import_ib(
  contract = 'SPY',
  frequencies = '1 day',
  duration = '15 Y',
  type = 'equity',
  trading_days = FALSE
)
colnames(market_data) <- c('open', 'high', 'low', 'close', 'volume', 'WAP', 'hasGaps', 'count')
vix <- import_ib(
  contract = 'VIX',
  frequencies = '1 day',
  duration = '16 Y',
  type = 'index',
  trading_days = FALSE
)
colnames(vix) <- c('open', 'high', 'low', 'close', 'volume', 'WAP', 'hasGaps', 'count')


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


# CUSUM MONITORING --------------------------------------------------------

# use the first n observations as history period
df <- as.data.frame(zoo::coredata(market_data))
n = nrow(df) * 0.01
e1 <- efp(kurtosis_squared ~ 1, data = df[1:n, ], type="OLS-CUSUM", h=1)  # dynamic = TRUE
me1 <- mefp(e1, alpha=0.05)
me3 <- monitor(me1, data=df)
plot(me3)



# LM instability ----------------------------------------------------------


roll_model <- function(data, window_size = 100) {
  coefficient <- slider_parallel(
    .x = as.data.table(data),
    .f =   ~ {
      x <- lm(.x$returns ~ 1)
      # $coefficients[2]
      x$coefficients
    },
    .before = window_size - 1,
    .after = 0L,
    .complete = TRUE,
    n_cores = -1
  )
  coefficient <- unlist(coefficient)
  coefficient <- c(rep(NA, window_size-1), coefficient)
}
test <- roll_model(market_data, 50)
plot(test, type = 'l')

# Roll regresion
# library(rollRegres)
# test <- roll::roll_lm(standardized_returns ~ 1,
#                       data = market_data,
#                       width = 16,
#                       do_compute = c('r.squareds'))
# r2 <-test$r.squareds
# plot(r2, type = 'l')
# test <- test$coefs[, 2]
# plot(test, type = 'l')

# df table
df <- cbind(market_data, coefs = test)
df$coefs_mean <- roll_mean(df$coefs, 8)
plot(df$coefs_mean, type = 'l')
df <- na.omit(df)

# sides!
side <- vector('integer', nrow(df))
coefs <- df$coefs_mean
label <- ifelse(market_data$returns > 0, 1, -1)
for (i in 1:nrow(df)) {
 if (i %in% c(1, 2)) {
   side[i] <- NA
 } else if (coefs[i-1] > 0.001) {
   side[i] <- -1
 } else {
   side[i] <- 1
 }
}
side <- ifelse(side == -1, 0, 1)
sum(side == 0, na.rm = TRUE) / length(side)  # TRADE FREQ
nrow(df) * (sum(side == 0, na.rm = TRUE) / length(side))  # Number of trades
df$returns_strategy <- df$returns * side
perf <- xts::xts(df[, c('returns', 'returns_strategy')])
PerformanceAnalytics::charts.PerformanceSummary(perf, plot.engine = 'ggplot2')



# TVREG -------------------------------------------------------------------

library(tvReg)
test <- tvLM(returns ~ returns_lag, data = market_data)
plot(test$coefficients[, 2])

test <- tvAR(as.vector(market_data$returns), p = 1)
model.tvAR.80 <- confint(test, tboot = "wild2", level = 0.8, runs = 50)
plot(model.tvAR.80)

