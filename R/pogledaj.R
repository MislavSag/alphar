# market data
# fundamental
library(data.table)
library(purrr)
library(xts)
library(tsbox)
library(slider)
library(tidyr)
library(stringr)
library(ggplot2)
library(ggpubr)
# data
library(IBrokers)
symbols <- c('SPY')  # , 'AMZN', 'UAL', 'T'
freq <- c('1 hour')
period <- c(4)
data_length <- '15 Y'
use_logs <- TRUE
# create tw connection and import data for every symbol
tws <- twsConnect(clientId = 3, host = '127.0.0.1', port = 7496)
contracts <- lapply(symbols, twsEquity, exch = 'SMART', primary = 'ISLAND')
market_data <- lapply(contracts, function(x) {
  lapply(freq, function(y) {
    ohlcv <- reqHistoricalData(tws, x, barSize = y, duration = data_length)
  })
})
market_data <- purrr::flatten(market_data)  # unlist second level
names(market_data) <- unlist(lapply(symbols, function(x) paste(x, gsub(' ', '_', freq), sep = '_')))
twsDisconnect(tws)

# remove outliers from intraday market data
market_data <- lapply(market_data, function(x) {
  remove_outlier_median(quantmod::OHLC(x), median_scaler = 25)
})

# example data
exampledata <- market_data$SPY_1_hour
exampledata <- as.data.table(exampledata)
exampledata <- exampledata[, paste0('SPY.Close', 1:3) := shift(SPY.Close, 1:3)][]
exampledata[, return := (SPY.Close - shift(SPY.Close)) / shift(SPY.Close)]
exampledata <- exampledata[, paste0('return', 1:3) := shift(return, 1:3)][]
exampledata <- na.omit(exampledata)

# 
test <- slider::slide(
  .x = exampledata,
  .f = ~ {
    # window_data <- .[, c('SPY.Close', 'SPY.Volume')]
    # SPY.Close1 <- .$SPY.Close1 - mean(.$SPY.Close1)
    # SPY.Close2 <- .$SPY.Close2 - mean(.$SPY.Close2)
    # SPY.Close3 <- .$SPY.Close3 - mean(.$SPY.Close3)
    # SPY.Volume <- .$SPY.Volume - mean(.$SPY.Volume)
    # x_ <- .$SPY.Close
    # y_ <- .$SPY.Close- mean(.$SPY.Close)
    # tm_vec <- lm(y_ ~ SPY.Close1 + SPY.Close2 + SPY.Close3 + SPY.Volume)
    y_ <- .$return - mean(.$return)
    x_ <- .$return1 - mean(.$return1)
    tm_vec <- lm(y_ ~ x_)
  },
  .before = 30L - 1L,
  .after = 0L,
  complete = TRUE
)
coefs <- lapply(test, function(x) {
  std_coef <- coef(x) / sqrt(diag(vcov(x)))
  # std_coef <- summary(x)$adj.r.squared
  unlist(std_coef[[2]])
  })
results <- cbind.data.frame(exampledata, coefs = unlist(coefs))
# plot(results$coefs, type = 'l')

# results
# input <- market_data[[1]]
# input <- input[3:nrow(input), ]
# results <- cbind.data.frame(t = tm_vec, input)
# results$return <- (results$SPY.Close - shift(results$SPY.Close)) / shift(results$SPY.Close)
results <- na.omit(results)
critical_value <- 1
for (i in 1:nrow(results)) {
  if (i == 1) {
    signs <- NA
  } else if (results[i-1, 'coefs'] < critical_value) {
    signs <- c(signs, 1)
  } else if (results[i-1, 'coefs'] > critical_value) {
    # if (results$return1[i-1] > 0) {
    #   signs <- c(signs, 1)
    # } else {
    #   signs <- c(signs, 0)
    # }
    signs <- c(signs, 0)
  } else {
    signs <- c(signs, 1)
  }
}
results$signs <- signs
results$returns_strategy <- results$return * results$sign
results <- na.omit(results)
time <- zoo::index(close)
perf <- xts::xts(results[, c('return', 'returns_strategy')], order.by = results$index)

PerformanceAnalytics::charts.PerformanceSummary(perf)



# inputs
ndn <- exampledata[, 1, drop = FALSE]
ndn <- as.matrix(as.integer((as.numeric(ndn$index) / 86400)) + 7195290)
y <- exampledata[, 13, drop = FALSE]
x <- exampledata[, 14, drop = FALSE]
m = 30
T_star = 302-m
E <- nrow(y)

# init
regnum <- 0
frdate <- -99
max_alphah <- -99
lx <- as.matrix(lag1(x[, 1, drop = FALSE]))
tm_vec <- as.matrix(rep(0, E))
k <- 0  # not defined in original function!

# t values
e <- m + 1
while (e <= E) {
  y_sub <- y[(e-m+1):e, 1]
  lx_sub <- lx[(e-m+1):e, 1]
  x_ <- as.matrix(y_sub-mean(unlist(y_sub)))
  y_ <- as.matrix(lx_sub-mean(unlist(lx_sub)))
  tm_vec[e] <- t_sub_proc(x_, y_)
  e <- e + 1
}

# performance
close <- market_data[[1]]$SPY.Close
close <- close[5:length(close)]
results <- cbind.data.frame(t = tm_vec, close = close)
results$return <- (close - shift(close)) / shift(close)
results <- na.omit(results)
critical_value <- 1
for (i in 1:nrow(results)) {
  if (i == 1) {
    signs <- NA
  } else if (results[i-1, 't'] < critical_value) {
    signs <- c(signs, 1)
  } else if (results[i-1, 't'] > critical_value) {
    if (results$return[i-1] > 0) {
      signs <- c(signs, 0)
    } else {
      signs <- c(signs, 1)
    }
    # signs <- c(signs, 0)
  } else {
    signs <- c(signs, 1)
  }
}
results$signs <- signs
results$returns_strategy <- results$return * results$sign
results <- na.omit(results)
time <- zoo::index(close)
perf <- xts::xts(results[, c('return', 'returns_strategy')], order.by = time[3:length(time)])

PerformanceAnalytics::charts.PerformanceSummary(perf)

nrow(results[results$signs == 0 & results$return < 0, ]) / nrow(results[results$signs == 0, ])


# plot t values
par(mfrow=c(2,1))
plot(tm_vec[1:500], type = 'l')
plot(results$coefs[1:500], type = 'l')



