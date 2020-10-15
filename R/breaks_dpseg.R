library(data.table)
library(purrr)
library(xts)
library(IBrokers)
library(quantmod)
library(dpseg)
library(PerformanceAnalytics)
library(runner)
library(future.apply)
source('R/parallel_functions.R')
source('R/outliers.R')
plan(multiprocess)  # for multithreading




# PARAMETERS --------------------------------------------------------------


symbols <- c('SPY', 'AMZN', 'T')
freq <- c('1 day', '1 hour')  # 1 day is mandatory
cache_path <- 'D:/algo_trading_files/ib_cache_r'
dpseg_rolling <- c(600, 1000)


# IMPORT DATA -------------------------------------------------------------


# create tw connection and import data for every symbol
tws <- twsConnect(clientId = 2, host = '127.0.0.1', port = 7496)
contracts <- lapply(symbols, twsEquity, exch = 'SMART', primary = 'ISLAND')
market_data <- lapply(contracts, function(x) {
  lapply(freq, function(y) {
    ohlcv <- reqHistoricalData(tws, x, barSize = y, duration = '5 Y')
  })
})
market_data <- purrr::flatten(market_data)  # unlist second level
names(market_data) <- unlist(lapply(symbols, function(x) paste(x, gsub(' ', '_', freq), sep = '_')))
twsDisconnect(tws)

# remove outliers from intraday market data
market_data <- lapply(market_data, function(x) {
  remove_outlier_median(quantmod::OHLC(x), median_scaler = 25)
})



# PARAMTERES OPTIMIZATION ---------------------------------------------------------------


# choose best P value

## NOTE: dpseg is slower for many segments!
x <- as.numeric(zoo::index(market_data[[2]]))[1:800]
y <- zoo::coredata(market_data[[2]]$SPY.Close)[1:800]
sp <- scanP(x=x, y=y, P=seq(-.01,.5,length.out=200), plot=TRUE)

p <- estimateP(x=x, y=y, plot=FALSE)
segs <- dpseg(data$time, data$price, jumps=FALSE, P=p, type='var', store.matrix=TRUE, verb=FALSE)
slope_last <- segs$segments$slope[length(segs$segments$slope)]
