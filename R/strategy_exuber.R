library(data.table)
library(exuber)
library(PerformanceAnalytics)
library(ggplot2)
library(runner)
library(future.apply)
library(parallel)
library(fmpcloudr)
library(leanr)
library(mrisk)


# set api token
APIKEY = "15cd5d0adf4bc6805a724b4417bbaafc"
fmpc_set_token(APIKEY)

# parameters
data_freq <- "minute"
data_freq_multiply <- 15
use_log <- c(TRUE)
windows <- c(100)
lags <- c(1, 2, 4, 8)
params <- expand.grid(use_log, windows, lags)
colnames(params) <- c("lag", "window", "lag")

# import data
if (data_freq == "hour") {
  market_data <- import_lean("D:/market_data/equity/usa/hour/trades_adjusted")
  spy <- import_lean("D:/market_data/equity/usa/hour/trades_adjusted", 'spy')
  risk_path <- file.path("D:/risks/radf_hour")

} else if (data_freq == "minute") {
  market_data <- leanr::get_market_equities_minutes("D:/market_data/equity/usa/minute", "SPY")
  risk_path <- file.path("D:/risks/radf-minute")

  # define sample
  market_data[, time := format.POSIXct(market_data$datetime, "%H:%M:%S")]
  market_data <- market_data[time %between% c("09:30:00", "16:00:00")]

  # upsample
  if (data_freq_multiply == 15) {
    ohlcv <- xts::to.minutes15(as.xts.data.table(market_data[, .(datetime, open, high, low, close, volume)]))
    market_data <- cbind(symbol = market_data$symbol[1], as.data.table(ohlcv))
    setnames(market_data, c("symbol", "datetime", "open", "high", "low", "close", "volume"))
  }
}

# define sample
sp500_stocks <- market_data[, .(symbol, datetime, close)]
sp500_stocks <- sp500_stocks[sp500_stocks[, .N, by = .(symbol)][N > max(windows)], on = "symbol"]

# calculate radf for all stocks
directories <- paste0(risk_path, "/", apply(params, 1, function(x) paste0(x, collapse = "-")))
lapply(directories, function(x) { # create directories if they doesn't exists
  if (!dir.exists(x)) {
    print("Create directory")
    dir.create(x)
  }
})
for (dirs in directories) {
  # dirs <- directories[1]
  print(dirs)
  symbols <- unique(sp500_stocks$symbol)
  estimated <- gsub('\\.csv', '', list.files(dirs))
  symbols <- setdiff(symbols, estimated)

  lapply(symbols, function(x) {
    if (!(x %in% c("PLLL", "PETM"))) {
      print(x)
      sample <- sp500_stocks[symbol == x]
      params_ <- stringr::str_split(gsub(".*/", "", dirs), "-")[[1]]
      estimated <- roll_radf(as.data.frame(sample$close),
                             as.logical(as.integer(params_[1])),
                             as.integer(params_[2]),
                             as.integer(params_[3]))
      estimated_appened <- cbind(symbol = x, datetime = sample$datetime, estimated)
      fwrite(estimated_appened, paste0(dirs, "/", x, '.csv'))
    }
  })
}
