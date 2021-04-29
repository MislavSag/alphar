library(data.table)
library(exuber)
library(PerformanceAnalytics)
library(ggplot2)
library(runner)
library(future.apply)
library(parallel)
library(fmpcloudr)
source('C:/Users/Mislav/Documents/GitHub/alphar/R/import_data.R')


# set api token
APIKEY = "15cd5d0adf4bc6805a724b4417bbaafc"
fmpc_set_token(APIKEY)

# parameters
data_freq <- "daily"

# import data
sp500_symbols <- import_sp500()
if (data_freq == "daily") {
  sp500_stocks <- import_daily("D:/market_data/equity/usa/day/trades", "csv")
  sp500_stocks <- sp500_stocks[, .(symbol, date, adjClose)]
  setnames(sp500_stocks, c("date", "adjClose"), c("datetime", "close"))
  sp500_stocks <- sp500_stocks[datetime %between% c(as.Date("2000-01-01"), Sys.Date())]
  spy <- fmpcloudr::fmpc_price_history("SPY", startDate = "2000-01-01")
  spy <- as.data.table(spy)
  spy <- spy[, .(symbol, date, adjClose)]
  setnames(spy, c("date", "adjClose"), c("datetime", "close"))
} else {
  sp500_stocks <- import_intraday("D:/market_data/equity/usa/hour/trades_adjusted", "csv")

}
sp500_stocks <- sp500_stocks[symbol %in% c(sp500_symbols)]


# calculate returns and exctract SPY
sp500_stocks[, returns := (close / shift(close)) - 1, by = .(symbol)]
sp500_stocks <- na.omit(sp500_stocks)
spy <- fmpcloudr::fmpc_price_history("SPY", startDate = "2000-01-01")
sp500_stocks <- sp500_stocks[symbol != "SPY"]
sp500_stocks <- sp500_stocks[, .(symbol, datetime, close, returns)]

# remove outliers
sp500_stocks <- sp500_stocks[abs(returns) < 0.48]

# prepare data
sp500_stocks <- sp500_stocks[sp500_stocks[, .N, by = .(symbol)][N > 1000], on = "symbol"]

# parameters
use_log <- c(TRUE)
windows <- c(100, 200, 400)
lags <- c(1, 2)
params <- expand.grid(use_log, windows, lags)
colnames(params) <- c("lag", "window", "lag")

# radf roll SPY
estimate_radf <- function(close, use_log, window, lag_) {

  if (use_log) {
    close <- log(close)
  }

  cl <- makeCluster(16L)
  clusterExport(cl, c("close", "use_log", "window", "lag_", "params"), envir = environment())
  roll_radf <- runner(
    x = as.data.frame(close),
    f = function(x) {
      library(exuber)
      library(data.table)
      y <- exuber::radf(x, lag = lag_)
      stats <- exuber::tidy(y)
      bsadf <- data.table::last(exuber::augment(y))[, 4:5]
      y <- cbind(stats, bsadf)
      return(y)
    },
    k = window,
    na_pad = TRUE,
    cl = cl
  )
  stopCluster(cl)
  roll_radf <- lapply(roll_radf, data.table::as.data.table)
  roll_radf <- data.table::rbindlist(roll_radf, fill = TRUE)[, `:=`(V1 = NULL, id = NULL)]
  return(roll_radf)
}

# test function
x <- estimate_radf(spy$close[1:600], use_log = TRUE, window = 500, lag = 0L)

# radf roll params
estimate_radf_params <- function(date_close, params) {
  estimates <- lapply(1:nrow(params), function(i) {
    y <- estimate_radf(date_close, use_log = params[i, 1], window = params[i, 2], lag_ = params[i, 3])
    colnames(y) <- paste(colnames(y), params[i, 1], params[i, 2], params[i, 3], sep = "_")
    y
  })
  radf_indicators <- do.call(cbind, estimates)
  return(radf_indicators)
}

# calculate for SPY
# cols <- apply(expand.grid(c('adf', 'sadf', 'gsadf', 'badf', 'bsadf'), apply(params, 1, paste0, collapse = '_')), 1, paste0, collapse = '_')
symbols <- unique(sp500_stocks$symbol)
plan(multiprocess(workers = 16L))
options(future.globals.maxSize= 850*1024^2) # 850 Mb
sp <- sp500_stocks[, .(symbol, datetime, close)]
estimated <- gsub('\\.csv', '', list.files('D:/risks/radf_daily'))
symbols <- setdiff(symbols, estimated)
lapply(symbols, function(x) {
  if (x %in% c("AMCR", 'CVG', "SII", "TSG", "VNT")) {
    return(NULL)
  } else {
    sample <- sp[symbol == x]
    estimated <- estimate_radf(as.data.frame(sample$close), TRUE, 200, 1L)
    estimated <- cbind(symbol = x, datetime = sample$datetime, estimated)
    fwrite(estimated, paste0('D:/risks/radf_daily/', x, '.csv'))
  }
})


# analyse SPY
# cols_data_plot <- c('symbol', 'datetime', colnames(test)[grep("bsadf", colnames(test))])
# data_plot <- test[, ..cols_data_plot]
# data_plot <- na.omit(data_plot)
# data_plot <- melt(data_plot, id.vars = c("symbol", "datetime"))
# data_plot$variable <- paste(data_plot$symbol, data_plot$variable, sep = "_")
# ggplot(data_plot,
#        aes(x = datetime, y = value, color = variable)) +
#   geom_line()
# ggplot(data_plot[datetime %between% c("2020-03-01", "2020-10-01")],
#        aes(x = datetime, y = value, color = variable)) +
#   geom_line()
