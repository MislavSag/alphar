library(data.table)
library(httr)
library(leanr)
library(QuantTools)
library(ggplot2)
library(TTR)
library(PerformanceAnalytics)
library(equityData)
library(AzureStor)
library(future.apply)
library(patchwork)


# set up
ENDPOINT = storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
CONT = storage_container(ENDPOINT, "equity-usa-hour-fmpcloud-adjusted")

# PARAMETERS
frequency = "hour"
windows = c(8 * 22, 8 * 22 * 3, 8 * 22 * 6,  8 * 22 * 12, 8 * 22 * 12 * 2, 8 * 22 * 12 * 5)

# import data
azure_blobs <- list_blobs(CONT)
market_data_list <- lapply(azure_blobs$name, function(x) {
  print(x)
  y <- tryCatch(storage_read_csv2(CONT, x), error = function(e) NA)
  if (is.null(y) | all(is.na(y))) return(NULL)
  y <- cbind(symbol = x, y)
  return(y)
})
market_data <- rbindlist(market_data_list)
market_data[, symbol := toupper(gsub("\\.csv", "", symbol))]
keep_symbols <- market_data[, .N, by = symbol][N > 600, symbol]
market_data <- market_data[symbol %in% keep_symbols]
market_data[, N := 0]
setorderv(market_data, c('symbol', 'datetime'))
market_data[, returns := close / shift(close) - 1, by = "symbol"]
market_data <- na.omit(market_data)
market_data$datetime <- as.POSIXct(as.numeric(market_data$datetime),
                                   origin=as.POSIXct("1970-01-01", tz="EST"),
                                   tz="EST")
spy <- market_data[symbol == "SPY", .(datetime, close, returns)]
symbols <- unique(market_data$symbol)
close_data <- market_data[, .(symbol, datetime, close)]
