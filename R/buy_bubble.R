library(data.table)
library(AzureStor)
library(ggplot2)


# set up
ENDPOINT = storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
CONT = storage_container(ENDPOINT, "equity-usa-hour-fmpcloud-adjusted")
CONTMIN = storage_container(ENDPOINT, "equity-usa-minute-fmpcloud")
fmpcloudr::fmpc_set_token(Sys.getenv("APIKEY-FMPCLOUD"))
radf_path <- "D:/risks/radfagg-live"
APIKEY = Sys.getenv("APIKEY-FMPCLOUD")

# import and prepare market data
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
market_data[, returns := close / shift(close) - 1]
market_data <- na.omit(market_data)
market_data$datetime <- as.POSIXct(as.numeric(market_data$datetime),
                                   origin=as.POSIXct("1970-01-01", tz="EST"),
                                   tz="EST")
market_data <- market_data[format(datetime, "%H:%M:%S") %between% c("10:00:00", "16:01:00")]
# market_data <- market_data[, .(symbol, datetime, close, returns)]
market_data <- unique(market_data, by = c("symbol", "datetime"))


list.files('D:/risks/radf-indicators', full.names = TRUE)
exuber_paths <- list.files("D:/risks/radf-hour/1-600-1", full.names = TRUE)
exuber_history <- lapply(exuber_paths, function(x){
  tryCatch(fread(x), error = function(e) NULL)
})
exuber_history <- rbindlist(exuber_history)
attr(exuber_history$datetime, "tzone") <- "EST"
exuber_history <- exuber_history[format(datetime, "%H:%M:%S") %between% c("10:00:00", "16:01:00")]

# merge exuber and market data
market_data <- merge(market_data, exuber_history, by = c("symbol", "datetime"), all.x = TRUE, all.y = FALSE)
market_data <- na.omit(market_data)
setorderv(market_data, c("symbol", "datetime"))

# backtest individual
symbol = "AAPL"
sample_ <- market_data[symbol == "AAPL"]
sample_[, signal := shift(bsadf > 0.5, n = 1L, type = "lag")]
sample_[signal == TRUE]
sample_[, beta := QuantTools::roll_lm(close, 1:nrow(sample_), n = 8 * 5)$beta]

# date segments
GFC <- c("2007-01-01", "2010-01-01")
AFTERGFCBULL <- c("2010-01-01", "2015-01-01")
COVID <- c("2020-01-01", "2021-06-01")
AFTER_COVID <- c("2021-06-01", "2022-01-01")
NEW <- c("2022-01-01", "2022-03-15")

dates_buy <- sample_[signal == TRUE & beta > 1, datetime]
ggplot(sample_, aes(x = datetime)) +
  geom_line(aes(y = close)) +
  geom_vline(xintercept = dates_buy, color = "green")
ggplot(sample_[datetime %between% GFC], aes(x = datetime)) +
  geom_line(aes(y = close)) +
  geom_vline(xintercept = dates_buy, color = "green")
ggplot(sample_[datetime %between% AFTERGFCBULL], aes(x = datetime)) +
  geom_line(aes(y = close)) +
  geom_vline(xintercept = dates_buy, color = "green")

# backtest
backtest <- function(returns, indicator, threshold, return_cumulative = TRUE) {
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1])) {
      sides[i] <- NA
    } else if (indicator[i-1] > threshold) { #  & indicator_2[i-1] > 1
      sides[i] <- 1
    } else {
      sides[i] <- 0
    }
  }
  sides <- ifelse(is.na(sides), 1, sides)
  returns_strategy <- returns * sides
  if (return_cumulative) {
    return(PerformanceAnalytics::Return.cumulative(returns_strategy))
  } else {
    return(returns_strategy)
  }
}
