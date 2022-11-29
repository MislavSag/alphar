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
library(equityData)
library(AzureStor)



# UPDATE RAW EXUBER DATA --------------------------------------------------

# # import data
# data_freq <- "hour"
# if (data_freq == "hour") {
#   market_data <- import_lean("D:/market_data/equity/usa/hour/trades_adjusted")
#   spy <- import_lean("D:/market_data/equity/usa/hour/trades_adjusted", 'spy')
#   risk_path <- file.path("D:/risks/radf-hour")
# }
#
# # define sample
# sp500_stocks <- market_data[, .(symbol, datetime, close)]
# sp500_stocks <- sp500_stocks[sp500_stocks[, .N, by = .(symbol)][N > 600], on = "symbol"]


# get data from azure
ENDPOINT = storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
CONT = storage_container(ENDPOINT, "equity-usa-hour-fmpcloud-adjusted")
CONTMIN = storage_container(ENDPOINT, "equity-usa-minute-fmpcloud")
fmpcloudr::fmpc_set_token(Sys.getenv("APIKEY-FMPCLOUD"))
risk_path <- file.path("D:/risks/radf-hour")


# UPDATE RAW EXUBER DATA --------------------------------------------------

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
market_data[, returns := close / shift(close) - 1]
market_data <- na.omit(market_data)
market_data$datetime <- as.POSIXct(as.numeric(market_data$datetime),
                                   origin=as.POSIXct("1970-01-01", tz="EST"),
                                   tz="EST")
market_data <- market_data[, .(symbol, datetime, close, returns)]
market_data <- unique(market_data, by = c("symbol", "datetime"))
sp500_stocks <- market_data[market_data[, .N, by = .(symbol)][N >= 600], on = "symbol"]
setorderv(sp500_stocks, c("symbol", "datetime"))

# calculate radf for all stocks
directories <- list.files(risk_path)
directories <- directories[grep("-600-1", directories)]
directories <- directories[2]
for (dirs in directories) {
  # dirs <- directories[1]
  print(dirs)
  dir_ <- file.path(risk_path, dirs)
  dir_files <- list.files(dir_)
  symbols <- gsub("\\.csv", "", dir_files)
  params_ <- stringr::str_split(gsub(".*/", "", dirs), "-")[[1]]

  lapply(symbols, function(x) {
    # x = symbols[1]
    # print(x)

    if (x %in% symbols) {
      print(x)
      radf_old <- tryCatch(fread(paste0(dir_, "/", x, ".csv")), error = function(e) NA)
      if (all(is.na(radf_old))) {
        return(NULL)
      }
      attributes(radf_old$datetime)
      sample <- sp500_stocks[symbol == x]
      if (nrow(sample) == 0) {
        return(NULL)
      }

      attributes(sample$datetime)$tzone <- "UTC"
      row_index <- which(sample$datetime == tail(radf_old$datetime, 1)) - as.integer(params_[2])
      if  ((length(row_index) == 0 || row_index < 0)) { # & max(as.Date(sample$datetime)) >= tail(as.Date(radf_old$datetime), 1)
        sample <- sample
      } else {
        sample <- sample[row_index:nrow(sample)]
      }

      # function(price, use_log, window, price_lags, no_cores = 4L)
      estimated <- roll_radf(as.data.frame(sample$close),
                             as.logical(as.integer(params_[1])),
                             as.integer(params_[2]),
                             as.integer(params_[3]))
      if (length(estimated) == 0) return(NULL)
      estimated_appened <- cbind(symbol = x, datetime = sample$datetime, estimated)
      estimated_appened <- na.omit(estimated_appened)
      estimated_appened <- estimated_appened[!(datetime %in% radf_old$datetime)]
      estimated_new <- rbind(radf_old, estimated_appened)
      estimated_new <- estimated_new[!duplicated(estimated_new$datetime)]
      fwrite(estimated_new, paste0("D:/risks/radf-hour/", dirs, "/", x, '.csv'))
    }
  })
}



# UPDATE EXUBER AGGREGATE DATA --------------------------------------------

# exuber paths
exuber_path <- "D:/risks/radf-hour"
exuber_paths <- list.files(exuber_path, full.names = TRUE)
exuber_paths <- "D:/risks/radf-hour/1-600-1"
symbols_saved <- gsub('.csv', '', list.files(exuber_paths))

# price data
print("Get prices data")
# sp500_stocks <- import_lean("D:/market_data/equity/usa/hour/trades_adjusted")
# sp500_stocks[, returns := (close / shift(close)) - 1, by = .(symbol)]
spy <- sp500_stocks[symbol == "SPY", .(datetime, close, returns)]
sp500_stocks <- sp500_stocks[symbol %in% symbols_saved]
sp500_stocks[, cum_returns := frollsum(returns, 10 * 8), by = .(symbol)]
sp500_stocks[, cum_returns := shift(cum_returns, type = "lag"), by = .(symbol)]
sp500_stocks <- na.omit(sp500_stocks)
sp500_stocks <- sp500_stocks[, .(symbol, datetime, returns, cum_returns)]

# function for calculating exuber aggregate values based on file which contains radf values
exuber_agg <- function(path) {

  # import exuber data
  exuber_file <- list.files(path, full.names = TRUE)
  if (length(exuber_file) < 500) {
    print("number of files in the folder is lower than 500.")
    return(NULL)
  }
  exuber_dt <- lapply(exuber_file, function(x) {tryCatch(fread(x), error = function(e) NULL)})
  exuber_dt <- rbindlist(exuber_dt)[, id := gsub(".*/", "", path)]
  exuber_dt[, radf_sum := adf + sadf + gsadf + badf + bsadf]
  exuber_dt <- exuber_dt[, .(id, symbol, datetime, adf, sadf, gsadf, badf, bsadf, radf_sum)]

  # merge exuber and stocks
  exuber_dt <- merge(exuber_dt, sp500_stocks, by = c("symbol", "datetime"), all.x = TRUE, a.y = FALSE)
  setorderv(exuber_dt, c("id", "symbol", "datetime"))

  # REMOVE OLD STOCKS !
  # exuber_dt[, old_data := max(datetime) < as.Date(Sys.Date() - 10), by = "symbol"]
  # exuber_dt <- exuber_dt[, .SD[all(!old_data)], by = "symbol"]

  # define indicators based on exuber
  radf_vars <- colnames(exuber_dt)[4:ncol(exuber_dt)]
  indicators_median <- exuber_dt[, lapply(.SD, median, na.rm = TRUE), by = c('id', 'datetime'), .SDcols = radf_vars]
  colnames(indicators_median)[3:ncol(indicators_median)] <- paste0("median_", colnames(indicators_median)[3:ncol(indicators_median)])
  indicators_sd <- exuber_dt[, lapply(.SD, sd, na.rm = TRUE), by = c('id', 'datetime'), .SDcols = radf_vars]
  colnames(indicators_sd)[3:ncol(indicators_sd)] <- paste0("sd_", colnames(indicators_sd)[3:ncol(indicators_sd)])
  indicators_mean <- exuber_dt[, lapply(.SD, mean, na.rm = TRUE), by = c('id', 'datetime'), .SDcols = radf_vars]
  colnames(indicators_mean)[3:ncol(indicators_mean)] <- paste0("mean_", colnames(indicators_mean)[3:ncol(indicators_mean)])
  indicators_sum <- exuber_dt[, lapply(.SD, sum, na.rm = TRUE), by = c('id', 'datetime'), .SDcols = radf_vars]
  colnames(indicators_sum)[3:ncol(indicators_sum)] <- paste0("sum_", colnames(indicators_sum)[3:ncol(indicators_sum)])

  # merge indicators
  indicators <- merge(indicators_sd, indicators_median, by = c("id", "datetime"))
  indicators <- merge(indicators, indicators_mean, by = c("id", "datetime"))
  indicators <- merge(indicators, indicators_sum, by = c("id", "datetime"))
  setorderv(indicators, c("id", "datetime"))
  indicators <- na.omit(indicators)

  return(indicators)
}

# get exuber agg for all files
print("Calculate exuber indicators")
exuber_list <- lapply(exuber_paths, exuber_agg)
exuber_list <- exuber_list[lengths(exuber_list) != 0]
exuber <- rbindlist(exuber_list)
exuber_indicators <- exuber[spy, on = "datetime"]
exuber_indicators <- na.omit(exuber_indicators)
setorderv(exuber_indicators, c("id", "datetime"))
exuber_indicators <- exuber_indicators[id %like% "-600-"] # filter,, comment if want to uise all ids

# save to local path and dropbox
print("Save all files.")
data_save <- lapply(exuber_list, function(x) {
  attr(x$datetime, "tzone") <- "EST"
  x
})
lapply(data_save, function(x) fwrite(x[, 2:8], paste0("D:/risks/radf-indicators/", x$id[1], ".csv"), col.names = FALSE, dateTimeAs = "write.csv"))
print("Save to Azure blob storage.")
my_save_blob_files <- function(object, file_name, container = "qc-backtest") {
  blob_endpoint <- "https://contentiobatch.blob.core.windows.net/"
  blob_key <- "qdTsMJMGbnbQ5rK1mG/9R1fzfRnejKNIuOv3X3PzxoBqc1wwTxMyUuxNVSxNxEasCotuzHxwXECo79BLv71rPw=="
  bl_endp_key <- storage_endpoint(blob_endpoint, key=blob_key)
  cont <- storage_container(bl_endp_key, container)
  if (grepl("csv", file_name)) {
    storage_write_csv2(object, cont, file = file_name)
  } else if (grepl("rds", file_name)) {
    storage_save_rds(object, cont, file = file_name)
  }
}
lapply(data_save, function(x) my_save_blob_files(x[, 2:8], paste0(x$id[1], ".csv"), "qc-backtest"))
