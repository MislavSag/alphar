library(httr)
library(QuantTools)
library(highfrequency)
library(equityData)
library(future.apply)
library(mlfinance)
library(lubridate)



# parameters
symbols = c("AAPL", "TSLA")
frequency_in_minutes <- "hour" # hour minute 5 mins

# get NY current time
get_ny_time <- function() {
  s <- Sys.time()
  s <- .POSIXct(s, "America/New_York")
  return(s)
}

# import mlr3 model
list.files("D:/mlfin/mlr3_models")
model <- readRDS(file.path("D:/mlfin/mlr3_models", "automl-hft-20211007-232554.rds"))
best_model <- model$learner
feature_names <- model$task$feature_names

# get and save most recent data
get_ticker_data(symbols)

# import recent history data
tick_data <- future_lapply(symbols, function(s) {
  files_import <- list.files(file.path("D:/tick_data", s), full.names = TRUE)
  files_import_dates <- as.Date(substr(basename(files_import), 1, 8), format = "%Y%m%d")
  recent_dates <- head(sort(files_import_dates, decreasing = TRUE), 2)
  file_index <- which(files_import_dates %in% recent_dates)
  recent_files <- files_import[file_index]
  tick_data <- lapply(recent_files, function(x) fread(cmd = paste0('unzip -p ', x), col.names = c('DT', "PRICE", "SIZE")))
  tick_data <- rbindlist(tick_data)
  tick_data[, SYMBOL := s]
  tick_data
})
tick_data <- rbindlist(tick_data)
setnames(tick_data, c("DT", "PRICE", "SIZE", "SYMBOL"))
tick_data <- tradesCleanup(tDataRaw = tick_data)
tick_data <- tick_data$tData
tbars <- lapply(symbols, function(s) aggregateTrades(tick_data[SYMBOL == s], alignBy = "minutes", alignPeriod = 1))
tbars <- rbindlist(tbars)

# filter bars
# data_ <- tbars[, .(SYMBOL, DT, VWPRICE)]
# setnames(data_, c("symbol", "date", "close"))
# backcusum_events_live <- rolling_backcusum(data_, windows = c(60 * 4), workers = 8L)
# backcusum_column <- "backcusum_rejections_1_240" # 0.1 0.05 0.01 0.001
# filtered_events_live <- backcusum_events_live[get(backcusum_column) == 1, .SD, .SDcols = c("symbol", "date")]


# set closing time
closing_time <- as.POSIXct(paste0(Sys.Date(), "18:00:00"), tz = "America/New_York")
open_time <- as.POSIXct(paste0(Sys.Date(), "08:30:00"), tz = "America/New_York")

# main function which calculates exuber aggregate indicator
s <- get_ny_time()
next_time_point <- ceiling_date(s, frequency_in_minutes)

# main loop
repeat {
  s <- get_ny_time()
  print(s)
  if (s >= next_time_point) {

    # get most recent tickdata and convert to minute data
    symbol = "AAPL"
    tick_data_new <- get_finam_data(symbol, Sys.Date(), Sys.Date(), period = "tick")

    # merge recent data and old data
    tick_data_old <- tick_data[symbol == symbol]
    tick_data_s <- rbind(tick_data_old, tick_data_new)





    # send mail notification if main indicator is above threshold (we have to sell)
    if (std_exuber$radf_sma_8 > 3) {
      send_email(message = "Exuber Agg is Above Threshold",
                 sender = "mislav.sagovac@contentio.biz",
                 recipients = c("mislav.sagovac@contentio.biz"))
    }

    # save result to file locally and to M-files
    file_name_date <- file.path(radf_path, paste0("radfagg-", Sys.Date(), ".csv"))
    file_name <- file.path(radf_path, paste0("radfagg.csv"))
    file_name_one <- file.path(radf_path, paste0("radfagg_one.csv"))
    if (!file.exists(file_name)) {

      # save locally
      fwrite(std_exuber, file_name)
      fwrite(std_exuber, file_name_date)

      # upload file to dropbox if it doesnt'exists
      drop_upload(file = file_name, path = "exuber")

    } else {

      # prepare data for dropbox
      fwrite(tail(std_exuber, 1), file_name, col.names = FALSE)

      # update dropbox
      drop_upload(file = file_name, path = "exuber")
    }
  }
  if (s > closing_time) {
    print("Market is closed")
    break
  }
  Sys.sleep(5L)
  next_time_point <- ceiling_date(s, frequency_in_minutes)
}

spy <- get_daily_prices("SPY", start_date = as.Date("2000-01-01"), end_date = Sys.Date(), blob_file = NA)
write.csv(spy, "C:/Users/Mislav/Documents/spy_fmpcloud.csv")
