library(leanr)
library(mrisk)
library(data.table)
library(rdrop2)
library(stringr)


# parameters
data_freq <- "hour"
save_folder <- paste0("D:/risks/radf-spy-", data_freq)

# import data
if (data_freq == "hour") {
  market_data <- import_lean("D:/market_data/equity/usa/hour/trades_adjusted", "SPY")
}

# calculate radf for all stocks
file_names <- list.files(save_folder, full.names = TRUE)
for (f in file_names) {
  print(f)
  log <- as.logical(as.integer(gsub(".*/|-.*", "", f)))
  window <- as.integer(str_extract(f, "\\d{3}"))
  lags <- as.integer(gsub(".*-|\\.csv", "", f))
  estimated_history <- fread(f)
  attr(estimated_history$datetime, "tzone") <- "EST"
  spy_new <- cbind(market_data[!(datetime %in% estimated_history$datetime)])
  if (nrow(spy_new) == 0) {
    fwrite(estimated_history, f)
    print("No new data")
    next()
  }
  window_data <- market_data[datetime < spy_new$datetime[1], tail(.SD, window + 1)]
  final_data <- rbind(window_data, spy_new)
  estimated <- roll_radf(as.data.frame(final_data$close), log, window, lags)
  estimated_appened <- cbind(datetime = final_data$datetime, estimated)
  estimated_appened <- estimated_appened[estimated_appened$datetime %in% spy_new$datetime]
  estimated_all <- rbind(estimated_history, estimated_appened)
  estimated_all <- unique(estimated_all)
  fwrite(estimated_all, f)
}

# add saved files to dropbox
paths_to_add <- list.files(save_folder, full.names = TRUE)
files_ <- gsub(".*/|", "", paths_to_add)
lapply(paths_to_add, drop_upload)
lapply(files_, function(x) {
  tryCatch({
    drop_share(path = x,
               requested_visibility = "public",
               link_password = NULL,
               expires = NULL)
  }, error = function(e) print(e))
})
