library(leanr)
library(mrisk)
library(data.table)
library(rdrop2)


# parameters
data_freq <- "hour"
save_folder <- paste0("D:/risks/radf-spy-", data_freq)
use_log <- c(FALSE, TRUE)
windows <- c(100, 200, 400, 600)
lags <- c(1, 2, 5, 10)
params <- expand.grid(use_log, windows, lags)
colnames(params) <- c("log", "window", "lag")

# import data
if (data_freq == "hour") {
  market_data <- import_lean("D:/market_data/equity/usa/hour/trades_adjusted", "SPY")
  market_data <- market_data[!duplicated(datetime)]
}

# calculate radf for all stocks
file_names <- apply(params, 1, function(x) paste0(x, collapse = "-"))
file_names <- setdiff(file_names, gsub(".csv", "", list.files(save_folder)))
for (f in file_names) {
  print(f)
  params_ <- strsplit(gsub(".*/", "", f), "-")[[1]]
  estimated <- roll_radf(as.data.frame(market_data$close),
                         as.logical(as.integer(params_[1])),
                         as.integer(params_[2]),
                         as.integer(params_[3]))
  estimated_appened <- cbind(datetime = market_data$datetime, estimated)
  estimated_appened <- na.omit(estimated_appened)
  fwrite(estimated_appened, file.path(save_folder, paste0(f, '.csv')), dateTimeAs = 'write.csv')
}

# add saved files to dropbox
paths_to_add <- list.files(save_folder, full.names = TRUE)
lapply(paths_to_add, drop_upload)
lapply(files_, function(x) {
  tryCatch({
    drop_share(path = x,
               requested_visibility = "public",
               link_password = NULL,
               expires = NULL)
  }, error = function(e) print(e))
})
