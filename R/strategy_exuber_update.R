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
data_freq <- "hour"

# import data
if (data_freq == "hour") {
  market_data <- import_lean("D:/market_data/equity/usa/hour/trades_adjusted")
  spy <- import_lean("D:/market_data/equity/usa/hour/trades_adjusted", 'spy')
  risk_path <- file.path("D:/risks/radf_hour")
}

# define sample
sp500_stocks <- market_data[, .(symbol, datetime, close)]
sp500_stocks <- sp500_stocks[sp500_stocks[, .N, by = .(symbol)][N > 100], on = "symbol"]

# calculate radf for all stocks
directories <- list.files(risk_path)
for (dirs in directories) {
  # dirs <- directories[3]
  print(dirs)
  dir_ <- file.path("D:/risks/radf_hour", dirs)
  dir_files <- list.files(dir_)
  symbols <- gsub("\\.csv", "", dir_files)
  params_ <- stringr::str_split(gsub(".*/", "", dirs), "-")[[1]]

  lapply(symbols, function(x) {
    # x = symbols[3]
    print(x)

    if (x %in% symbols) {
      radf_old <- fread(paste0(dir_, "/", x, ".csv"))
      attributes(radf_old$datetime)
      sample <- sp500_stocks[symbol == x]
      attributes(sample$datetime)$tzone <- "UTC"
      row_index <- which(sample$datetime == tail(radf_old$datetime, 1)) - as.integer(params_[2])
      sample <- sample[row_index:nrow(sample)]

      # function(price, use_log, window, price_lags, no_cores = 4L)
      estimated <- roll_radf(as.data.frame(sample$close),
                             as.logical(as.integer(params_[1])),
                             as.integer(params_[2]),
                             as.integer(params_[3]))
      estimated_appened <- cbind(symbol = x, datetime = sample$datetime, estimated)
      estimated_appened <- na.omit(estimated_appened)
      estimated_appened <- estimated_appened[!(datetime %in% radf_old$datetime)]
      estimated_new <- rbind(radf_old, estimated_appened)
      estimated_new <- estimated_new[!duplicated(estimated_new$datetime)]
      fwrite(estimated_new, paste0("D:/risks/radf_hour/", dirs, "/", x, '.csv'))

    }
  })
}
