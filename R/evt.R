library(data.table)
library(AzureStor)
library(ggplot2)
library(PerformanceAnalytics)
library(QuantTools)
require(finfeatures, lib.loc = "C:/Users/Mislav/Documents/GitHub/finfeatures/renv/library/R-4.1/x86_64-w64-mingw32")
# library(reticulate)
# # python packages
# reticulate::use_python("C:/ProgramData/Anaconda3/envs/mlfinlabenv/python.exe", required = TRUE)
# mlfinlab = reticulate::import("mlfinlab", convert = FALSE)
# pd = reticulate::import("pandas", convert = FALSE)
# builtins = import_builtins(convert = FALSE)
# main = import_main(convert = FALSE)



# SET UP ------------------------------------------------------------------

# get data from azure
ENDPOINT = storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
CONT = storage_container(ENDPOINT, "equity-usa-hour-fmpcloud-adjusted")
CONTMIN = storage_container(ENDPOINT, "equity-usa-minute-fmpcloud")
fmpcloudr::fmpc_set_token(Sys.getenv("APIKEY-FMPCLOUD"))


# IMPORT DATA -------------------------------------------------------------

# import all data from Azure storage
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
market_data[, returns := close / shift(close) - 1, by = .(symbol)]
market_data <- na.omit(market_data)
market_data$datetime <- as.POSIXct(as.numeric(market_data$datetime),
                                   origin=as.POSIXct("1970-01-01", tz="EST"),
                                   tz="EST")
market_data <- market_data[close > 1e-005 & open > 1e-005 & high > 1e-005 & low > 1e-005]
market_data <- unique(market_data, by = c("symbol", "datetime"))
market_data_n <- market_data[, .N, by = symbol]
market_data_n <- market_data_n[which(market_data_n$N > 8 * 5 * 22 * 12)]  # remove prices with only 60 or less observations
market_data <- market_data[symbol %in% market_data_n$symbol]



# IDENTIFY BIG JUMPS ------------------------------------------------------

#
market_data[, sd_flt := roll_sd_filter(x = returns, n = 8 * 5 * 22, k = 3, m = 5L), by = symbol]
table(market_data$sd_flt)
