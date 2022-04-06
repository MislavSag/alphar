library(data.table)
library(checkmate)
library(AzureStor)
library(httr)
library(fmpcloudr)
library(pins)
library(mlr3verse)
library(mlr3torch)
library(finfeatures)
library(ggplot2)
library(gausscov)
library(DescTools)
library(fredr)
library(future.apply)
library(reticulate)
# Python environment and python modules
# Instructions: some functions use python modules. Steps to use python include:
# 1. create new conda environment:
#    https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html
#    Choose python 3.8. for example:
#    conda create -n mlfinlabenv python=3.8
# 2. Install following packages inside environments
#    mlfinlab
#    tsfresh
#    TSFEL
reticulate::use_python("C:/ProgramData/Anaconda3/envs/mlfinlabenv/python.exe", required = TRUE) # check the path on yout system!
pd <- reticulate::import("pandas", convert = FALSE) # import pandas
mlfinlab <- reticulate::import("mlfinlab", convert = FALSE) # import mlfinlab


# SET UP ------------------------------------------------------------------
# checks
assert_choice("BLOB-ENDPOINT", names(Sys.getenv()))
assert_choice("BLOB-KEY", names(Sys.getenv()))
assert_choice("APIKEY-FMPCLOUD", names(Sys.getenv()))

# global vars
ENDPOINT = storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
CONT = storage_container(ENDPOINT, "fmpcloud")
CONTINVESTINGCOM = storage_container(ENDPOINT, "investingcom")
fmpc_set_token(Sys.getenv("APIKEY-FMPCLOUD"))
CACHEDIR = "D:/findata" # here define your local folder wher data will be saved
fred_api_key <- "fb7e8cbac4b84762980f507906176c3c"
fredr_set_key(fred_api_key)

# parameters
strategy = "PEAD"  # can be PEAD (for predicting post announcement drift) or PRE (for predicting pre announcement)
use_fundamental_data = FALSE  # should we use fundamental data as features?
start_holdout_date = as.Date("2021-01-01") # observations after this date belongs to holdout set


# EVENTS ------------------------------------------------------------------
# get events data
events <- as.data.table(storage_read_csv(CONT, "EarningAnnouncements.csv")) # PINS !
events <- na.omit(events, cols = c("eps", "epsEstimated")) # remove rows with NA for earnings
events <- events[date < Sys.Date()] # remove announcements for today

# keep only usa stocks
url <- modify_url("https://financialmodelingprep.com/", path = "api/v3/available-traded/list",
                  query = list(apikey = Sys.getenv("APIKEY-FMPCLOUD") ))
stocks <- rbindlist(content(GET(url)))
filter_symbols <- stocks[exchange %in% c("AMEX", "New York Stock Exchange Arca",
                                         "New York Stock Exchange", "NasdaqGS",
                                         "Nasdaq", "NASDAQ", "AMEX", "NYSEArca",
                                         "NASDAQ Global Market", "Nasdaq Global Market",
                                         "NYSE American", "Nasdaq Capital Market",
                                         "Nasdaq Global Select")]
events <- events[symbol %in% filter_symbols$symbo]

# investing.com data
investingcom_ea <- as.data.table(storage_read_csv(CONTINVESTINGCOM, "EarningAnnouncementsInvestingCom.csv"))
investingcom_ea <- na.omit(investingcom_ea, cols = c("eps", "eps_forecast"))
investingcom_ea <- investingcom_ea[, .(symbol, datetime, eps, eps_forecast, revenue, revenue_forecast, right_time)]
investingcom_ea[, date_investingcom := as.Date(datetime)]
setnames(investingcom_ea, colnames(investingcom_ea)[2:6], paste0(colnames(investingcom_ea)[2:6], "_investingcom"))

# merge DT and investing com earnings surprises
events <- merge(events, investingcom_ea,
                by.x = c("symbol", "date"),
                by.y = c("symbol", "date_investingcom"),
                all.x = TRUE, all.y = FALSE)
events <- events[date == as.Date(datetime_investingcom)] # keep only observations available in both datasets

# replace FMP cloud data with investing.com data if FMP CLOUD data doesn't exists
events[, eps := ifelse(is.na(eps), eps_investingcom, eps)]
events[, epsEstimated := ifelse(is.na(epsEstimated), eps_forecast_investingcom, epsEstimated)]
events[, revenue := ifelse(is.na(revenue), revenue_investingcom, revenue)]
events[, revenueEstimated := ifelse(is.na(revenueEstimated), revenue_forecast_investingcom, revenueEstimated)]

# check if time are the same
events[, right_time := ifelse(is.na(right_time), NA, ifelse(right_time == "marketClosed ", "amc", "bmc"))]
events[, time_dummy := time == right_time]

# if both fmp cloud and investing.com data exists keep similar
print(paste0("Number of removed observations because of investing.com / FMP cloud disbalance is :",
             nrow(events[abs(eps - eps_investingcom) > 0.04])))
events <- events[abs(eps - eps_investingcom) < 0.04] # keep only observations where earnings are very similar

# remove duplicated events
events <- unique(events, by = c("symbol", "date"))


# MARKET DATA -------------------------------------------------------------
# import market data from blob. This takes long time for the firs time
board <- board_azure(
  container = storage_container(ENDPOINT, "fmpcloud-daily"),
  path = "",
  n_processes = 6L,
  versioned = FALSE,
  cache = CACHEDIR
)
files_ <- pin_list(board)
files_ <- setdiff(files_, list.files(CACHEDIR))
lapply(files_, pin_download, board = board)
files_ <- pin_list(board)
files_ <- lapply(file.path(CACHEDIR, files_), list.files, recursive = TRUE, pattern = "\\.csv", full.names = TRUE)
files_ <- unlist(files_)
s <- Sys.time()
prices_dt <- lapply(files_, fread)
e <- Sys.time()
e - s
prices <- prices_dt[vapply(prices_dt, function(x) nrow(x) > 0, FUN.VALUE = logical(1))]
prices <- rbindlist(prices)

# cleand daily prices
prices <- prices[symbol %in% unique(events$symbol)] # keep only symbols available in events
prices[, date := as.Date(date)]
prices <- prices[open > 0 & high > 0 & low > 0 & close > 0 & adjClose > 0] # remove rows with zero and negative prices
setorder(prices, "symbol", "date")
prices[, returns := adjClose   / data.table::shift(adjClose) - 1, by = symbol] # calculate returns
prices <- prices[returns < 1] # TODO:: better outlier detection mechanism. For now, remove daily returns above 100%
adjust_cols <- c("open", "high", "low")
prices[, (adjust_cols) := lapply(.SD, function(x) x * (adjClose / close)), .SDcols = adjust_cols] # adjust open, high and low prices
prices[, close := adjClose]
prices <- na.omit(prices[, .(symbol, date, open, high, low, close, volume, returns)])
prices_n <- prices[, .N, by = symbol]
prices_n <- prices_n[which(prices_n$N > 700)]  # remove prices with only 700 or less observations
prices <- prices[symbol %in% prices_n$symbol]
prices <- prices[date %between% c(min(events$date) - 1100, max(prices$date, na.rm = TRUE))] # keep only data needed for events
prices <- unique(prices, by = c("symbol", "date")) # remove duplicates if they exists



# LABELING ----------------------------------------------------------------
# calculate returns
prices[, ret_22 := shift(close, -21L, "shift") / shift(close, -1L, "shift") - 1, by = "symbol"]
prices[, ret_44 := shift(close, -43L, "shift") / shift(close, -1L, "shift") - 1, by = "symbol"]
prices[, ret_66 := shift(close, -65L, "shift") / shift(close, -1L, "shift") - 1, by = "symbol"]

# calculate rolling sd
prices[, sd_22 := roll::roll_sd(close / shift(close, 1L) - 1, 22), by = "symbol"]
prices[, sd_44 := roll::roll_sd(close / shift(close, 1L) - 1, 44), by = "symbol"]
prices[, sd_66 := roll::roll_sd(close / shift(close, 1L) - 1, 66), by = "symbol"]

# calculate spy returns
spy <- as.data.table(fmpcloudr::fmpc_price_history("SPY", min(prices$date) - 5, Sys.Date()))
spy[, ret_22_spy := shift(adjClose, -21L, "shift") / shift(adjClose, -1L, "shift") - 1, by = "symbol"]
spy[, ret_44_spy := shift(adjClose, -43L, "shift") / shift(adjClose, -1L, "shift") - 1, by = "symbol"]
spy[, ret_66_spy := shift(adjClose, -65L, "shift") / shift(adjClose, -1L, "shift") - 1, by = "symbol"]

# calculate excess returns
prices <- merge(prices, spy[, .(date, ret_22_spy, ret_44_spy, ret_66_spy)], by = "date", all.x = TRUE, all.y = FALSE)
prices[, ret_22_excess := ret_22 - ret_22_spy]
prices[, ret_44_excess := ret_44 - ret_44_spy]
prices[, ret_66_excess := ret_66 - ret_66_spy]
prices[, `:=`(ret_22_spy = NULL, ret_44_spy = NULL, ret_66_spy = NULL)]
setorder(prices, symbol, date)

# calculate standardized returns
prices[, ret_excess_stand_22 := ret_22_excess / shift(sd_22, -21L), by = "symbol"]
prices[, ret_excess_stand_44 := ret_44_excess / shift(sd_44, -43L), by = "symbol"]
prices[, ret_excess_stand_66 := ret_66_excess / shift(sd_66, -65L), by = "symbol"]

# remove unnecesary coluns
prices[, `:=`(ret_22 = NULL, ret_44 = NULL, ret_66 = NULL,
              sd_22 = NULL, sd_44 = NULL, sd_66 = NULL,
              ret_22_excess = NULL, ret_44_excess = NULL, ret_66_excess = NULL)]

# labels for PRE announcement drift
# prices_dt[, close_open_return := close / open - 1, by = "symbol"]
# prices_dt[, open_close_return := open / shift(close) - 1, by = "symbol"]




# ADD FEATURES (LATER THIS GOES TO FINFEATURES) ---------------------------
# ath divergence
# prices[, ath_month_div := (shift(frollapply(high, 22, max), 1L) / close) - 1]
# prices[, ath_week_div := (shift(frollapply(high, 5, max), 1L) / close) - 1]


# MERGE MARKET DATA, EVENTS AND LABELS ------------------------------------
# merge clf_data and labels
dataset <- merge(events,
                 prices[, .(symbol, date, ret_excess_stand_22, ret_excess_stand_44, ret_excess_stand_66)],
                 by = c("symbol", "date"), all.x = TRUE, all.y = FALSE)

# extreme labeling
possible_target_vars <- c("ret_excess_stand_22", "ret_excess_stand_44", "ret_excess_stand_66")
bin_extreme_col_names <- paste0("bin_extreme_", possible_target_vars)
dataset[, (bin_extreme_col_names) := lapply(.SD, function(x) {
  y <- cut(x,
           quantile(x, probs = c(0, 0.2, 0.8, 1), na.rm = TRUE),
           labels = c(-1, NA, 1),
           include.lowest = TRUE)
  as.factor(droplevels(y))
}), .SDcols = possible_target_vars]
# around zero labeling
labeling_around_zero <- function(x) {
  x_abs <- abs(x)
  bin <- cut(x_abs, quantile(x_abs, probs = c(0, 0.3333), na.rm = TRUE), labels = 0L, include.lowest = TRUE)
  max_0 <- max(x[bin == 0], na.rm = TRUE)
  min_0 <- min(x[bin == 0], na.rm = TRUE)
  levels(bin) <- c(levels(bin), 1L, -1L)
  bin[x > max_0] <- as.character(1L)
  bin[x < min_0] <- as.factor(-1)
  return(bin)
}
bin_aroundzero_col_names <- paste0("bin_aroundzero_", possible_target_vars)
dataset[, (bin_aroundzero_col_names) := lapply(.SD, labeling_around_zero), .SDcols = possible_target_vars]

# sort dataset
setorderv(dataset, c("symbol", "date"))


# FEATURES ----------------------------------------------------------------
# prepare arguments for features
prices_events <- merge(prices, dataset, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE)
at_ <- which(!is.na(prices_events$eps))
# fwrite(prices_dt[, 1:7], "D:/temp/PEAD-testdata.csv") # FOR TET IN FINFEATURES
# fwrite(as.data.frame(at_), "D:/temp/PEAD-at.csv") # FOR TET IN FINFEATURES
OhlcvInstance = Ohlcv$new(prices_dt[, 1:7], date_col = "date")
if (strategy == "PEAD") {
  lag_ <- -1L
} else {
  lag_ <- 1L
  # ako je red u events amc. label je open_t+1 / close_t; lag je -1L
  # ako je red u events bmo. label je open_t / close_t-1; lag je -2L
}

# Features from OHLLCV
print("Calculate Ohlcv features.")
OhlcvFeaturesInit = OhlcvFeatures$new(windows = c(5, 10, 22, 22 * 3, 22 * 6, 22 * 12),
                                      quantile_divergence_window =  c(22, 22*3, 22*6, 22*12))
OhlcvFeaturesSet = OhlcvFeaturesInit$get_ohlcv_features(OhlcvInstance)
OhlcvFeaturesSetSample <- OhlcvFeaturesSet[at_ - lag_]
setorderv(OhlcvFeaturesSetSample, c("symbol", "date"))
############ DEBUG ##############
head(dataset[, .(symbol, date)])
head(OhlcvFeaturesSetSample[symbol == "AA", .(symbol, date)])
############ DEBUG ##############

# BidAsk features
print("Calculate BidAsk features.")
RollingBidAskInstance <- RollingBidAsk$new(windows = c(5, 22),
                                           workers = 8L,
                                           at = at_,
                                           lag = lag_,
                                           na_pad = TRUE,
                                           simplify = FALSE)
RollingBidAskFeatures = RollingBidAskInstance$get_rolling_features(OhlcvInstance)
############ DEBUG ##############
head(dataset[, .(symbol, date)])
head(RollingBidAskFeatures[symbol == "AA", .(symbol, date)])
############ DEBUG ##############

gc()
# BackCUSUM features
print("Calculate BackCUSUM features.")
RollingBackcusumInit = RollingBackcusum$new(windows = c(22 * 3),
                                            workers = 8L,
                                            at = at_,
                                            lag = lag_,
                                            na_pad = TRUE,
                                            simplify = FALSE)
RollingBackCusumFeatures = RollingBackcusumInit$get_rolling_features(OhlcvInstance)
gc()
# Exuber features
print("Calculate Exuber features.")
RollingExuberInit = RollingExuber$new(windows = c(300, 600),
                                      workers = 8L,
                                      at = at_,
                                      lag = lag_,
                                      na_pad = TRUE,
                                      simplify = FALSE,
                                      exuber_lag = 1L)
RollingExuberFeatures = RollingExuberInit$get_rolling_features(OhlcvInstance)
head(dataset[, .(symbol, date)])
head(RollingExuberFeatures[symbol == "A", .(symbol, date)])
gc()
# Forecast Features
print("Calculate AutoArima features.")
RollingForecatsInstance = RollingForecats$new(windows = c(100, 252),
                                              workers = 8L,
                                              lag = lag_,
                                              at = at_,
                                              na_pad = TRUE,
                                              simplify = FALSE,
                                              forecast_type = "autoarima",
                                              h = 22)
RollingForecatsAutoarimaFeatures = RollingForecatsInstance$get_rolling_features(OhlcvInstance)
print("Calculate Nnetar features.")
gc()
RollingForecatsInstance = RollingForecats$new(windows = c(200),
                                              workers = 8L,
                                              lag = lag_,
                                              at = at_,
                                              na_pad = TRUE,
                                              simplify = FALSE,
                                              forecast_type = "nnetar",
                                              h = 22)
RollingForecatNnetarFeatures = RollingForecatsInstance$get_rolling_features(OhlcvInstance)
gc()
# GAS
print("Calculate GAS features.")
RollingGasInit = RollingGas$new(windows = c(150),
                                workers = 8L,
                                at = at_,
                                lag = lag_,
                                na_pad = TRUE,
                                simplify = FALSE,
                                gas_dist = "sstd",
                                gas_scaling = "Identity",
                                prediction_horizont = 10)
RollingGasFeatures = RollingGasInit$get_rolling_features(OhlcvInstance)
gc()
# Gpd features
print("Calculate Gpd features.")
RollingGpdInit = RollingGpd$new(windows = c(22 * 3, 22 * 6),
                                workers = 8L,
                                at = at_,
                                lag = lag_,
                                na_pad = TRUE,
                                simplify = FALSE,
                                threshold = 0.05)
RollingGpdFeatures = RollingGpdInit$get_rolling_features(OhlcvInstance)
gc()
# theft catch22 features
print("Calculate Catch22 features.")
RollingTheftInit = RollingTheft$new(windows = c(5, 22, 22 * 3, 22 * 12),
                                    workers = 8L,
                                    at = at_,
                                    lag = lag_,
                                    na_pad = TRUE,
                                    simplify = FALSE,
                                    features_set = "catch22")
RollingTheftCatch22Features = RollingTheftInit$get_rolling_features(OhlcvInstance)
gc()
# theft feasts features
print("Calculate feasts features.")
RollingTheftInit = RollingTheft$new(windows = c(22 * 3, 22 * 12),
                                    workers = 8L,
                                    at = at_,
                                    lag = lag_,
                                    na_pad = TRUE,
                                    simplify = FALSE,
                                    features_set = "feasts")
RollingTheftFeastsFatures = RollingTheftInit$get_rolling_features(OhlcvInstance)
gc()
# theft tsfel features NO MESSAGES  !!!!
print("Calculate Tsfeatures features.")
RollingTheftInit = RollingTheft$new(windows = c(22 * 3, 22 * 12),
                                    workers = 1L,
                                    at = at_,
                                    lag = lag_,
                                    na_pad = TRUE,
                                    simplify = FALSE,
                                    features_set = "tsfel")
RollingTheftTsfelFeatures = suppressMessages(RollingTheftInit$get_rolling_features(OhlcvInstance))
gc()
# theft tsfeatures features
print("Calculate tsfeatures features.")
RollingTsfeaturesInit = RollingTsfeatures$new(windows = c(22 * 3, 22 * 6),
                                              workers = 8L,
                                              at = at_,
                                              lag = lag_,
                                              na_pad = TRUE,
                                              simplify = FALSE)
RollingTsfeaturesFeatures = RollingTsfeaturesInit$get_rolling_features(OhlcvInstance)
gc()


# merge all features test
cols_ohlcv <- c("symbol", "date", colnames(OhlcvFeaturesSetSample)[15:ncol(OhlcvFeaturesSetSample)])
features <- Reduce(function(x, y) merge(x, y, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE),
                   list(OhlcvFeaturesSetSample[, ..cols_ohlcv], RollingBidAskFeatures,
                        RollingBackCusumFeatures, RollingExuberFeatures,
                        RollingForecatsAutoarimaFeatures, RollingGasFeatures, RollingGpdFeatures,
                        RollingTheftCatch22Features, RollingTheftFeastsFatures, RollingTheftTsfelFeatures,
                        RollingTsfeaturesFeatures))

# merge OHLCV and events
features[, trading_date_after_event := date]
features <- features[dataset, on = c("symbol", "date"), roll = -Inf]
features[, .(symbol, date, trading_date_after_event)]
colnames(features)

# actual vs estimated
features[, `:=`(
  eps_diff = (eps - epsEstimated + 0.0001) / sd(eps - epsEstimated + 0.0001)
  # rev_diff = (revenue - revenueEstimated) / close
)]

# save features to Azure blob
setorderv(features, c("symbol", "date"))
# save_blob_files(features, paste0("nofund-", file_name), "features")

# fundamental data
reports <- fread("D:/fundamental_data/pl.csv")
reports[, `:=`(date = as.Date(date),
               fillingDate = as.Date(fillingDate),
               acceptedDateTime = as.POSIXct(acceptedDate, format = "%Y-%m-%d %H:%M:%S"),
               acceptedDate = as.Date(acceptedDate, format = "%Y-%m-%d %H:%M:%S"))]
fin_growth <- fread("D:/fundamental_data/fin_growth.csv")
fin_growth[, date := as.Date(date)]
fin_ratios <- fread("D:/fundamental_data/key_metrics.csv")
fin_ratios[, date := as.Date(date)]

# merge all fundamental data
fundamentals <- merge(reports, fin_growth, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE)
fundamentals <- merge(fundamentals, fin_ratios, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE)
fundamentals <- fundamentals[date > as.Date("1998-01-01")]
fundamentals[, acceptedDateFundamentals := acceptedDate]
data.table::setnames(fundamentals, "date", "fundamental_date")
fundamentals[, fundamental_acceptedDate := acceptedDate]
str(fundamentals)

# merge features and fundamental data
features <- merge(features, fundamentals, by.x = c("symbol", "date"),
                  by.y = c("symbol", "acceptedDate"), all.x = TRUE, all.y = FALSE)
features[symbol == "AAPL", .(symbol, fundamental_date, date, fundamental_acceptedDate)]

# mannually for naow
features$priceToSalesRatio <- as.numeric(features$priceToSalesRatio)
features$pfcfRatio <- as.numeric(features$pfcfRatio)
features$evToSales <- as.numeric(features$evToSales)
features$enterpriseValueOverEBITDA <- as.numeric(features$enterpriseValueOverEBITDA)
features$evToFreeCashFlow <- as.numeric(features$evToFreeCashFlow)
features$netDebtToEBITDA <- as.numeric(features$netDebtToEBITDA)
features[, grep("^lm_", colnames(features)) := NULL]
features[, grep("changes", colnames(features)) := NULL]

# save data with fundamentals
setorderv(features, c("symbol", "date"))
print("Saving features with fundamentals to blob.")
# fwrite(features, "D:/mlfin/mlr3_models/PEAD-features.csv")
# features <- fread("D:/mlfin/mlr3_models/PEAD-features.csv")
# colnames(features) <- gsub(" |-", "_", colnames(features))


# FEATURES SPACE ----------------------------------------------------------
# use fundamnetal ratios or not
if (use_fundamental_data) {
  features <- features[!is.na(epsgrowth)]
} else {
  keep_cols <- colnames(features)[1:which(colnames(features) == "eps_diff")]
  features <- features[, ..keep_cols]
  colnames(features) <- gsub("\\.x", "", colnames(features))
}

# features space from features raw
cols_remove <- c("trading_date_after_event", "time", "datetime_investingcom",
                 "eps_investingcom", "eps_forecast_investingcom", "revenue_investingcom",
                 "revenue_forecast_investingcom", "time_dummy",
                 "trading_date_after_event", "fundamental_date", "cik", "link", "finalLink",
                 "fillingDate", "calendarYear", "eps.y", "revenue.y", "period.x", "period.y",
                 "acceptedDateTime", "acceptedDateFundamentals", "reportedCurrency",
                 "fundamental_acceptedDate", "period", "right_time")
cols_non_features <- c("symbol", "date", "time", "right_time",
                       "ret_excess_stand_22", "ret_excess_stand_44", "ret_excess_stand_66",
                       colnames(features)[grep("aroundzero", colnames(features))],
                       colnames(features)[grep("extreme", colnames(features))])
cols_features <- setdiff(colnames(features), c(cols_remove, cols_non_features))
cols <- c(cols_non_features, cols_features)
features <- features[, .SD, .SDcols = cols]

# add fred data
get_fread_data <- function(id = "VIXCLS") {
  data_ <- fredr(series_id = id, observation_start = min(features$date), observation_end = Sys.Date())
  data_ <- as.data.table(data_)
  data_ <- data_[, .(date, value)]
  colnames(data_)[2] <- id
  return(data_)
}
vix <- get_fread_data("VIXCLS")
sp500 <- get_fread_data("SP500")
# t10 <- get_fread_data("T10Y2Y")
# ffr <- get_fread_data("DFF")
# oilprices <- get_fread_data("DCOILWTICO")
fread_features <- Reduce(function(x, y) merge(x, y, by = c("date"), all.x = TRUE, all.y = FALSE),
                         list(vix, sp500))
fread_features[, SP500_ret_month := SP500 / shift(SP500, 22) - 1]
fread_features[, SP500_ret_year := SP500 / shift(SP500, 252) - 1]
fread_features[, SP500_ret_week := SP500 / shift(SP500, 5) - 1]
fread_features[, SP500 := NULL]
features <- merge(features, fread_features, by = "date", all.x = TRUE, all.y = FALSE)



# CLEAN DATA --------------------------------------------------------------
# convert columns to numeric. This is important only if we import existing features
clf_data <- copy(features)
chr_to_num_cols <- colnames(clf_data[, ..cols_features][, .SD, .SDcols = is.character])
clf_data <- clf_data[, (chr_to_num_cols) := lapply(.SD, as.numeric), .SDcols = chr_to_num_cols]
int_to_num_cols <- colnames(clf_data[, ..cols_features][, .SD, .SDcols = is.integer])
clf_data <- clf_data[, (int_to_num_cols) := lapply(.SD, as.numeric), .SDcols = int_to_num_cols]

# remove columns with many Inf
# is.infinite.data.frame <- function(x) do.call(cbind, lapply(x, is.infinite))
keep_cols <- names(which(colMeans(!is.infinite(as.data.frame(clf_data))) > 0.9))
print(paste0("Removing columns with Inf values: ", setdiff(colnames(clf_data), keep_cols)))
clf_data <- clf_data[, .SD, .SDcols = keep_cols]

# remove rows with Inf values
clf_data <- clf_data[is.finite(rowSums(clf_data[, .SD, .SDcols = is.numeric], na.rm = TRUE))]
nrow(features)
nrow(clf_data)

# remove columns with lots of NA values
keep_cols <- names(which(colMeans(!is.na(features)) > 0.9))
keep_cols <- c(keep_cols, "bin_extreme_ret_excess_stand_22", "bin_extreme_ret_excess_stand_44", "bin_extreme_ret_excess_stand_66")
print(paste0("Removing columns with many NA values: ", setdiff(colnames(features), keep_cols)))
clf_data <- features[, .SD, .SDcols = keep_cols]

# convert logical to integer
chr_to_num_cols <- setdiff(colnames(clf_data[, .SD, .SDcols = is.character]), c("symbol", "time", "right_time"))
clf_data <- clf_data[, (chr_to_num_cols) := lapply(.SD, as.numeric), .SDcols = chr_to_num_cols]


# INSPECT FEATURES --------------------------------------------------------
# select features vector
feature_cols <- intersect(cols_features, colnames(clf_data))

# remove NA values
clf_data <- na.omit(clf_data, cols = feature_cols)

# remove constant columns
features_ <- clf_data[, ..feature_cols]
remove_cols <- colnames(features_)[apply(features_, 2, var, na.rm=TRUE) == 0]
print(paste0("Removing feature with 0 standard deviation: ", remove_cols))
feature_cols <- setdiff(feature_cols, remove_cols)

# remove highly correlated features
features_ <- clf_data[, ..feature_cols]
cor_matrix <- cor(features_)
cor_matrix_rm <- cor_matrix                  # Modify correlation matrix
cor_matrix_rm[upper.tri(cor_matrix_rm)] <- 0
diag(cor_matrix_rm) <- 0
remove_cols <- colnames(features_)[apply(cor_matrix_rm, 2, function(x) any(x > 0.98))]
print(paste0("Removing highly correlated featue (> 0.98): ", remove_cols))
feature_cols <- setdiff(feature_cols, remove_cols)

# TODO ADD THIS INSIDE MLR3 GRAPH
clf_data[, y := data.table::year(as.Date(date))]
cols <- c(feature_cols, "y")
clf_data[, (cols) := lapply(.SD, function(x) {Winsorize(as.numeric(x), probs = c(0.01, 0.99), na.rm = TRUE)}), by = "y", .SDcols = cols]
clf_data[, y := NULL]

# remove constant columns
features_ <- clf_data[, ..feature_cols]
remove_cols <- colnames(features_)[apply(features_, 2, var, na.rm=TRUE) == 0]
print(paste0("Removing feature with 0 standard deviation: ", remove_cols))
feature_cols <- setdiff(feature_cols, remove_cols)

# choose label
print(paste0("Choose among this features: ", colnames(clf_data)[grep("^ret_excess_stand", colnames(clf_data))]))
LABEL = "ret_excess_stand_22"


# FEATURE SELECTION -------------------------------------------------------

# gausscov features selection exmaple from package
# library(gausscov)
# data(boston)
# bostint <- fgeninter(boston[,1:13],2)[[1]]
# dim(bostint)
# a<-f1st(boston[,14],bostint,kmn=10,sub=TRUE)

# another criterion
# data(leukemia)
# covch=c(2.023725,1182,1219,2888,0)
# ind<-c(1182,1219,2888,0,0,0,0,0,0)
# ind<-matrix(ind,ncol=3)
# m<-3
# a<-f3sti(leukemia[[1]],leukemia[[2]],covch,ind,m)

# my try
cols_keep <- c(feature_cols, LABEL)
X <- clf_data[, ..cols_keep]
X <- na.omit(X)
X <- as.matrix(X)
# X <- model.matrix(ret_44_excess ~ .^2, data = X)

# f1st
f1st_fi <- f1st(X[, ncol(X)], X[, -ncol(X)], kmn = 20, sub = TRUE)
cov_index_f1st <- colnames(X[, -ncol(X)])[f1st_fi[[1]][, 1]]

# f3st_1
f3st_1 <- f3st(X[, ncol(X)], X[, -ncol(X)], m = 1)
cov_index_f3st_1 <- unique(as.integer(f3st_1[[1]][1, ]))[-1]
cov_index_f3st_1 <- cov_index_f3st_1[cov_index_f3st_1 != 0]
cov_index_f3st_1 <- colnames(X[, -ncol(X)])[cov_index_f3st_1]

# interesection of all important vars
most_important_vars <- intersect(cov_index_f1st, cov_index_f3st_1)
important_vars <- c(cov_index_f1st, cov_index_f3st_1)


# DEFINE TASKS ------------------------------------------------------------
# train/test and holdout set
holdout_ids <- which(clf_data$date > start_holdout_date)
X_model <- clf_data[-holdout_ids, ]
X_holdout <- clf_data[holdout_ids, ] # TODO SAVE THIS FOR QUANTCONNECT BACKTESTING

# select only labels and features
labels <- colnames(X_model)[grep(LABEL, colnames(X_model))]
X_model <- X_model[, .SD, .SDcols = c("symbol", "date", feature_cols, labels)]
X_holdout <- X_holdout[, .SD, .SDcols = c("symbol", "date", feature_cols, labels)]

# task with extreme bins
X_model_ <- X_model[get(labels[3]) %in% c(-1, 1)]
# X_model_[, labels[3] := droplevels(X_model_[, get(labels[3])])]
X_holdout_ <- X_holdout[get(labels[3]) %in% c(-1, 1)]
# X_holdout_[, labels[3] := droplevels(X_holdout_[, get(labels[3])])]
X_model_$bin_extreme_ret_excess_stand_22 <- as.factor(X_model_$bin_extreme_ret_excess_stand_22)
X_holdout_$bin_extreme_ret_excess_stand_22 <- as.factor(X_holdout_$bin_extreme_ret_excess_stand_22)
task_extreme <- as_task_classif(X_model_[, .SD, .SDcols = !c("symbol","date", labels[1:2])],
                                id = "extreme", target = labels[3], positive = "1")
task_extreme_holdout <- as_task_classif(X_holdout_[, .SD, .SDcols = !c("symbol","date", labels[1:2])],
                                        id = "extreme_holdout", target = labels[3], positive = "1")

# task with aroundzero bins
X_model$bin_aroundzero_ret_excess_stand_22 <- as.factor(X_model$bin_aroundzero_ret_excess_stand_22)
X_holdout$bin_aroundzero_ret_excess_stand_22 <- as.factor(X_holdout$bin_aroundzero_ret_excess_stand_22)
task_aroundzero <- as_task_classif(X_model[, .SD, .SDcols = !c("symbol","date", labels[c(1, 3)])],
                                   id = "aroundzero", target = labels[2])
task_aroundzero_holdout <- as_task_classif(X_holdout[, .SD, .SDcols = !c("symbol","date", labels[c(1, 3)])],
                                           id = "aroundzero_holdout", target = labels[2])

# task for regression
task_reg <- as_task_regr(X_model[, .SD, .SDcols = !c("symbol","date", labels[2:3])], id = "reg", target = labels[1])
task_reg_holdout <- as_task_regr(X_holdout[, .SD, .SDcols = !c("symbol","date", labels[2:3])], id = "reg_holdout", target = labels[1])



# FEATURE SELECTION (TEST) ------------------------------------------------
# select features
# important_vars <- setdiff(important_vars, "TSFEL_0_Histogram_7_132")
task_extreme$select(most_important_vars)
task_aroundzero$select(most_important_vars)
task_reg$select(most_important_vars)
task_extreme_holdout$select(most_important_vars)
task_aroundzero_holdout$select(most_important_vars)
task_reg_holdout$select(most_important_vars)



# DESCRIPTIVE ANALYSIS ----------------------------------------------------
# dependent variable
data_ <- task_reg$data()
dim(data_)
skimr::skim(data_$ret_excess_stand_66)

# rpart tree
# minsplit [2,128]
# minbucket [1,64]
# cp  [1e−04,0.1]
# cp [1e−04,1]
# maxdepth [1,30]
# minbucket [1,100]
# minsplit [1,100]
library(rpart.plot)
learner = lrn("classif.rpart", maxdepth = 4, predict_type = "prob", minbucket = 50, minsplit = 10, cp = 0.001)
learner$param_set
task_ <- task_extreme$clone()
learner$train(task_)
predictins = learner$predict(task_)
predictins$score(msr("classif.acc"))
learner$importance()
rpart_model <- learner$model
rpart.plot(rpart_model)



# CLASSIFICATION AUTOML ---------------------------------------------------
# learners
learners = list(
  ranger = lrn("classif.ranger", predict_type = "prob", id = "ranger"),
  # log_reg = lrn("classif.log_reg", predict_type = "prob", id = "log_reg"),
  kknn = lrn("classif.kknn", predict_type = "prob", id = "kknn"),
  # extratrees = lrn("classif.extratrees", predict_type = "prob", id = "extratrees"),
  cv_glmnet = lrn("classif.cv_glmnet", predict_type = "prob", id = "cv_glmnet"),
  xgboost = lrn("classif.xgboost", predict_type = "prob", id = "xgboost")
)
# create graph from list of learners
# choices = c("ranger", "log_reg", "kknn")
# learners = po("branch", choices, id = "branch_learners") %>>%
#   gunion(learners_l) %>>%
#   po("unbranch", choices, id = "unbranch_learners")

# create complete grapg
graph = po("removeconstants", ratio = 0.05) %>>%
  # modelmatrix
  # po("branch", options = c("nop_filter", "modelmatrix"), id = "interaction_branch") %>>%
  # gunion(list(po("nop", id = "nop_filter"), po("modelmatrix", formula = ~ . ^ 2))) %>>%
  # po("unbranch", id = "interaction_unbranch") %>>%
  # scaling
  po("branch", options = c("nop_prep", "yeojohnson", "pca", "ica"), id = "prep_branch") %>>%
  gunion(list(po("nop", id = "nop_prep"), po("yeojohnson"), po("pca", scale. = TRUE), po("ica"))) %>>%
  po("unbranch", id = "prep_unbranch") %>>%
  learners %>>%
  po("classifavg", innum = length(learners))
plot(graph)
graph_learner = as_learner(graph)
as.data.table(graph_learner$param_set)[1:100, .(id, class, lower, upper)]
as.data.table(graph_learner$param_set)[100:190, .(id, class, lower, upper)]
search_space = ps(
  # preprocesing
  # interaction_branch.selection = p_fct(levels = c("nop_filter", "modelmatrix")),
  prep_branch.selection = p_fct(levels = c("nop_prep", "yeojohnson", "pca", "ica")),
  pca.rank. = p_int(2, 6, depends = prep_branch.selection == "pca"),
  ica.n.comp = p_int(2, 6, depends = prep_branch.selection == "ica"),
  yeojohnson.standardize = p_lgl(depends = prep_branch.selection == "yeojohnson"),
  # models
  ranger.ranger.mtry.ratio = p_dbl(0.2, 1),
  ranger.ranger.max.depth = p_int(2, 6),
  kknn.kknn.k = p_int(5, 20),
  # extratrees.extratrees.ntree = p_int(200, 1000),
  # extratrees.extratrees.mtry = p_int(5, task_extreme$ncol),
  # extratrees.extratrees.nodesize = p_int(2, 10),
  # extratrees.extratrees.numRandomCuts = p_int(2, 5)
  xgboost.xgboost.nrounds = p_int(100, 5000),
  xgboost.xgboost.eta = p_dbl(1e-4, 1),
  xgboost.xgboost.max_depth = p_int(1, 8),
  xgboost.xgboost.colsample_bytree = p_dbl(0.1, 1),
  xgboost.xgboost.colsample_bylevel = p_dbl(0.1, 1),
  xgboost.xgboost.lambda = p_dbl(0.1, 1),
  xgboost.xgboost.gamma = p_dbl(1e-4, 1000),
  xgboost.xgboost.alpha = p_dbl(1e-4, 1000),
  xgboost.xgboost.subsample = p_dbl(0.1, 1)
)
# plan("multisession", workers = 4L)
at_classif = auto_tuner(
  method = "random_search",
  learner = graph_learner,
  resampling = rsmp("cv", folds = 3),
  measure = msr("classif.acc"),
  search_space = search_space,
  term_evals = 20
)
at_classif$train(task_extreme)

# inspect results
archive <- as.data.table(at_classif$archive)
archive
length(at_classif$state)
ggplot(archive[, mean(classif.acc), by = "ranger.ranger.max.depth"], aes(x = ranger.ranger.max.depth, y = V1)) + geom_line()
ggplot(archive[, mean(classif.acc), by = "prep_branch.selection"], aes(x = prep_branch.selection, y = V1)) + geom_bar(stat = "identity")
preds = at_classif$predict(task_extreme)
preds$confusion
preds$score(msr("classif.acc"))

# holdout extreme
preds_holdout <- at_classif$predict(task_extreme_holdout)
preds_holdout$confusion
autoplot(preds_holdout, type = "roc")
preds_holdout$score(msrs(c("classif.acc"))) # , "classif.recall", "classif.precision"
prediciotns_extreme_holdout <- as.data.table(preds_holdout)
prediciotns_extreme_holdout <- prediciotns_extreme_holdout[prob.1 > 0.6]
nrow(prediciotns_extreme_holdout)
mlr3measures::acc(prediciotns_extreme_holdout$truth, prediciotns_extreme_holdout$response)

# predictions for qc
cols_qc <- c("symbol", "date")
predictoins_qc <- cbind(X_holdout_[, ..cols_qc], as.data.table(preds_holdout))
predictoins_qc[, grep("row_ids|truth", colnames(predictoins_qc)) := NULL]
predictoins_qc <- unique(predictoins_qc)
setorder(predictoins_qc, "date")

# save to dropbox for live trading (create table for backtest)
cols <- c("date", "symbol", colnames(predictoins_qc)[3:ncol(predictoins_qc)])
pead_qc <- predictoins_qc[, ..cols]
pead_qc <- pead_qc[, .(symbol = paste0(unlist(symbol), collapse = ", ")), by = date]
bl_endp_key <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
cont <- storage_container(bl_endp_key, "qc-backtest")
storage_write_csv2(pead_qc, cont, file = "pead_qc_backtest_graph.csv", col_names = FALSE)

# save last predicitons for live trading
# pead_qc_live <- pead_qc[date == max(date)]
# bl_endp_key <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
# cont <- storage_container(bl_endp_key, "qc-live")
# storage_write_csv2(pead_qc_live, cont, file = "pead_qc_livr.csv", col_names = FALSE)




# REGRESSION -------------------------------------------------------------
# create graph
# learners
learners = list(
  ranger = lrn("regr.ranger", id = "ranger"),
  kknn = lrn("regr.kknn", id = "kknn"),
  # extratrees = lrn("classif.extratrees", predict_type = "prob", id = "extratrees"),
  cv_glmnet = lrn("regr.cv_glmnet", id = "cv_glmnet"),
  xgboost = lrn("regr.xgboost", id = "xgboost")
)

# create complete grapg
graph = po("removeconstants", ratio = 0.05) %>>%
  # scaling
  po("branch", options = c("nop_prep", "yeojohnson", "pca", "ica"), id = "prep_branch") %>>%
  gunion(list(po("nop", id = "nop_prep"), po("yeojohnson"), po("pca", scale. = TRUE), po("ica"))) %>>%
  po("unbranch", id = "prep_unbranch") %>>%
  # learners
  learners %>>%
  po("regravg")
plot(graph)
graph_learner = as_learner(graph)
as.data.table(graph_learner$param_set)[1:100, .(id, class, lower, upper)]
as.data.table(graph_learner$param_set)[100:190, .(id, class, lower, upper)]
search_space = ps(
  # preprocesing
  # interaction_branch.selection = p_fct(levels = c("nop_filter", "modelmatrix")),
  prep_branch.selection = p_fct(levels = c("nop_prep", "yeojohnson", "pca", "ica")),
  pca.rank. = p_int(2, 6, depends = prep_branch.selection == "pca"),
  ica.n.comp = p_int(2, 6, depends = prep_branch.selection == "ica"),
  yeojohnson.standardize = p_lgl(depends = prep_branch.selection == "yeojohnson"),
  # models
  ranger.ranger.mtry.ratio = p_dbl(0.2, 1),
  ranger.ranger.max.depth = p_int(2, 6),
  kknn.kknn.k = p_int(5, 20),
  # extratrees.extratrees.ntree = p_int(200, 1000),
  # extratrees.extratrees.mtry = p_int(5, task_extreme$ncol),
  # extratrees.extratrees.nodesize = p_int(2, 10),
  # extratrees.extratrees.numRandomCuts = p_int(2, 5)
  xgboost.xgboost.nrounds = p_int(100, 5000),
  xgboost.xgboost.eta = p_dbl(1e-4, 1),
  xgboost.xgboost.max_depth = p_int(1, 8),
  xgboost.xgboost.colsample_bytree = p_dbl(0.1, 1),
  xgboost.xgboost.colsample_bylevel = p_dbl(0.1, 1),
  xgboost.xgboost.lambda = p_dbl(0.1, 1),
  xgboost.xgboost.gamma = p_dbl(1e-4, 1000),
  xgboost.xgboost.alpha = p_dbl(1e-4, 1000),
  xgboost.xgboost.subsample = p_dbl(0.1, 1)
)
# plan("multisession", workers = 4L)
at_regr = auto_tuner(
  method = "random_search",
  learner = graph_learner,
  resampling = rsmp("cv", folds = 3),
  measure = msr("regr.mae"),
  search_space = search_space,
  term_evals = 20
)
at_regr$train(task_reg)

# inspect results
archive <- as.data.table(at_regr$archive)
archive
ggplot(archive, aes(x = ranger.ranger.mtry.ratio, y = regr.mae)) + geom_line()
ggplot(archive[, mean(regr.mae), by = "ranger.ranger.max.depth"], aes(x = ranger.ranger.max.depth, y = V1)) + geom_line()
ggplot(archive[, mean(regr.mae), by = "prep_branch.selection"], aes(x = prep_branch.selection, y = V1)) + geom_bar(stat = "identity")
preds = at_ranger$predict(task_reg)
preds$score(msr("regr.rmse"))

# holdout extreme
preds_holdout <- at_regr$predict(task_reg_holdout)
preds_holdout$score(msrs(c("regr.rmse")))
prediciotns_extreme_holdout <- as.data.table(preds_holdout)

prediciotns_extreme_holdout_buy <- prediciotns_extreme_holdout[response > 1]
nrow(prediciotns_extreme_holdout_buy)
prediciotns_extreme_holdout_buy[, `:=`(truth_01 = ifelse(truth > 0, 1, 0),
                                       response_01 = ifelse(response > 0, 1, 0))]
mlr3measures::acc(as.factor(prediciotns_extreme_holdout_buy$truth_01),
                  factor(prediciotns_extreme_holdout_buy$response_01, levels = c("0", "1")))


# SAVE DATA FRO BACKTEST --------------------------------------------------

# predictions for qc
cols_qc <- c("symbol", "date")
predictoins_qc <- cbind(X_holdout[, ..cols_qc], as.data.table(preds_holdout))
predictoins_qc[, grep("row_ids|truth", colnames(predictoins_qc)) := NULL]
predictoins_qc <- unique(predictoins_qc)
setorder(predictoins_qc, "date")

# save to dropbox for live trading (create table for backtest)
cols <- c("date", "symbol", colnames(predictoins_qc)[3:ncol(predictoins_qc)])
pead_qc <- predictoins_qc[, ..cols]
pead_qc <- pead_qc[, .(symbol = paste0(unlist(symbol), collapse = ", "),
                       prob1 = paste0(unlist(response), collapse = ",")), by = date]
bl_endp_key <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
cont <- storage_container(bl_endp_key, "qc-backtest")
storage_write_csv2(pead_qc, cont, file = "pead_qc_backtest_graph.csv", col_names = FALSE)


# save last predicitons for live trading
pead_qc_live <- pead_qc[date == max(date)]
bl_endp_key <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
cont <- storage_container(bl_endp_key, "qc-live")
storage_write_csv2(pead_qc_live, cont, file = "pead_qc_livr.csv", col_names = FALSE)




### GLMNET
learner = lrn("regr.glmnet")
graph = po("removeconstants", ratio = 0.05) %>>%
  # scaling
  po("branch", options = c("nop_prep", "yeojohnson", "pca", "ica"), id = "prep_branch") %>>%
  gunion(list(po("nop", id = "nop_prep"), po("yeojohnson"), po("pca", scale. = TRUE), po("ica"))) %>>%
  po("unbranch", id = "prep_unbranch") %>>%
  learner
plot(graph)
graph_learner = as_learner(graph)
as.data.table(graph_learner$param_set)[1:70, .(id, class, lower, upper)]
search_space = ps(
  # preprocesing
  prep_branch.selection = p_fct(levels = c("nop_prep", "yeojohnson", "pca", "ica")),
  pca.rank. = p_int(2, 6, depends = prep_branch.selection == "pca"),
  ica.n.comp = p_int(2, 6, depends = prep_branch.selection == "ica"),
  yeojohnson.standardize = p_lgl(depends = prep_branch.selection == "yeojohnson"),
  # model
  regr.glmnet.alpha = p_dbl(0, 1),
  regr.glmnet.s = p_dbl(1e-4, 1000)
)
plan("multisession", workers = 4L)
at_glmnet = auto_tuner(
  method = "random_search",
  learner = graph_learner,
  resampling = rsmp("cv", folds = 3),
  measure = msr("regr.rmse"),
  search_space = search_space,
  term_evals = 25
)
at_glmnet$train(task_reg)

# inspect results
archive <- as.data.table(at_glmnet$archive)
archive
ggplot(archive, aes(x = regr.glmnet.alpha, y = regr.rmse)) + geom_line()
ggplot(archive, aes(x = regr.glmnet.s, y = regr.rmse)) + geom_line()
preds = at_glmnet$predict(task_reg)
preds$score(msr("regr.rmse"))

# holdout extreme
preds_holdout <- at_glmnet$predict(task_reg_holdout)
preds_holdout$score(msrs(c("regr.rmse")))
prediciotns_extreme_holdout <- as.data.table(preds_holdout)

prediciotns_extreme_holdout_buy <- prediciotns_extreme_holdout[response > 0.02]
nrow(prediciotns_extreme_holdout_buy)
prediciotns_extreme_holdout_buy[, `:=`(truth_01 = ifelse(truth > 0, 1, 0),
                                       response_01 = ifelse(response > 0, 1, 0))]
mlr3measures::acc(as.factor(prediciotns_extreme_holdout_buy$truth_01),
                  factor(prediciotns_extreme_holdout_buy$response_01, levels = c("0", "1")))



### XGBOOST
learner = lrn("regr.xgboost")
graph = po("removeconstants", ratio = 0.05) %>>%
  # scaling
  po("branch", options = c("nop_prep", "yeojohnson", "pca", "ica"), id = "prep_branch") %>>%
  gunion(list(po("nop", id = "nop_prep"), po("yeojohnson"), po("pca", scale. = TRUE), po("ica"))) %>>%
  po("unbranch", id = "prep_unbranch") %>>%
  learner
plot(graph)
graph_learner = as_learner(graph)
as.data.table(graph_learner$param_set)[1:80, .(id, class, lower, upper)]
search_space = ps(
  # preprocesing
  prep_branch.selection = p_fct(levels = c("nop_prep", "yeojohnson", "pca", "ica")),
  pca.rank. = p_int(2, 6, depends = prep_branch.selection == "pca"),
  ica.n.comp = p_int(2, 6, depends = prep_branch.selection == "ica"),
  yeojohnson.standardize = p_lgl(depends = prep_branch.selection == "yeojohnson"),
  # model
  regr.xgboost.nrounds = p_int(100, 5000),
  regr.xgboost.eta = p_dbl(1e-4, 1),
  regr.xgboost.max_depth = p_int(1, 8),
  regr.xgboost.colsample_bytree = p_dbl(0.1, 1),
  regr.xgboost.colsample_bylevel = p_dbl(0.1, 1),
  regr.xgboost.lambda = p_dbl(0.1, 1),
  regr.xgboost.gamma = p_dbl(1e-4, 1000),
  regr.xgboost.alpha = p_dbl(1e-4, 1000),
  regr.xgboost.subsample = p_dbl(0.1, 1)
)
plan("multisession", workers = 4L)
at_xgboost = auto_tuner(
  method = "random_search",
  learner = graph_learner,
  resampling = rsmp("cv", folds = 3),
  measure = msr("regr.rmse"),
  search_space = search_space,
  term_evals = 25
)
at_xgboost$train(task_reg)

# inspect results
archive <- as.data.table(at_xgboost$archive)
archive
preds = at_xgboost$predict(task_reg)
preds$score(msr("regr.rmse"))

# holdout extreme
preds_holdout <- at_xgboost$predict(task_reg_holdout)
preds_holdout$score(msrs(c("regr.rmse")))
prediciotns_extreme_holdout <- as.data.table(preds_holdout)

prediciotns_extreme_holdout_buy <- prediciotns_extreme_holdout[response > 2]
nrow(prediciotns_extreme_holdout_buy)
prediciotns_extreme_holdout_buy[, `:=`(truth_01 = ifelse(truth > 0, 1, 0),
                                       response_01 = ifelse(response > 0, 1, 0))]
mlr3measures::acc(as.factor(prediciotns_extreme_holdout_buy$truth_01),
                  factor(prediciotns_extreme_holdout_buy$response_01, levels = c("0", "1")))



# TORCH -------------------------------------------------------------------
learner <- lrn("classif.torch.tabnet", epochs = 15, batch_size = 128, predict_type = "prob")
graph = po("removeconstants", ratio = 0.05) %>>%
  # scaling
  po("branch", options = c("nop_prep", "yeojohnson"), id = "prep_branch") %>>%
  gunion(list(po("nop", id = "nop_prep"), po("yeojohnson"))) %>>%
  po("unbranch", id = "prep_unbranch") %>>%
  learner
plot(graph)
graph_learner = as_learner(graph)
as.data.table(graph_learner$param_set)[1:60, .(id, class, lower, upper)]
search_space = ps(
  # preprocesing
  prep_branch.selection = p_fct(levels = c("nop_prep", "yeojohnson")),
  yeojohnson.standardize = p_lgl(depends = prep_branch.selection == "yeojohnson"),
  # model
  classif.torch.tabnet.decision_width = p_int(8, 64),
  classif.torch.tabnet.attention_width = p_int(8, 64),
  classif.torch.tabnet.num_steps = p_int(4, 7),
  classif.torch.tabnet.learn_rate = p_dbl(0.0001, 0.1)
)
plan("multisession", workers = 4L)
at_tabnet = auto_tuner(
  method = "random_search",
  learner = graph_learner,
  resampling = rsmp("cv", folds = 3),
  measure = msr("classif.acc"),
  search_space = search_space,
  term_evals = 15
)
at_tabnet$train(task_extreme)

# inspect results
archive <- as.data.table(at_tabnet$archive)
archive
ggplot(archive[, mean(classif.acc), by = "classif.torch.tabnet.decision_width"], aes(x = classif.torch.tabnet.decision_width, y = V1)) +
  geom_line()
ggplot(archive[, mean(classif.acc), by = "classif.torch.tabnet.attention_width"], aes(x = classif.torch.tabnet.attention_width, y = V1)) +
  geom_line()
ggplot(archive[, mean(classif.acc), by = "classif.torch.tabnet.num_steps"], aes(x = classif.torch.tabnet.num_steps, y = V1)) +
  geom_line()
ggplot(archive[, mean(classif.acc), by = "prep_branch.selection"], aes(x = prep_branch.selection, y = V1)) + geom_bar(stat = "identity")
preds = at_tabnet$predict(task_extreme)
preds$score(msr("classif.acc"))

# holdout extreme
preds_holdout <- at_tabnet$predict(task_extreme_holdout)
preds_holdout$score(msrs(c("classif.acc")))
prediciotns_extreme_holdout <- as.data.table(preds_holdout)

prediciotns_extreme_holdout_buy <- prediciotns_extreme_holdout[prob.1 > 0.6]
nrow(prediciotns_extreme_holdout_buy)
prediciotns_extreme_holdout_buy[, `:=`(truth_01 = ifelse(truth > 0, 1, 0),
                                       response_01 = ifelse(response > 0, 1, 0))]
mlr3measures::acc(prediciotns_extreme_holdout$truth, prediciotns_extreme_holdout$response)
