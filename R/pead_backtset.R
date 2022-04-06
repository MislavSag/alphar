library(data.table)    # core
library(xts)           # core
library(httr)          # core
library(mlr3verse)     # core
library(ggplot2)       # core
library(mlr3automl)    # mlr3 Automl
library(mlr3fselect)   # mlr3 package for feature selection
library(paradox)       # define parameters space
library(AzureStor)     # import data from Azure blob storage
library(roll)          # very fast rolling functions (roll_mean, roll_sd etc)
library(RollingWindow) # contains some functions not available in roll
library(equityData)    # Mislav's package for loading and scraping financial data
library(mlfinance)     # Mislav's package for financial machine learning
library(DescTools)     # only fpor Winsorize function. TODO Develope mlr3 pipeline for this preprocessing step
library(rdrop2)
drop_auth()



# SETUP -------------------------------------------------------------------

# globals
apikey_fmp = Sys.getenv("APIKEY-FMPCLOUD")
fmp_host = "https://financialmodelingprep.com/"
save_path_mlr3models = file.path("D:/mlfin/mlr3_models")
refresh_data_old_days = 100

# usa and sp500 stocks. This is used later for sabsampling
url <- modify_url(fmp_host, path = "api/v3/available-traded/list",
                  query = list(apikey = apikey_fmp))
stocks <- rbindlist(content(GET(url)))
usa_symbols <- stocks[exchange %in% c("AMEX", "New York Stock Exchange Arca",
                                      "New York Stock Exchange", "NasdaqGS",
                                      "Nasdaq", "NASDAQ", "AMEX", "NYSEArca",
                                      "NASDAQ Global Market", "Nasdaq Global Market",
                                      "NYSE American", "Nasdaq Capital Market",
                                      "Nasdaq Global Select")]
url <- modify_url(fmp_host, path = "api/v3/historical/sp500_constituent",
                  query = list(apikey = apikey_fmp))
sp500 <- rbindlist(httr::content(GET(url)))
sp500_symbols <- unique(c(sp500$removedTicker, sp500$symbol))

# import mlr3 benchmark results
list.files('D:/mlfin/mlr3_models')
pick_model_index <- 7
model_file <- list.files('D:/mlfin/mlr3_models', full.names = TRUE)[pick_model_index]
model_01ext <- readRDS(model_file)
autoplot(model_01ext) + theme(axis.text.x = element_text(angle = 45, hjust = 1))
measures = list(
  msr("classif.acc", id = "acc_train", predict_sets = 'train'),
  msr("classif.acc", id = "acc_test", predict_sets = 'test')
)
best_models <- model_01ext$score(measures)[,best := acc_test == max(acc_test),  by = learner_id]
best_models <- best_models[best == 1]
best_models <- best_models[learner_id != 'classif.featureless']
feature_names <- best_models$task[[1]]$feature_names

# get earnings estimates and actual earnings
events <- get_blob_file("earnings-calendar.rds", "fundamentals",
                        save_file = "D:/fundamental_data/earnings-calendar.rds", refresh_data_old = 100)
events <- events[date < Sys.Date()] # remove announcements for today (not important if don't use)
events <- na.omit(events, cols = c("eps", "epsEstimated")) # remove rows with NA for earnings
events[revenue == 0 | revenueEstimated == 0] # number of missing revenues data (be aware)


# get earnings announcements from investing.com
investingcom_ea <- get_blob_file("earnings-calendar-investingcom.rds", "fundamentals",
                                 save_file = "D:/fundamental_data/earnings-calendar-investingcom.rds",
                                 refresh_data_old = 100)
investingcom_ea <- na.omit(investingcom_ea, cols = c("eps", "eps_forecast"))
investingcom_ea <- investingcom_ea[, .(symbol, datetime, eps, eps_forecast, revenue, revenue_forecast, right_time)]
investingcom_ea[, date_investingcom := as.Date(datetime)]
setnames(investingcom_ea, colnames(investingcom_ea)[2:6], paste0(colnames(investingcom_ea)[2:6], "_investingcom"))

# merge DT and investing com earnings surprises
DT <- merge(events, investingcom_ea, by.x = c("symbol", "date"), by.y = c("symbol", "date_investingcom"), all.x = TRUE, all.y = FALSE)
DT <- DT[date == as.Date(datetime_investingcom)] # keep only observations where dates are equal in FMP CLOUD and investing.com
DT <- DT[abs(eps - eps_investingcom) < 0.02]     # keep only observations where earnings are very similar

# get daily market data for all stocks
prices <- get_blob_file("prices.rds", container = "fundamentals", save_file = "D:/fundamental_data/prices.rds", refresh_data_old = 100)
prices <- prices[open > 0 & high > 0 & low > 0 & close > 0 & adjClose > 0] # remove rows with zero and negative prices
setorderv(prices, c("symbol", "date"))
prices[, returns := adjClose / data.table::shift(adjClose) - 1, by = symbol]
prices <- prices[returns < 1] # remove observations where returns are lower than 100%. TODO:: better outlier detection mechanism
adjust_cols <- c("open", "high", "low")
prices[, (adjust_cols) := lapply(.SD, function(x) x * (adjClose / close)), .SDcols = adjust_cols] # adjust open, high and low prices
prices[, close := adjClose]
prices <- na.omit(prices[, .(symbol, date, open, high, low, close, volume, vwap, returns)])
prices_n <- prices[, .N, by = symbol]
prices_n <- prices_n[which(prices_n$N > 150)]  # remove prices with only 60 or less observations
prices <- prices[symbol %in% prices_n$symbol]
prices <- prices[symbol %in% unique(DT$symbol)]

# Create features from ohlcv data
features_set <- features_from_ohlcv(prices[, .(symbol, date, open, high, low, close, volume)],
                                    window_sizes = c(5, 22, 22 * 3))
DT[, date_events := date] # change column, so we know which date it is. Additionaly, we will loose date column in mergeing.
DT[, date_events_lead := date + 1] # change column, so we know which date it is. Additionaly, we will loose date column in mergeing.
features_set[, date_features := date]
features_events <- merge(DT, features_set, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE)
features_lead <- features_set[DT, on = c(symbol = "symbol", date = "date_events_lead"), roll = +Inf]
features <- copy(features_lead)

# actual vs estimated
features[, `:=`(
  eps_diff = (eps - epsEstimated) / close,
  rev_diff = (revenue - revenueEstimated) / close
)]
setorderv(features, c("symbol", "date"))

# filter rows
clf_data <- copy(features)
clf_data <- clf_data[symbol %in% usa_symbols$symbol] # keep only USA stocks
clf_data <- clf_data[revenueEstimated != 0]          # keep observations with revenue estimates

# select features vector
keep_meta_features <- c("symbol", "date", feature_names)
clf_data <- clf_data[, ..keep_meta_features]

# remove NA values
clf_data <- na.omit(clf_data)
all(sapply(clf_data[, ..feature_names], function(x) all(is.finite(x)))) # check for infinity values
# clf_data <- clf_data[is.finite(rowSums(clf_data[, 3:ncol(clf_data)])),]
all(sapply(clf_data[, ..feature_names], function(x) all(is.finite(x)))) # check for infinity values
all(sapply(clf_data[, ..feature_names], function(x) all(!is.na(x)))) # check NAN

# make predictions
predictions <- lapply(best_models$learner, function(x) {
  y <- as.data.table(x$learner$predict_newdata(newdata = clf_data[, ..feature_names]))
  colnames(y) <- paste(colnames(y), x$id, sep = "_")
  y
})
predictions <- do.call(cbind, predictions)
predictions[, grep("row_ids|truth_", colnames(predictions)) := NULL]
predictions <- cbind(clf_data[, .(symbol, date)], predictions)
predictions <- unique(predictions)
setorder(predictions, "date")
colnames(predictions) <- gsub(".tuned", "", colnames(predictions))

# create table for backtest
cols <- c("date", "symbol", colnames(predictions)[3:ncol(predictions)])
pead_backtest <- predictions[, ..cols]
# pead_backtest[, mean_prob_1 = mean(), by = date]
pead_backtest <- pead_backtest[, .(symbol = list(symbol), prob.1_classif.ranger = list(prob.1_classif.ranger)), by = date]

# save to local path and dropbox
fwrite(pead_backtest, "D:/mlfin/backtest_data/pead_backtest.csv", col.names = FALSE, dateTimeAs = "write.csv")
drop_upload(file = "D:/mlfin/backtest_data/pead_backtest.csv", path = "pead")
