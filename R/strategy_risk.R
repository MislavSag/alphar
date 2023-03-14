# Title:  Title
# Author: Name
# Description: Description

# packages
library(tiledb)
library(data.table)
library(checkmate)
library(httr)
library(findata)



# SET UP ------------------------------------------------------------------
# check if we have all necessary env variables
assert_choice("AWS-ACCESS-KEY", names(Sys.getenv()))
assert_choice("AWS-SECRET-KEY", names(Sys.getenv()))
assert_choice("AWS-REGION", names(Sys.getenv()))
assert_choice("BLOB-ENDPOINT", names(Sys.getenv()))
assert_choice("BLOB-KEY", names(Sys.getenv()))
assert_choice("APIKEY-FMPCLOUD", names(Sys.getenv()))
assert_choice("FRED-KEY", names(Sys.getenv()))

# set credentials
config <- tiledb_config()
config["vfs.s3.aws_access_key_id"] <- Sys.getenv("AWS-ACCESS-KEY")
config["vfs.s3.aws_secret_access_key"] <- Sys.getenv("AWS-SECRET-KEY")
config["vfs.s3.region"] <- Sys.getenv("AWS-REGION")
context_with_config <- tiledb_ctx(config)
fredr_set_key(Sys.getenv("FRED-KEY"))

# parameters

# date segments
DOTCOM <- c("2000-01-01", "2002-01-01")
GFC <- c("2007-01-01", "2010-01-01")
AFTERGFCBULL <- c("2010-01-01", "2015-01-01")
COVID <- c("2020-01-01", "2021-06-01")
AFTER_COVID <- c("2021-06-01", "2022-01-01")
NEW <- c("2022-01-01", as.character(Sys.Date()))



# UNIVERSES ---------------------------------------------------------------
# universe consists of US stocks
url <- modify_url("https://financialmodelingprep.com/",
                  path = "api/v3/stock/list",
                  query = list(apikey = Sys.getenv("APIKEY-FMPCLOUD")))
p <- GET(url)
res <- content(p)
securities <- rbindlist(res, fill = TRUE)
stocks_us <- securities[type == "stock" &
                          exchangeShortName %in% c("AMEX", "NASDAQ", "NYSE", "OTC")]
symbols_list <- stocks_us[, unique(symbol)]

# sp500 symbols
fmp = FMP$new()
sp500_symbols <- fmp$get_sp500_symbols()
sp500_symbols <- na.omit(sp500_symbols)
sp500_symbols <- sp500_symbols[!grepl("/", sp500_symbols)]




# MARKET DATA -------------------------------------------------------------
# import market data (choose frequency)
arr <- tiledb_array("D:/equity-usa-daily-fmp",
                    as.data.frame = TRUE,
                    query_layout = "UNORDERED"
)
system.time(prices <- arr[])
tiledb_array_close(arr)
prices <- as.data.table(prices)

# filter dates and symbols
prices_dt <- prices[symbol %in% c(unique(events), "SPY")]
prices_dt <- prices_dt[date > as.Date("2010-01-01")]
prices_dt <- unique(prices_dt, by = c("symbol", "date"))
prices_dt[symbol == "SPY"]
prices_dt[symbol == "ZYXI"]

# free resources
rm(prices)
gc()

# clean daily prices
prices_dt <- prices_dt[open > 0 & high > 0 & low > 0 & close > 0 & adjClose > 0] # remove rows with zero and negative prices
setorder(prices_dt, "symbol", "date")
prices_dt[, returns := adjClose   / data.table::shift(adjClose) - 1, by = symbol] # calculate returns
prices_dt <- prices_dt[returns < 1] # TODO:: better outlier detection mechanism. For now, remove daily returns above 100%
adjust_cols <- c("open", "high", "low")
prices_dt[, (adjust_cols) := lapply(.SD, function(x) x * (adjClose / close)), .SDcols = adjust_cols] # adjust open, high and low prices
prices_dt[, close := adjClose]
prices_dt <- na.omit(prices_dt[, .(symbol, date, open, high, low, close, volume, returns)])
prices_n <- prices_dt[, .N, by = symbol]
prices_n <- prices_n[N > 700]  # remove prices with only 700 or less observations
prices_dt <- prices_dt[symbol %in% prices_n]

# save SPY for later and keep only events symbols
spy <- prices_dt[symbol == "SPY"]
setorder(spy, date)










# IMPORT DATA -------------------------------------------------------------
# clean daily prices
prices <- prices[open > 0 & high > 0 & low > 0 & close > 0 & adjClose > 0] # remove rows with zero and negative prices
prices <- unique(prices, by = c("symbol", "date")) #TODO SEND MAIL TO FMP CLOUD ! WHY WE HAVE DUPLICATED ROWS, DIFFERENT ADJUSTED CLOSE PRICES
setorder(prices, "symbol", "date")
prices[, returns := adjClose   / data.table::shift(adjClose) - 1, by = symbol]
adjust_cols <- c("open", "high", "low")
prices[, (adjust_cols) := lapply(.SD, function(x) x * (adjClose / close)), .SDcols = adjust_cols] # adjust open, high and low prices
prices[, close_raw := close]
prices[, close := adjClose]
prices <- na.omit(prices[, .(symbol, date, close_raw, open, high, low, close, volume, returns)])
prices <- unique(prices, by = c("symbol", "date")) # remove duplicates if they exists
prices <- prices[returns < 0.8 & returns > -0.8]
prices[, high_return := high / shift(high) - 1, by = symbol]
prices <- prices[high_return < 0.8 & high_return > -0.8]
prices[, low_return := low / shift(low) - 1, by = symbol]
prices <- prices[low_return < 0.8 & low_return > -0.8]
prices[, `:=`(low_return = NULL, high_return = NULL)]

# keep usa, non-etf
prices_usa <- prices[symbol %in% usa_symbols$symbol]
prices_usa[, `:=`(year = year(date), month = month(date))] # help columns

# keep only trading days
dim(prices_usa)
prices_usa <- prices_usa[date %in% getBusinessDays(as.Date("2000-01-01"), Sys.Date())]
dim(prices_usa)

# SPY data
spy <- prices[symbol == "SPY", .(date, close, returns)]

# find bonds with long history data
bonds_hy_symbols <- usa_symbols[grep("bond", name, ignore.case = TRUE)]
bonds_hy <- prices[symbol %in% bonds_hy_symbols$symbol]
bonds_hy[date < as.Date("2002-01-01"), unique(symbol)]

# market cap
arr <- tiledb_array("s3://equity-usa-market-cap",
                    as.data.frame = TRUE,
                    selected_ranges = list(symbol = cbind(unique(prices_usa$symbol), unique(prices_usa$symbol))))
market_cap_data <- arr[]
market_cap_data <- as.data.table(market_cap_data)

# merge prices and market cap
prices_usa <- merge(prices_usa, market_cap_data, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE)



# FRED DATA ---------------------------------------------------------------
# get categories
categories_id <- read_html("https://fred.stlouisfed.org/categories/") |>
  html_elements("a") |>
  html_attr("href")
categories_id <- categories_id[grep("categories/\\d+", categories_id)]
categories_id <- gsub("/categories/", "", categories_id)
categories_id <- as.integer(categories_id)

# get chinld categories
child_categories <- lapply(categories_id, fredr_category_children)
child_categories_ids <- rbindlist(child_categories, fill = TRUE)
child_categories_ids <- child_categories_ids$id

# scrap ata for every category
fredr_categories_daily <- list()
for (i in seq_along(child_categories_ids)) {
  fredr_categories_daily[[i]] <- fredr_category_series(
    category_id = child_categories_ids[i], # Housing
    limit = 1000L,
    order_by = "last_updated",
    filter_variable = "frequency",
    filter_value = "Daily"
  )
  Sys.sleep(0.8)
}
daily_series <- fredr_categories_daily[unlist(lapply(fredr_categories_daily, function(x) length(x) > 0))]
daily_series <- rbindlist(daily_series, fill = TRUE)
dim(daily_series)

# remove observations that end before week ago
keep_ids <- daily_series[as.Date(observation_end) > (Sys.Date() - 7), id]
daily_series <- daily_series[id %in% keep_ids]
dim(daily_series)

# remove observation wiyh not enough infomration
keep_ids <- daily_series[as.Date(observation_start) < as.Date("2004-01-01"), id]
daily_series <- daily_series[id %in% keep_ids]
dim(daily_series)

# get series
get_fread_data <- function(id = "VIXCLS") {
  data_ <- fredr(series_id = id,
                 observation_start = as.Date("2000-01-01"),
                 observation_end = Sys.Date())
  data_ <- as.data.table(data_)
  data_ <- data_[, .(series_id, date, value)]
  Sys.sleep(0.8)
  return(data_)
}
daily_series_l <- lapply(unique(daily_series$id), function(x) get_fread_data(id = x))
daily_series_raw <- rbindlist(daily_series_l, fill = TRUE)
daily_series_raw <- dcast(daily_series_raw, date ~ series_id, value.var = "value")
fred_daily <- copy(daily_series_raw)
colnames(fred_daily)[grepl("VIX", colnames(fred_daily))]

# keep only trading days by NY SE
dim(fred_daily)
fred_daily <- fred_daily[date %in% RcppQuantuccia::getBusinessDays(as.Date("2000-01-01"), Sys.Date())]
dim(fred_daily)

# inspect daily series
naniar::vis_miss(fred_daily[, 31:50])
vix <- fred_daily[, .(date, VIXCLS)]
plot(na.omit(as.xts.data.table(vix)))
vix <- vix[!is.na(VIXCLS)]
vix$date[!(vix$date %in% unique(prices_usa$date))]
vix[date == "2019-05-01"]
prices_usa[date == as.Date("2019-05-01")]

# keep data after 2004
# fred_daily <- fred_daily[date > as.Date("2004-01-01")]

# remove columns with many NA
keep_cols <- names(which(colMeans(!is.na(fred_daily)) > 0.99))
print(paste0("Removing columns with many NA values: ", setdiff(colnames(fred_daily), keep_cols)))
fred_daily <- fred_daily[, .SD, .SDcols = keep_cols]

# remove Inf and Nan values if they exists
is.infinite.data.frame <- function(x) do.call(cbind, lapply(x, is.infinite))
keep_cols <- names(which(colMeans(!is.infinite(as.data.frame(fred_daily))) > 0.9999999))
print(paste0("Removing columns with Inf values: ", setdiff(colnames(fred_daily), keep_cols)))
fred_daily <- fred_daily[, .SD, .SDcols = keep_cols]

# remove inf values
dim(fred_daily)
fred_daily <- fred_daily[is.finite(rowSums(fred_daily[, .SD, .SDcols = is.numeric], na.rm = TRUE))]
dim(fred_daily)

# define features
fred_predictors <- colnames(fred_daily)[2:ncol(fred_daily)]

# remove NA values
dim(fred_daily)
fred_daily <- na.omit(fred_daily, cols = fred_predictors)
dim(fred_daily)

# remove constant columns
remove_cols <- fred_predictors[apply(fred_daily[, ..fred_predictors], 2, var, na.rm=TRUE) == 0]
print(paste0("Removing feature with 0 standard deviation: ", remove_cols))
fred_predictors <- setdiff(fred_predictors, remove_cols)

# # # winsorization (remove outliers)
# # fred_daily[, (fred_predictors) := lapply(.SD, Winsorize, probs = c(0.001, 0.999), na.rm = TRUE), .SDcols = fred_predictors]
#
# # # remove constant columns
# # remove_cols <- fred_predictors[apply(fred_daily[, ..fred_predictors], 2, var, na.rm=TRUE) == 0]
# # print(paste0("Removing feature with 0 standard deviation: ", remove_cols))
# # fred_predictors <- setdiff(fred_predictors, remove_cols)
#
# remove highly correlated features
cor_matrix <- cor(fred_daily[, ..fred_predictors])
cor_matrix_rm <- cor_matrix
cor_matrix_rm[upper.tri(cor_matrix_rm)] <- 0
diag(cor_matrix_rm) <- 0
remove_cols <- fred_predictors[apply(cor_matrix_rm, 2, function(x) any(abs(x) > 0.97))]
print(paste0("Removing highly correlated featue (> 0.97): ", remove_cols))
fred_predictors <- setdiff(fred_predictors, remove_cols)



# CALULATE PREDICTIONS ----------------------------------------------------
# mcap (micro, mid, large) 52w high / 52w low
mcap_52 <- prices_usa[, .(symbol, date, high, low, close, marketCap)]
mcap_52[, q_33 := quantile(marketCap, probs = c(0.33), na.rm = TRUE), by = "date"]
mcap_52[, q_66 := quantile(marketCap, probs = c(0.66), na.rm = TRUE), by = "date"]
mcap_52 <- mcap_52[!is.na(q_33) & !is.na(q_66)]
mcap_52[marketCap < q_33, size := "micro" ]
mcap_52[marketCap > q_66, size := "large" ]
mcap_52[marketCap >= q_33 & marketCap <= q_66, size := "mid" ]
mcap_52[is.na(size)]
setorderv(mcap_52, c("symbol", "date"))
mcap_52[, high52w := frollapply(high, 52 * 5, max), by = "symbol"]
mcap_52[, low52w := frollapply(low, 52 * 5, min), by = "symbol"]
divergence <- 0.02 # PARAMETER
mcap_52[(high52w / close  - 1) < divergence, high52w_dummy := 1]
mcap_52[(low52w / close  - 1) > -divergence, low52w_dummy := 1]
print(paste0("Many missing values!!! Percent of total observations: ", sum(is.na(mcap_52$marketCap)) / nrow(mcap_52) ))
mcap_52 <- mcap_52[!is.na(marketCap)]
mcap_52_indicator <- mcap_52[, .(high52w_sum = sum(high52w_dummy, na.rm = TRUE),
                                 low52w_sum = sum(low52w_dummy, na.rm = TRUE)), by = c("size", "date")]
setorderv(mcap_52_indicator, c("size", "date"))
mcap_52_indicator[, low_high_52w_ratio := low52w_sum / high52w_sum]
ggplot(mcap_52_indicator, aes(x = date, y = low_high_52w_ratio, color = size)) +
  geom_line() +
  ggtitle("Whole period")
g1 <- ggplot(mcap_52_indicator[date %between% DOTCOM], aes(x = date, y = low_high_52w_ratio, color = size)) +
  geom_line() +
  ggtitle("DOTCOM")
g2 <- ggplot(mcap_52_indicator[date %between% GFC], aes(x = date, y = low_high_52w_ratio, color = size)) +
  geom_line() +
  ggtitle("GFC")
g3 <- ggplot(mcap_52_indicator[date %between% AFTERGFCBULL], aes(x = date, y = low_high_52w_ratio, color = size)) +
  geom_line() +
  ggtitle("AFTERGFCBULL")
g4 <- ggplot(mcap_52_indicator[date %between% COVID], aes(x = date, y = low_high_52w_ratio, color = size)) +
  geom_line() +
  ggtitle("COVID")
(g1 / g2) | (g3 / g4)
ggplot(mcap_52_indicator[date %between% NEW], aes(x = date, y = low_high_52w_ratio, color = size)) +
  geom_line() +
  ggtitle("NEW")

# volume of growth / decline stocks
vol_52 <- prices_usa[, .(symbol, date, high, low, close, volume)]
vol_52[, return_52w := close / shift(close, n = 52 * 5) - 1, by = "symbol"]
vol_52[return_52w > 0, return_52w_dummy := "up", by = "symbol"]
vol_52[return_52w <= 0, return_52w_dummy := "down", by = "symbol"]
vol_52 <- vol_52[!is.na(return_52w_dummy)]
vol_52_indicators <- vol_52[, .(volume = sum(volume, na.rm = TRUE)), by = c("return_52w_dummy", "date")]
setorderv(vol_52_indicators, c("return_52w_dummy", "date"))
vol_52_indicators <- dcast(vol_52_indicators, date ~ return_52w_dummy)
vol_52_indicators[, volume_down_up_ratio := down / up]
ggplot(vol_52_indicators, aes(x = date, y = volume_down_up_ratio)) +
  geom_line() +
  ggtitle("Whole period")
g1 <- ggplot(vol_52_indicators[date %between% DOTCOM], aes(x = date, y = volume_down_up_ratio)) +
  geom_line() +
  ggtitle("DOTCOM")
g2 <- ggplot(vol_52_indicators[date %between% GFC], aes(x = date, y = volume_down_up_ratio)) +
  geom_line() +
  ggtitle("GFC")
g3 <- ggplot(vol_52_indicators[date %between% AFTERGFCBULL], aes(x = date, y = volume_down_up_ratio)) +
  geom_line() +
  ggtitle("AFTERGFCBULL")
g4 <- ggplot(vol_52_indicators[date %between% COVID], aes(x = date, y = volume_down_up_ratio)) +
  geom_line() +
  ggtitle("COVID")
(g1 / g2 / g3 / g4)
ggplot(vol_52_indicators[date %between% NEW], aes(x = date, y = volume_down_up_ratio)) +
  geom_line() +
  ggtitle("NEW")

# spread high yield / investment grade
bonds <- prices[symbol %in% c("DHF", "DSM"), .(symbol, date, close)]
bonds <- dcast(bonds, date ~ symbol)
bonds$spread <- bonds[, 2] / bonds[, 3]
ggplot(bonds, aes(x = date, y = spread)) +
  geom_line()

# ohlcv object
ohlcv = Ohlcv$new(prices_usa[symbol == "SPY", .(symbol, date, open, high, low, close, volume)])

# exuber
exuber_obj = RollingExuber$new(windows = c(150, 300, 600),
                               workers = 8L,
                               lag = 0L,
                               at = 1:nrow(ohlcv$X),
                               na_pad = TRUE,
                               simplify = FALSE,
                               exuber_lag = 1L)
exuber_features <- exuber_obj$get_rolling_features(ohlcv)

# autoarima
autoarima_obj = RollingForecats$new(windows = c(252, 252 * 2),
                                    workers = 8L,
                                    lag = 0L,
                                    at = 1:nrow(ohlcv$X),
                                    na_pad = TRUE,
                                    simplify = FALSE,
                                    forecast_type = "autoarima")
autoarima_features <- autoarima_obj$get_rolling_features(ohlcv)

# BidAsk features
RollingBidAskInstance <- RollingBidAsk$new(c(22, 22 * 3),
                                           8L,
                                           at = 1:nrow(ohlcv$X),
                                           lag = 0L,
                                           na_pad = TRUE,
                                           simplify = FALSE)
RollingBidAskFeatures = RollingBidAskInstance$get_rolling_features(ohlcv)


# Rcatch22
RollingTheftInit = RollingTheft$new(c(22, 22*6, 22*12, 22*12*2),
                                    workers = 8L,
                                    at = 1:nrow(ohlcv$X),
                                    lag = 0L,
                                    na_pad = TRUE,
                                    simplify = FALSE,
                                    features_set = "catch22")
RollingTheftCatch22Features = RollingTheftInit$get_rolling_features(ohlcv)

# feasts
RollingTheftInit = RollingTheft$new(c(22*3, 22*12),
                                    workers = 8L,
                                    at = 1:nrow(ohlcv$X),
                                    lag = 0L,
                                    na_pad = TRUE,
                                    simplify = FALSE,
                                    features_set = "feasts")
RollingTheftFeastsFatures = RollingTheftInit$get_rolling_features(ohlcv)

# tsfeatures
RollingTsfeaturesInit = RollingTsfeatures$new(windows = 22 * 6,
                                              workers = 8L,
                                              at = 1:nrow(ohlcv$X),
                                              lag = 0L,
                                              na_pad = TRUE,
                                              simplify = FALSE)
RollingTsfeaturesFeatures = RollingTsfeaturesInit$get_rolling_features(ohlcv)

# Wavelet arima
RollingWaveletArimaInstance = RollingWaveletArima$new(windows = 250, 250*2,
                                                      workers = 8L,
                                                      lag = 0L,
                                                      at = 1:nrow(ohlcv$X),
                                                      na_pad = TRUE,
                                                      simplify = FALSE,
                                                      filter = "haar")
RollingWaveletArimaFeatures = RollingWaveletArimaInstance$get_rolling_features(ohlcv) # y <- WaveletArima::WaveletFittingarma(ts = na.omit(data$returns),


# OhlvFeatures
ohlcv_features = OhlcvFeatures$new(at = NULL,
                                   windows = c(5, 22, 22 * 3, 22 * 6, 22 * 12),
                                   quantile_divergence_window = c(22, 22 * 6, 22 * 12, 22 * 12 * 2))
ohlcv_feature_set = ohlcv_features$get_ohlcv_features(ohlcv)

# merge finfeatures features
features <- Reduce(function(x, y) merge(x, y, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE),
                   list(ohlcv_feature_set,
                        exuber_features,
                        autoarima_features,
                        RollingBidAskFeatures,
                        RollingTheftCatch22Features,
                        RollingTheftFeastsFatures,
                        RollingTsfeaturesFeatures
                        # RollingWaveletArimaFeatures
                        ))
features[, symbol := NULL]
features <- features[, .SD, .SDcols = c(1, 8:ncol(features))]

# merge all data
mcap_52_indicator_ <- melt(mcap_52_indicator, id.vars = c("date", "size"))
mcap_52_indicator_[, var := paste0(size, "_", variable)]
mcap_52_indicator_ <- dcast(mcap_52_indicator_, date ~ var, value.var = "value")
predictors <- Reduce(function(x, y) merge(x, y, by = "date", all.x = TRUE, all.y = FALSE),
                     list(spy, vol_52_indicators, mcap_52_indicator_,
                          fred_daily[, .SD, .SDcols = c("date", fred_predictors)],
                          bonds[, .(date, spread)], features))
predictors[, `:=`(down = NULL, up = NULL)]

# labels for feature importance and ML
predictors[, ret_1 := shift(close, -1L, "shift") / close  - 1]
# DT[, ret_5 := shift(close, -4L, "shift") / close - 1]



# PREPROCESSING (NO TUNING) -----------------------------------------------
# keep omly after 2004
# predictors <- predictors[date > as.Date("2004-01-01")]

# na analysis
naniar::vis_miss(predictors[, .SD, .SDcols = colnames(predictors)[grep("exuber", colnames(predictors))]])
naniar::vis_miss(predictors[, .SD, .SDcols = colnames(predictors)[grep("arima", colnames(predictors))]])
naniar::vis_miss(predictors[, .SD, .SDcols = fred_predictors])

# remove columns with many NA
# naniar::vis_miss(predictors[, .SD, .SDcols = c("date", feature_cols)])
keep_cols <- names(which(colMeans(!is.na(predictors)) > 0.85))
print(paste0("Removing columns with many NA values: ", setdiff(colnames(predictors), keep_cols)))
predictors <- predictors[, .SD, .SDcols = keep_cols]

# remove Inf and Nan values if they exists
is.infinite.data.frame <- function(x) do.call(cbind, lapply(x, is.infinite))
keep_cols <- names(which(colMeans(!is.infinite(as.data.frame(predictors))) > 0.9999999))
print(paste0("Removing columns with Inf values: ", setdiff(colnames(predictors), keep_cols)))
predictors <- predictors[, .SD, .SDcols = keep_cols]

# remove inf values
predictors <- predictors[is.finite(rowSums(predictors[, .SD, .SDcols = is.numeric], na.rm = TRUE))] # remove inf values

# define feature columns and LABEL
LABEL = "ret_1"
first_feature_name <- which(colnames(predictors) == "volume_down_up_ratio")
last_feature_name <- ncol(predictors) - 1
feature_cols <- colnames(predictors)[first_feature_name:last_feature_name]

# remove NA values
s <- nrow(predictors)
predictors <- na.omit(predictors, cols = feature_cols)
e <- nrow(predictors)
cat("Remove", -(e - s), "missing values.")

# remove constant columns
remove_cols <- feature_cols[apply(predictors[, ..feature_cols], 2, var, na.rm=TRUE) == 0]
print(paste0("Removing feature with 0 standard deviation: ", remove_cols))
feature_cols <- setdiff(feature_cols, remove_cols)

#  winsorization (remove outliers)
# TODO: Move this to mlr3 pipeline !
predictors[, (feature_cols) := lapply(.SD, Winsorize, probs = c(0.001, 0.999), na.rm = TRUE), .SDcols = feature_cols]

# remove constant columns
remove_cols <- feature_cols[apply(predictors[, ..feature_cols], 2, var, na.rm=TRUE) == 0]
print(paste0("Removing feature with 0 standard deviation: ", remove_cols))
feature_cols <- setdiff(feature_cols, remove_cols)

# remove highly correlated features
cor_matrix <- cor(predictors[, ..feature_cols])
cor_matrix_rm <- cor_matrix
cor_matrix_rm[upper.tri(cor_matrix_rm)] <- 0
diag(cor_matrix_rm) <- 0
remove_cols <- feature_cols[apply(cor_matrix_rm, 2, function(x) any(abs(x) > 0.97))]
print(paste0("Removing highly correlated featue (> 0.98): ", remove_cols))
feature_cols <- setdiff(feature_cols, remove_cols)

# # add interactions
# interactions <- model.matrix( ~ .^2 - 1, data = predictors[, ..feature_cols])
# cols_keep <- c("date", "close", "returns", LABEL)
# predictors <- cbind(predictors[, ..cols_keep], as.data.table(interactions))
# colnames(predictors) <- gsub(":", "_", colnames(predictors))
# features_cols_inter <- colnames(predictors)[which(colnames(predictors) == "volume_down_up_ratio"):ncol(predictors)]
#
# # remove constant columns
# remove_cols <- features_cols_inter[apply(predictors[, ..features_cols_inter], 2, var, na.rm=TRUE) == 0]
# print(paste0("Removing feature with 0 standard deviation: ", remove_cols))
# features_cols_inter <- setdiff(features_cols_inter, remove_cols)
#
# # remove highly correlated features
# system.time(cor_matrix <- cor(predictors[, ..features_cols_inter])) # 6.8 min
# cor_matrix <- cor(predictors[, ..features_cols_inter])
# cor_matrix_rm <- cor_matrix
# cor_matrix_rm[upper.tri(cor_matrix_rm)] <- 0
# diag(cor_matrix_rm) <- 0
# remove_cols <- features_cols_inter[apply(cor_matrix_rm, 2, function(x) any(abs(x) > 0.96))]
# print(paste0("Removing highly correlated featue (> 0.97): ", remove_cols))
# features_cols_inter <- setdiff(features_cols_inter, remove_cols)
#
# # create lags
# rpv  <- rep(1, each=length(features_cols_inter))
# DT[, paste(features_cols_inter, "lag", rpv, sep="_") := Map(shift, .SD, rpv), .SDcols=features_cols_inter]
# DT <- na.omit(DT)
# features_cols_inter <- colnames(DT)[which(colnames(DT) == "VIXCLS"):ncol(DT)]
#
# # remove highly correlated features
# cor_matrix <- cor(DT[, ..features_cols_inter])
# cor_matrix_rm <- cor_matrix
# cor_matrix_rm[upper.tri(cor_matrix_rm)] <- 0
# diag(cor_matrix_rm) <- 0
# remove_cols <- features_cols_inter[apply(cor_matrix_rm, 2, function(x) any(abs(x) > 0.98))]
# print(paste0("Removing highly correlated featue (> 0.98): ", remove_cols))
# features_cols_inter <- setdiff(features_cols_inter, remove_cols)


############ FRAC DIFF ##############

# frac diff
library(fracdiff)
diff_times <- lapply(predictors[, ..feature_cols],
                     function(x) fdGPH(x, bandw.exp = 0.5)$d)
diff_times_vec <- unlist(diff_times)
# diff_times_vec <- diff_times_vec[diff_times_vec > 0]
predictors_stat <- copy(predictors)
predictors_stat[ , (names(diff_times_vec)) := lapply(seq_along(.SD), function(x) diffseries(.SD[[x]], diff_times_vec[x])),
                 .SDcols = names(diff_times_vec)]

# remove columns close to constant
cols_keep <- c(feature_cols, LABEL)
task = TaskRegr$new("example", predictors_stat[, ..cols_keep], target = "ret_1")
po = po("removeconstants", ratio = 0.1)
predictors_stat_ <- po$train(list(task = task))[[1]]$data()
dim(predictors_stat)
dim(predictors_stat_)
predictors_vector <- setdiff(colnames(predictors_stat_), LABEL)



# FEATURE SELECTION -------------------------------------------------------
# importnat features
# LAG!!!!!!!!!!!!!!!!!
cols_keep <- c(predictors_vector, LABEL)
X <- na.omit(predictors_stat[, ..cols_keep])
X <- as.matrix(X)

# f1st
f1st_fi <- f1st(X[, ncol(X)], X[, -ncol(X)], kmn = 10)
cov_index_f1st <- colnames(X[, -ncol(X)])[f1st_fi[[1]][, 1]]

# f3st_1
f3st_1 <- f3st(X[, ncol(X)], X[, -ncol(X)], kmn = 10, m = 1)
cov_index_f3st_1 <- unique(as.integer(f3st_1[[1]][1, ]))[-1]
cov_index_f3st_1 <- cov_index_f3st_1[cov_index_f3st_1 != 0]
cov_index_f3st_1 <- colnames(X[, -ncol(X)])[cov_index_f3st_1]

# f3st_1 m=2
f3st_2 <- f3st(X[, ncol(X)], X[, -ncol(X)], kmn = 10, m = 2)
cov_index_f3st_2 <- unique(as.integer(f3st_2[[1]][1, ]))[-1]
cov_index_f3st_2 <- cov_index_f3st_2[cov_index_f3st_2 != 0]
cov_index_f3st_2 <- colnames(X[, -ncol(X)])[cov_index_f3st_2]

# f3st_1 m=3
f3st_3 <- f3st(X[, ncol(X)], X[, -ncol(X)], kmn = 10, m = 3)
cov_index_f3st_3 <- unique(as.integer(f3st_3[[1]][1, ]))[-1]
cov_index_f3st_3 <- cov_index_f3st_3[cov_index_f3st_3 != 0]
cov_index_f3st_3 <- colnames(X[, -ncol(X)])[cov_index_f3st_3]

# inspect
plot(predictors_stat[, catch22_CO_trev_1_num_528])
plot(roll::roll_sd(predictors_stat[, catch22_CO_trev_1_num_528], 22))

# interesection of all important vars
# most_important_vars <- Reduce(intersect, list(cov_index_f1st, cov_index_f3st_1, cov_index_f3st_2))
most_important_vars <- unique(c(cov_index_f1st[1:4], cov_index_f3st_1[1:4], cov_index_f3st_2[1:4]))
important_vars <- unique(c(cov_index_f1st, cov_index_f3st_1, cov_index_f3st_2))

# save important variables to blob to blob
bl_endp_key <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
cont = storage_container(bl_endp_key, "systemic-risk")
storage_write_csv(data.frame(cov_index_f1st = cov_index_f1st), cont, "cov_index_f1st_v7.csv")
storage_write_csv(data.frame(cov_index_f3st_1 = cov_index_f3st_1), cont, "cov_index_f3st_1_v7.csv")

# # train test holdout split
# start_holdout_date <- as.Date("2021-01-01")
# holdout_ids <- which(DT$date > start_holdout_date)
# X_model <- DT[-holdout_ids, ]
# X_holdout <- DT[holdout_ids, ] # TODO SAVE THIS FOR QUANTCONNECT BACKTESTING
#
# # select only labels and features
# X_model <- X_model[, .SD, .SDcols = c("date", features_cols_inter, "returns")]
# X_holdout <- X_holdout[, .SD, .SDcols = c("date", features_cols_inter, "returns")]
#
# # train data
# cols_keep <- c("date", "returns", most_important_vars)
# X_train <- X_model[, ..cols_keep]
# X_train <- na.omit(X_train)
# X_test <- na.omit(X_holdout[, ..cols_keep])


# MODELING ----------------------------------------------------------------
# we don't need holdout split
chosen_predictors <- cov_index_f3st_3
cols_keep <- c("date", "returns", chosen_predictors)
predictors_short <- na.omit(predictors_stat[, ..cols_keep])
X <- as.data.frame(predictors_short[, 2:ncol(predictors_short)])

# parameters
window_lengths <- c(252, 252 * 2, 252 * 4)

# VAR
cl <- makeCluster(4L)
clusterExport(cl, "X", envir = environment())
clusterEvalQ(cl,{library(vars); library(BigVAR); library(tsDyn)})
roll_preds <- lapply(window_lengths, function(win) {
  runner(
    x = X,
    f = function(x) {
      # x <- X_train[1:300, 1:ncol(X_train)]
      # y <- as.data.frame(x[, 2:ncol(x)])
      y <- x[, which(apply(x, 2 , sd) != 0), drop = FALSE]
      if(length(y) == 1) print("STOP!")

      # VAR
      res <- VAR(y, lag.max = 10, type = "both")
      p <- predict(res)
      var_pred_onestep <- p$fcst[[1]][1, 1]

      # BigVar
      Y <- as.matrix(y)
      B = BigVAR.fit(Y, struct='Basic', p=10, lambda=1)[,,1]
      Z = VARXLagCons(Y, p=10, oos=TRUE)$Z
      yhat = B %*% Z[,ncol(Z),drop=FALSE]
      bigvar_pred_onestep <- yhat[1]

      # TVAR (1)
      tv1 <- tryCatch(TVAR(y, lag=10, nthresh=1, thDelay=1, trim=0.05, mTh=1, plot=FALSE),
                      error = function(e) NULL)
      if (is.null(tv1)) {
        tv1_pred_onestep <- NA
      } else {
        tv1 <- predict(tv1)
        tv1_pred_onestep <- tv1[1, 1]
      }

      # TVAR (2)
      # tv2 <- tryCatch(TVAR(y, lag=10, nthresh=2, thDelay=1, trim=0.1, mTh=1, plot=FALSE), error = function(e) NULL)
      # if (is.null(tv2)) {
      #   tv2_pred_onestep <- NA
      # } else {
      #   tv2 <- predict(tv2)
      #   tv2_pred_onestep <- tv2[1, 1]
      # }

      # merge all predictions
      data.frame(var_pred_onestep = var_pred_onestep,
                 bigvar_pred_onestep = bigvar_pred_onestep,
                 tv1_pred_onestep = tv1_pred_onestep)
                 # tv2_pred_onestep = tv2_pred_onestep)
    },
    k = win,
    lag = 0L,
    na_pad = TRUE,
    cl = cl
  )
})
parallel::stopCluster(cl)

# clean predicitons
roll_preds_merged <- lapply(roll_preds, function(x) lapply(x, as.data.table))
roll_preds_merged <- lapply(roll_preds_merged, rbindlist, fill = TRUE)
lapply(roll_preds_merged, function(x) x[, V1 := NULL])
roll_preds_merged <- lapply(seq_along(roll_preds_merged), function(i) {
  colnames(roll_preds_merged[[i]]) <- paste0(colnames(roll_preds_merged[[i]]), "_", window_lengths[i])
  roll_preds_merged[[i]]
})
roll_preds_merged <- as.data.table(do.call(cbind, roll_preds_merged))
roll_preds_merged <- roll_preds_merged[, lapply(.SD, unlist)]
roll_preds_merged$mean_pred <- apply(roll_preds_merged, 1, function(x) mean(x, na.rm = TRUE))
roll_preds_merged$median_pred <- apply(roll_preds_merged, 1, function(x) median(x, na.rm = TRUE))
roll_preds_merged$sd_pred <- apply(roll_preds_merged, 1, function(x) sd(x, na.rm = TRUE))
roll_preds_merged$sum_pred <- apply(roll_preds_merged, 1, function(x) sum(x))
roll_preds_merged$verylow_pred <- apply(roll_preds_merged, 1, function(x) sum(x < -0.001))
predictions_var <- cbind(date = predictors_short$date, roll_preds_merged)



# BACKTEST ----------------------------------------------------------------
# backtest apply
backtest <- function(returns, indicator, threshold, return_cumulative = TRUE) {
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1])) {
      sides[i] <- NA
    } else if (indicator[i-1] < threshold) { #  & indicator_2[i-1] > 1
      sides[i] <- 0
    } else {
      sides[i] <- 1
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
Performance <- function(x) {
  cumRetx = Return.cumulative(x)
  annRetx = Return.annualized(x, scale=252 * 8)
  sharpex = SharpeRatio.annualized(x, scale=252 * 8)
  winpctx = length(x[x > 0])/length(x[x != 0])
  annSDx = sd.annualized(x, scale=252 * 8)

  DDs <- findDrawdowns(x)
  maxDDx = min(DDs$return)
  maxLx = max(DDs$length)

  Perf = c(cumRetx, annRetx, sharpex, winpctx, annSDx, maxDDx, maxLx)
  names(Perf) = c("Cumulative Return", "Annual Return","Annualized Sharpe Ratio",
                  "Win %", "Annualized Volatility", "Maximum Drawdown", "Max Length Drawdown")
  return(Perf)
}

# handle missing values
backtest_data <- merge(spy, predictions_var, by = "date", all.x = TRUE, all.y = FALSE)
# naniar::vis_miss(backtest_data)
backtest_data <- na.omit(backtest_data)
# naniar::vis_miss(backtest_data)

# get CAR's for all strategies
Return.cumulative(backtest_data$returns)
backtest_results <- lapply(backtest_data[, 4:ncol(backtest_data)], function(x) backtest(backtest_data$returns, x, -0.01))
backtest_results <- as.data.table(backtest_results)
backtest_results

# individual backtests
x <- backtest(backtest_data$returns, backtest_data$var_pred_onestep_1008, -0.01, FALSE)
backtest_results_ind <- as.xts(cbind(SPY = backtest_data$returns, Strategy = x), order.by = as.Date(backtest_data$date))
charts.PerformanceSummary(backtest_results_ind)
Performance(backtest_results_ind$SPY)
Performance(backtest_results_ind$Strategy)
# subsample
charts.PerformanceSummary(as.xts(cbind(backtest_data$returns, x), order.by = as.Date(backtest_data$date))[1:1000])
charts.PerformanceSummary(as.xts(cbind(backtest_data$returns, x), order.by = as.Date(backtest_data$date))[1000:2000])
charts.PerformanceSummary(as.xts(cbind(backtest_data$returns, x), order.by = as.Date(backtest_data$date))[2000:3000])
charts.PerformanceSummary(as.xts(cbind(backtest_data$returns, x), order.by = as.Date(backtest_data$date))[3000:3500])
charts.PerformanceSummary(as.xts(cbind(backtest_data$returns, x), order.by = as.Date(backtest_data$date))[3500:4000])
charts.PerformanceSummary(as.xts(cbind(backtest_data$returns, x), order.by = as.Date(backtest_data$date))[4000:4500])
charts.PerformanceSummary(as.xts(cbind(backtest_data$returns, x), order.by = as.Date(backtest_data$date))[4500:4660])

# save data to blob
predictions <- na.omit(predictions_var)
predictions <- unique(predictions, by = "date")
setorderv(predictions, "date")
qc_backtest <- copy(predictions)
# cols <- setdiff(cols_keep, "date")
# qc_backtest[, (cols) := lapply(.SD, shift), .SDcols = cols] # VERY IMPORTANT STEP !
file_name <- "D:/risks/pr/systemic_risk_v8.csv"
fwrite(qc_backtest, file_name, col.names = FALSE, dateTimeAs = "write.csv")
bl_endp_key <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
cont <- storage_container(bl_endp_key, "qc-backtest")
storage_upload(cont, file_name, basename(file_name))

# save data to blob for second case - we buy in after market
qc_backtest_after_market <- copy(qc_backtest)
qc_backtest_after_market[, date := paste(date, "23:00")]
file_name <- "D:/risks/pr/systemic_risk_afterhour.csv"
fwrite(qc_backtest_after_market, file_name, col.names = FALSE, dateTimeAs = "write.csv")
bl_endp_key <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
cont <- storage_container(bl_endp_key, "qc-backtest")
storage_upload(cont, file_name, basename(file_name))

"LOOK AHEAD BIAS"
