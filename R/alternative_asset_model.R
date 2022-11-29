library(tiledb)
library(data.table)
library(fasttime)
library(finfeatures)
library(lubridate)
library(rtsplot)
library(dataPreparation)
library(DescTools)
library(nanotime)
library(mlr3)
library(mlr3pipelines)
library(gausscov)


# setup
symbols = c("TLT", "GLD", "USO", "SPY")

# configure s3
config <- tiledb_config()
config["vfs.s3.aws_access_key_id"] <- "AKIA43AHCLIIM6XWLNLJ"
config["vfs.s3.aws_secret_access_key"] <- "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"
config["vfs.s3.region"] <- "eu-central-1"
context_with_config <- tiledb_ctx(config)

# read data
arr <- tiledb_array("s3://equity-usa-hour-fmp-adjusted", as.data.frame = TRUE)
selected_ranges(arr) <- list(symbol = cbind(symbols, symbols))
system.time(market_data <- arr[])
market_data <- as.data.table(market_data)
market_data[, date := as.nanotime(date, tz = "UTC")]
market_data[, date := as.POSIXct(date, tz = "UTC")]
market_data[, date := with_tz(date, "America/New_York")]
market_data <- market_data[format.POSIXct(date, format = "%H:%M:%S") %between% c("09:30:00", "16:00:00")]
market_data <- unique(market_data, by = c("symbol", "date"))
setorder(market_data, symbol, date)

# convert minute data to hour
hour_data <- market_data[, .(open = head(open, 1),
                         high = max(high, na.rm = TRUE),
                         low = min(low, na.rm = TRUE),
                         close = tail(close, 1),
                         volume = sum(volume, na.rm = TRUE)),
                     by = .(symbol, date = ceiling_date(date, "hour"))]

# visualize
rtsplot(as.xts.data.table(hour_data[symbol == "SPY", .(date, close)]))
rtsplot(as.xts.data.table(hour_data[symbol == "GLD", .(date, close)]))
rtsplot(as.xts.data.table(hour_data[symbol == "TLT", .(date, close)]))
rtsplot(as.xts.data.table(hour_data[symbol == "USO", .(date, close)]))

# remove outliers
# calcualte ohlc returns
cols <- c("open", "high", "low", "close")
hour_data[, (paste0(cols, "_returns")) := lapply(.SD, function(x) (x / shift(x)) - 1), .SDcols = cols, by = symbol]
hour_data <- na.omit(hour_data)
remove_sd_outlier_recurse <- function(df, ...) {

  # remove outliers
  x_ <- remove_sd_outlier(data_set = df, ...)
  while (nrow(x_) < nrow(df)) {
    df <- x_
    x_ <- remove_sd_outlier(data_set = df, ...)
  }
  return(df)
}
hour_data <-  hour_data[, remove_sd_outlier_recurse(df = .SD, cols = c("high_returns", "high", "low_returns", "low"), n_sigmas = 4),
                        by = symbol]

# calculate predictors
ohlcv <- Ohlcv$new(hour_data)
lag_ <- 0L
at_ <- 1:nrow(ohlcv$X)

# OhlvFeatures
ohlcv_features = OhlcvFeatures$new(at = NULL,
                                   windows = c(5, 22, 22 * 3, 22 * 6, 22 * 12),
                                   quantile_divergence_window = c(22, 22 * 6, 22 * 12, 22 * 12 * 2))
ohlcv_feature_set = ohlcv_features$get_ohlcv_features(ohlcv)

# exuber
RollingExuberInit = RollingExuber$new(c(100, 200, 600), 8L, at = at_, lag = lag_,
                                      na_pad = TRUE, simplify = FALSE,
                                      exuber_lag = 1L)
RollingExuberFeatures = RollingExuberInit$get_rolling_features(ohlcv)

# ARIMA
RollArimaInit <- RollingForecats$new(windows = c(250L),
                                     workers = 8L,
                                     lag = lag_,
                                     at = 1:nrow(ohlcv$X),
                                     na_pad = TRUE,
                                     simplify = FALSE,
                                     forecast_type = "autoarima",
                                     h = 10)
RollArimaFeatures <- RollArimaInit$get_rolling_features(ohlcv)

# BidAsk features
RollingBidAskInstance <- RollingBidAsk$new(c(30, 60), 4L, at = at_, lag = 1, na_pad = TRUE, simplify = FALSE)
RollingBidAskFeatures = RollingBidAskInstance$get_rolling_features(ohlcv)

# Rcatch22
RollingTheftInit = RollingTheft$new(c(60 * 4, 60 * 8), workers = 4L, at = at_, lag = lag_, na_pad = TRUE, simplify = FALSE,
                                    features_set = "catch22")
RollingTheftCatch22Features = RollingTheftInit$get_rolling_features(ohlcv)

# feasts
RollingTheftInit = RollingTheft$new(c(60 * 6, 60 * 8 * 5), 4L, at = at_, lag = lag_, na_pad = TRUE, simplify = FALSE,
                                    features_set = "feasts")
RollingTheftFeastsFatures = RollingTheftInit$get_rolling_features(ohlcv)

#  tsfel
RollingTheftInit = RollingTheft$new(windows = c(60 * 4), workers = 1L, at = at_, lag = lag_, na_pad = TRUE, simplify = FALSE,
                                    features_set = "tsfel")
RollingTheftTsfelFeatures = suppressMessages(RollingTheftInit$get_rolling_features(ohlcv))

# tsfeatures
RollingTsfeaturesInit = RollingTsfeatures$new(windows = 60 * 6, workers = 8L, at = at_, lag = lag_, na_pad = TRUE, simplify = FALSE)
RollingTsfeaturesFeatures = RollingTsfeaturesInit$get_rolling_features(ohlcv)

# quarks
RollingQuarksInit = RollingQuarks$new(windows = 200,
                                      workers = 4L,
                                      at = at_,
                                      lag = lag_,
                                      na_pad = TRUE,
                                      simplify = FALSE,
                                      model = c("EWMA", "GARCH"),
                                      method = c("plain", "age"))
RollingQuarksFeatures = RollingQuarksInit$get_rolling_features(ohlcv)



# merge features
features <- Reduce(function(x, y) merge(x, y, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE),
                   list(ohlcv_feature_set,
                        RollingExuberFeatures,
                        RollArimaFeatures,
                        RollingBidAskFeatures,
                        RollingTheftCatch22Features,
                        RollingTheftFeastsFatures,
                        RollingTsfeaturesFeatures,
                        RollingQuarksFeatures))

# save number of differences for all columns to blob
# library(AzureStor)
# bl_endp_key <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
# cont = storage_container(bl_endp_key, "features")
# file_name <- paste0("AAM-features-", format(Sys.time(), format = "%Y%m%d%H%M%S"), ".csv")
# storage_write_csv(as.data.frame(features), cont, file_name)

# import saved features
bl_endp_key <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
cont = storage_container(bl_endp_key, "features")
features <- storage_read_csv(cont, "AAM-features-20221014113736.csv")
features <- as.data.table(features)



# LABELING ----------------------------------------------------------------
# labels for feature importance and ML
features[, ret_1 := shift(close, -1L, "shift") / close  - 1, by = symbol]



# PREPROCESSING (NO TUNING) -----------------------------------------------
# remove columns with many NA
keep_cols <- names(which(colMeans(!is.na(features)) > 0.60))
print(paste0("Removing columns with many NA values: ", setdiff(colnames(features), keep_cols)))
features <- features[, .SD, .SDcols = keep_cols]

# remove Inf and Nan values if they exists
is.infinite.data.frame <- function(x) do.call(cbind, lapply(x, is.infinite))
keep_cols <- names(which(colMeans(!is.infinite(as.data.frame(features))) > 0.9999))
print(paste0("Removing columns with Inf values: ", setdiff(colnames(features), keep_cols)))
features <- features[, .SD, .SDcols = keep_cols]

# remove inf values
features <- features[is.finite(rowSums(features[, .SD, .SDcols = is.numeric], na.rm = TRUE))] # remove inf values

# define feature columns and LABEL
LABEL = "ret_1"
first_feature_name <- which(colnames(features) == "close_ath")
last_feature_name <- ncol(features) - 1
feature_cols <- colnames(features)[first_feature_name:last_feature_name]

# remove NA values
features <- na.omit(features, cols = feature_cols)

# remove constant columns
remove_cols <- feature_cols[apply(features[, ..feature_cols], 2, var, na.rm=TRUE) == 0]
print(paste0("Removing feature with 0 standard deviation: ", remove_cols))
feature_cols <- setdiff(feature_cols, remove_cols)

#  winsorization (remove outliers)
# TODO: Move this to mlr3 pipeline !
features[, (feature_cols) := lapply(.SD, Winsorize, probs = c(0.01, 0.99), na.rm = TRUE), .SDcols = feature_cols]

# remove constant columns
remove_cols <- feature_cols[apply(features[, ..feature_cols], 2, var, na.rm=TRUE) == 0]
print(paste0("Removing feature with 0 standard deviation: ", remove_cols))
feature_cols <- setdiff(feature_cols, remove_cols)

# remove highly correlated features
cor_matrix <- cor(features[, ..feature_cols])
cor_matrix_rm <- cor_matrix
cor_matrix_rm[upper.tri(cor_matrix_rm)] <- 0
diag(cor_matrix_rm) <- 0
remove_cols <- feature_cols[apply(cor_matrix_rm, 2, function(x) any(abs(x) > 0.98))]
print(paste0("Removing highly correlated featue (> 0.98): ", remove_cols))
feature_cols <- setdiff(feature_cols, remove_cols)

# # add interactions
# interactions <- model.matrix( ~ .^2 - 1, data = features[, ..feature_cols])
# cols_keep <- c("date", "close", "returns", LABEL)
# features <- cbind(DT[, ..cols_keep], interactions)
# colnames(features) <- gsub(":", "_", colnames(features))
# features_cols_inter <- colnames(features)[which(colnames(features) == "close_ath"):ncol(DT)]
#
# # remove constant columns
# remove_cols <- features_cols_inter[apply(features[, ..features_cols_inter], 2, var, na.rm=TRUE) == 0]
# print(paste0("Removing feature with 0 standard deviation: ", remove_cols))
# features_cols_inter <- setdiff(features_cols_inter, remove_cols)
#
# # remove highly correlated features
# system.time(cor_matrix <- cor(DT[, ..features_cols_inter])) # 6.8 min
# cor_matrix <- cor(DT[, ..features_cols_inter])
# cor_matrix_rm <- cor_matrix
# cor_matrix_rm[upper.tri(cor_matrix_rm)] <- 0
# diag(cor_matrix_rm) <- 0
# remove_cols <- features_cols_inter[apply(cor_matrix_rm, 2, function(x) any(abs(x) > 0.98))]
# print(paste0("Removing highly correlated featue (> 0.98): ", remove_cols))
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

# stationarity
number_differences <- lapply(features[, ..feature_cols],
                             function(x) forecast::ndiffs(x))
number_differences_cols <- number_differences[number_differences > 0]
features[, (names(number_differences_cols)) := lapply(.SD, function(x) x - shift(x)), .SDcols = names(number_differences_cols)]

# remove columns close to constant
cols_keep <- c(feature_cols, LABEL)
task = TaskRegr$new("example", features[, ..cols_keep], target = "ret_1")
po = po("removeconstants", ratio = 0.1)
DT <- po$train(list(task = task))[[1]]$data()
dim(features)
dim(DT)
feature_cols <- setdiff(colnames(DT), LABEL)



# FEATURE SELECTION -------------------------------------------------------
# define feature matrix
cols_keep <- c(feature_cols, "ret_1")
X <- DT[, ..cols_keep]
X <- na.omit(X)
X <- as.matrix(X)
dim(X)

# f1st
f1st_fi_ <- f1st(X[, ncol(X)], X[, -ncol(X)], kmn = 20, sub = TRUE)
cov_index_f1st_ <- colnames(X[, -ncol(X)])[f1st_fi_[[1]][, 1]]

# f3st_1
f3st_1_ <- f3st(X[, ncol(X)], X[, -ncol(X)], kmn = 20, m = 1)
cov_index_f3st_1_ <- unique(as.integer(f3st_1_[[1]][1, ]))[-1]
cov_index_f3st_1 <- cov_index_f3st_1[cov_index_f3st_1 != 0]
cov_index_f3st_1 <- colnames(X[, -ncol(X)])[cov_index_f3st_1]

# f3st_1 m=2
f3st_2_ <- f3st(X[, ncol(X)], X[, -ncol(X)], kmn = 20, m = 2)
cov_index_f3st_2_ <- unique(as.integer(f3st_2_[[1]][1, ]))[-1]
cov_index_f3st_2 <- cov_index_f3st_2[cov_index_f3st_2 != 0]
cov_index_f3st_2 <- colnames(X[, -ncol(X)])[cov_index_f3st_2]

# f3st_1 m=3
f3st_3_ <- f3st(X[, ncol(X)], X[, -ncol(X)], kmn = 20, m = 3)
cov_index_f3st_3_ <- unique(as.integer(f3st_3_[[1]][1, ]))[-1]
cov_index_f3st_3 <- cov_index_f3st_3[cov_index_f3st_3 != 0]
cov_index_f3st_3 <- colnames(X[, -ncol(X)])[cov_index_f3st_3]

# interesection of all important vars
most_important_vars <- intersect(cov_index_f1st, cov_index_f3st_1)
important_vars <- unique(c(cov_index_f1st, cov_index_f3st_1))

# save important vars
ivars <- list(
  most_important_vars = most_important_vars,
  important_vars = important_vars
)
saveRDS(ivars, paste0("D:/mlfin/mlr3_models/HFT-ivars-", time_, ".rds"))
# ivars <- readRDS(paste0("D:/mlfin/mlr3_models/HFT-ivars-20220821170631.rds"))
important_vars <- ivars$important_vars
most_important_vars <- ivars$most_important_vars



