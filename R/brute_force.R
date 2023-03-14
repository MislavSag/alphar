library(data.table)
library(tiledb)
library(checkmate)
library(fredr)
library(findata)
library(finfeatures)
library(moments)
library(gausscov)
library(ggplot2)
library(DescTools)



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

# date segments
GFC <- c("2007-01-01", "2010-01-01")
COVID <- c("2020-01-01", "2021-06-01")
AFTER_COVID <- c("2021-06-01", "2022-01-01")
NEW <- c("2022-01-01", as.character(Sys.Date()))


# parameters
# start_holdout_date = as.Date("2021-06-01") # observations after this date belongs to holdout set



# IMPORT DATA -------------------------------------------------------------
# define symbols
fmp = FMP$new()
symbols <- fmp$get_sp500_symbols()
symbols <- na.omit(symbols)
symbols <- symbols[!grepl("/", symbols)]
symbols <- c("SPY", symbols)

# import daily data
arr <- tiledb_array("D:/equity-usa-daily-fmp",
                    as.data.frame = TRUE,
                    query_layout = "UNORDERED"
)
system.time(prices <- arr[])
tiledb_array_close(arr)
prices_dt <- as.data.table(prices)
prices_dt <- prices_dt[symbol %in% symbols]
prices_dt <- unique(prices_dt, by = c("symbol", "date"))
setorder(prices_dt, symbol, date)
prices_dt[symbol == "SPY"]

# free resources
rm(prices)
gc()

# calculate ohlcv predictors
prices_dt <- prices_dt[open > 0 & high > 0 & low > 0 & close > 0 & adjClose > 0] # remove rows with zero and negative prices
adjust_cols <- c("open", "high", "low")
prices_dt[, (adjust_cols) := lapply(.SD, function(x) x * (adjClose / close)), .SDcols = adjust_cols] # adjust open, high and low prices
prices_dt[, close := adjClose]
prices_dt <- na.omit(prices_dt[, .(symbol, date, open, high, low, close, volume)])
prices_n <- prices_dt[, .N, by = symbol]
prices_n <- prices_n[N > 700]  # remove prices with only 700 or less observations
prices_dt <- prices_dt[symbol %in% prices_n$symbol]

# calculate predictors
ohlcv = Ohlcv$new(X = prices_dt)
OhlcvFeaturesInit = OhlcvFeatures$new(at = NULL,
                                      windows = c(5, 10, 22, 22 * 3, 22 * 6, 22 * 12, 22 * 12 * 2),
                                      quantile_divergence_window =  c(22, 22*3, 22*6, 22*12, 22*12*2))
OhlcvFeaturesSet = OhlcvFeaturesInit$get_ohlcv_features(ohlcv)
dim(OhlcvFeaturesSet)

# theft catch22 features
# print("Calculate Catch22 predictors")
# RollingTheftInit = RollingTheft$new(windows = c(22, 22 * 3, 22 * 12, 22 * 12 * 2),
#                                     workers = 4L,
#                                     at = 1:nrow(ohlcv$X),
#                                     lag = 0L,
#                                     features_set = "catch22")
# RollingTheftCatch22Features = RollingTheftInit$get_rolling_features(ohlcv)
# gc()


# predictors column nam vector
cols_features <- colnames(OhlcvFeaturesSet)[9:ncol(OhlcvFeaturesSet)]

# convert columns to numeric. This is important only if we import existing features
DT <- copy(OhlcvFeaturesSet)
chr_to_num_cols <- setdiff(colnames(DT[, .SD, .SDcols = is.character]), c("symbol", "date"))
DT <- DT[, (chr_to_num_cols) := lapply(.SD, as.numeric), .SDcols = chr_to_num_cols]
int_to_num_cols <- colnames(DT[, ..cols_features][, .SD, .SDcols = is.integer])
DT <- DT[, (int_to_num_cols) := lapply(.SD, as.numeric), .SDcols = int_to_num_cols]

# remove columns with many NA
keep_cols <- names(which(colMeans(!is.na(DT)) > 0.9))
print(paste0("Removing columns with many NA values: ", setdiff(colnames(DT), c(keep_cols, "right_time"))))
DT <- DT[, .SD, .SDcols = keep_cols]

# remove Inf and Nan values if they exists
is.infinite.data.frame <- function(x) do.call(cbind, lapply(x, is.infinite))
keep_cols <- names(which(colMeans(!is.infinite(as.data.frame(DT))) > 0.99))
print(paste0("Removing columns with Inf values: ", setdiff(colnames(DT), keep_cols)))
DT <- DT[, .SD, .SDcols = keep_cols]

# remove inf values
n_0 <- nrow(DT)
DT <- DT[is.finite(rowSums(DT[, .SD, .SDcols = is.numeric], na.rm = TRUE))]
n_1 <- nrow(DT)
print(paste0("Removing ", n_0 - n_1, " rows because of Inf values"))

# define feature columns
first_feature_name <- which(colnames(DT) == "returns") + 1
feature_cols <- colnames(DT)[first_feature_name:ncol(DT)]

# # remove NA values
# n_0 <- nrow(DT)
# DT <- na.omit(DT, cols = feature_cols)
# n_1 <- nrow(DT)
# print(paste0("Removing ", n_0 - n_1, " rows because of NA values"))

# remove constant columns
# features_ <- DT[, ..feature_cols]
# remove_cols <- colnames(features_)[apply(features_, 2, var, na.rm = TRUE) == 0]
# print(paste0("Removing feature with 0 standard deviation: ", remove_cols))
# feature_cols <- setdiff(feature_cols, remove_cols)

#  winsorization (remove ooutliers)
DT[, (feature_cols) := lapply(.SD, Winsorize, probs = c(0.001, 0.999), na.rm = TRUE), .SDcols = feature_cols, by = symbol]

# remove constant columns
# TODO: Move this to mlr3 pipeline !
features_ <- DT[, ..feature_cols]
remove_cols <- colnames(features_)[apply(features_, 2, var, na.rm=TRUE) == 0]
print(paste0("Removing feature with 0 standard deviation: ", remove_cols))
feature_cols <- setdiff(feature_cols, remove_cols)

# define indicators based on ohlcv
predictors <- colnames(DT)[9:ncol(DT)]
indicators_median <- DT[, lapply(.SD, median, na.rm = TRUE), by = c('date'), .SDcols = predictors]
colnames(indicators_median)[2:ncol(indicators_median)] <- paste0("median_", colnames(indicators_median)[2:ncol(indicators_median)])
indicators_sd <- DT[, lapply(.SD, sd, na.rm = TRUE), by = c('date'), .SDcols = predictors]
colnames(indicators_sd)[2:ncol(indicators_sd)] <- paste0("sd_", colnames(indicators_sd)[2:ncol(indicators_sd)])
indicators_mean <- DT[, lapply(.SD, mean, na.rm = TRUE), by = c('date'), .SDcols = predictors]
colnames(indicators_mean)[2:ncol(indicators_mean)] <- paste0("mean_", colnames(indicators_mean)[2:ncol(indicators_mean)])
indicators_q99 <- DT[, lapply(.SD, quantile, probs = 0.99, na.rm = TRUE), by = c('date'), .SDcols = predictors]
colnames(indicators_q99)[2:ncol(indicators_q99)] <- paste0("q99_", colnames(indicators_q99)[2:ncol(indicators_q99)])
indicators_q97 <- DT[, lapply(.SD, quantile, probs = 0.97, na.rm = TRUE), by = c('date'), .SDcols = predictors]
colnames(indicators_q97)[2:ncol(indicators_q97)] <- paste0("q97_", colnames(indicators_q97)[2:ncol(indicators_q97)])
indicators_q95 <- DT[, lapply(.SD, quantile, probs = 0.95, na.rm = TRUE), by = c('date'), .SDcols = predictors]
colnames(indicators_q95)[2:ncol(indicators_q95)] <- paste0("q95_", colnames(indicators_q95)[2:ncol(indicators_q95)])
indicators_q01 <- DT[, lapply(.SD, quantile, probs = 0.01, na.rm = TRUE), by = c('date'), .SDcols = predictors]
colnames(indicators_q01)[2:ncol(indicators_q01)] <- paste0("q01_", colnames(indicators_q01)[2:ncol(indicators_q01)])
indicators_q03 <- DT[, lapply(.SD, quantile, probs = 0.03, na.rm = TRUE), by = c('date'), .SDcols = predictors]
colnames(indicators_q03)[2:ncol(indicators_q03)] <- paste0("q03_", colnames(indicators_q03)[2:ncol(indicators_q03)])
indicators_q05 <- DT[, lapply(.SD, quantile, probs = 0.05, na.rm = TRUE), by = c('date'), .SDcols = predictors]
colnames(indicators_q05)[2:ncol(indicators_q05)] <- paste0("q05_", colnames(indicators_q05)[2:ncol(indicators_q05)])
indicators_skew <- DT[, lapply(.SD, skewness, na.rm = TRUE), by = c('date'), .SDcols = predictors]
colnames(indicators_skew)[2:ncol(indicators_skew)] <- paste0("skew_", colnames(indicators_skew)[2:ncol(indicators_skew)])
indicators_kurt <- DT[, lapply(.SD, kurtosis, na.rm = TRUE), by = c('date'), .SDcols = predictors]
colnames(indicators_kurt)[2:ncol(indicators_kurt)] <- paste0("kurt_", colnames(indicators_kurt)[2:ncol(indicators_kurt)])

# merge indicators
indicators <- Reduce(function(x, y) merge(x, y, by = "date", all.x = TRUE, all.y = FALSE),
                     list(indicators_median,
                          indicators_sd,
                          indicators_mean,
                          indicators_q99,
                          indicators_q97,
                          indicators_q95,
                          indicators_q01,
                          indicators_q03,
                          indicators_q05,
                          indicators_skew,
                          indicators_kurt))
setorderv(indicators, c("date"))
indicators <- na.omit(indicators)
dim(indicators)

# save indicators7
fwrite(indicators, "D:/features/brute_force.csv")

# save indicators7
indicators <- fread("D:/features/brute_force.csv")

# merge SPY and indicators
spy <- prices_dt[symbol == "SPY"]
dt <- merge(spy, indicators, by = 'date', all.x = TRUE, all.y = FALSE)

# labels
dt[, ret_1 := shift(close, -1L, "shift") / close - 1, by = "symbol"]
dt[, ret_5 := shift(close, -5L, "shift") / close - 1, by = "symbol"]
LABEL = "ret_1"

# define feature columns
feature_cols <- colnames(dt)[8:(ncol(dt) - 2)]

# check for missing values
naniar::vis_miss(dt[, 1:10])
keep_cols <- names(which(colMeans(!is.na(dt)) > 0.8))
print(paste0("Removing columns with many NA values: ", setdiff(colnames(dt), c(keep_cols, "right_time"))))
DT <- DT[, .SD, .SDcols = keep_cols]



# FEATURE SLECTION --------------------------------------------------------
# define feature matrix
cols_keep <- c(feature_cols, LABEL)
X <- dt[, ..cols_keep]
X <- na.omit(X)
X <- as.matrix(X)

# f1st
f1st_fi_ <- f1st(X[, ncol(X)], X[, -ncol(X)], kmn = 20, sub = TRUE)
cov_index_f1st_ <- colnames(X[, -ncol(X)])[f1st_fi_[[1]][, 1]]

# f3st_1
f3st_1_ <- f3st(X[, ncol(X)], X[, -ncol(X)], kmn = 20, m = 1)
cov_index_f3st_1 <- unique(as.integer(f3st_1_[[1]][1, ]))[-1]
cov_index_f3st_1 <- cov_index_f3st_1[cov_index_f3st_1 != 0]
cov_index_f3st_1 <- colnames(X[, -ncol(X)])[cov_index_f3st_1]

# f3st_1 m=2
f3st_2_ <- f3st(X[, ncol(X)], X[, -ncol(X)], kmn = 20, m = 2)
cov_index_f3st_2 <- unique(as.integer(f3st_2_[[1]][1, ]))[-1]
cov_index_f3st_2 <- cov_index_f3st_2[cov_index_f3st_2 != 0]
cov_index_f3st_2 <- colnames(X[, -ncol(X)])[cov_index_f3st_2]


# plots of most important variables
ggplot(dt, aes(x = date)) +
  geom_line(aes(y = close / 10)) +
  geom_line(aes(y = q99_aroon_aroonUp_22))
ggplot(dt[date %between% GFC], aes(x = date)) +
  geom_line(aes(y = close / 10)) +
  geom_line(aes(y = q99_aroon_aroonUp_22))
ggplot(dt[date %between% COVID], aes(x = date)) +
  geom_line(aes(y = close / 1000)) +
  geom_line(aes(y = q99_q99_close_divergence_22))
ggplot(dt[date %between% NEW], aes(x = date)) +
  geom_line(aes(y = close / 10)) +
  geom_line(aes(y = q99_aroon_aroonUp_22))


# remove NA values
naniar::vis_miss(dt[, 1:10])
naniar::vis_miss(dt[, 10:30])
naniar::vis_miss(dt[, 200:220])
naniar::vis_miss(dt[, 500:520])
naniar::vis_miss(indicators[, 200:220])
naniar::vis_miss(spy)

all(OhlcvFeaturesSet[, unique(date)] %in% spy[, date])
all(spy[, date] %in% OhlcvFeaturesSet[, unique(date)])

all(DT[, unique(date)] %in% spy[, date])
all(spy[, date] %in% DT[, unique(date)])

# remove highly correlated features
# TODO move this to mlr3 pipeline !
features_ <- dt[, ..feature_cols]
cor_matrix <- cor(features_)
cor_matrix_rm <- cor_matrix
cor_matrix_rm[upper.tri(cor_matrix_rm)] <- 0
diag(cor_matrix_rm) <- 0
remove_cols <- colnames(features_)[apply(cor_matrix_rm, 2, function(x) any(x > 0.98))]
print(paste0("Removing highly correlated featue (> 0.98): ", remove_cols))
feature_cols <- setdiff(feature_cols, remove_cols)

