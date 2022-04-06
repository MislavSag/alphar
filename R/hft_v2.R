library(data.table)
library(ggplot2)
library(AzureStor)
library(xts)
library(QuantTools)
library(mlfinance)
library(mlr3verse)
library(finautoml)
# require(finfeatures, lib.loc = "C:/Users/Mislav/Documents/GitHub/finfeatures/renv/library/R-4.1/x86_64-w64-mingw32")
library(reticulate)
# python packages
reticulate::use_python("C:/ProgramData/Anaconda3/envs/mlfinlabenv/python.exe", required = TRUE)
mlfinlab = reticulate::import("mlfinlab", convert = FALSE)
pd = reticulate::import("pandas", convert = FALSE)
builtins = import_builtins(convert = FALSE)
main = import_main(convert = FALSE)



# UTILS -------------------------------------------------------------------

check_dates <- function(x) {
  min_ <- min(format.POSIXct(as.POSIXct(rownames(x), tz = "EST"), format = "%H:%M:%S"), na.rm = TRUE)
  max_ <- max(format.POSIXct(as.POSIXct(rownames(x), tz = "EST"), format = "%H:%M:%S"), na.rm = TRUE)
  print(paste("Min ", min_, "Max", max_))
}


# SET UP AND DATA IMPORT --------------------------------------------------

# setup
BLOBKEY=Sys.getenv("BLOB-KEY")
BLOBENDPOINT=Sys.getenv("BLOB-ENDPOINT")
CONTENDPOINT=storage_endpoint(BLOBENDPOINT, BLOBKEY)
CONT=storage_container(CONTENDPOINT, "equity-usa-minute-fmpcloud")
CONTFACTOR=storage_container(CONTENDPOINT, "factor-files")
CONTFILTERS=storage_container(CONTENDPOINT, "filters-equity-minute")
CONTFEATURES=storage_container(CONTENDPOINT, "features")
CONTMODELS=storage_container(CONTENDPOINT, "mlr3models")

# parameters
symbols = c("AAPL", "AAL", "TSLA", "FB")
start_holdout_date = as.Date("2022-06-01")

# import data
azure_files <- paste0(symbols, ".csv")
market_data_l <- lapply(azure_files, function(x) cbind(symbol = gsub(".csv", "", x), storage_read_csv(container=CONT, x)))
market_data <- rbindlist(market_data_l)


# import factor files
factor_files <- lapply(tolower(azure_files), function(x) {
  y <- storage_read_csv(CONTFACTOR, x, col_names = FALSE)
  if (nrow(y) > 0) {
    y$symbol <- toupper(gsub("\\.csv", "", x))
  }
  y
})
factor_files <- rbindlist(factor_files)
setnames(factor_files, colnames(factor_files), c("date", "price_factor", "split_factor", "previous_price", "symbol"))
factor_files[, symbol := toupper(symbol)]
factor_files[, date := as.Date(as.character(date), "%Y%m%d")]

# filter only trading hours
market_data <- market_data[format.POSIXct(formated, format = "%H:%M:%S") %between% c("09:30:00", "16:00:00")]
market_data <- market_data[, .(symbol, formated, o, h, l, c, v)]
setnames(market_data, c("symbol", "datetime", "open", "high", "low", "close", "volume"))
market_data$date <- as.Date(market_data$datetime)
market_data <- unique(market_data, by = c("symbol", "datetime"))
market_data[, datetime := as.POSIXct(as.numeric(datetime), origin=as.POSIXct("1970-01-01", tz="EST"), tz="EST")]

# adjust
ohlcv <- merge(market_data, factor_files, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE)
ohlcv[, `:=`(split_factor = na.locf(split_factor, na.rm = FALSE, rev = TRUE),
             price_factor = na.locf(price_factor, na.rm = FALSE, rev = TRUE)), by = symbol]
ohlcv[, `:=`(split_factor = ifelse(is.na(split_factor), 1, split_factor),
             price_factor = ifelse(is.na(price_factor), 1, price_factor))]
cols_change <- c("open", "high", "low", "close")
ohlcv[, `:=`(split_factor = ifelse(is.na(split_factor), 1, split_factor),
             price_factor = ifelse(is.na(price_factor), 1, price_factor))][, (cols_change) := lapply(.SD, function(x) {x * price_factor * split_factor}), .SDcols = cols_change]
ohlcv <- ohlcv[, .(symbol, datetime, open, high, low, close, volume)]
ohlcv <- unique(ohlcv, by = c("symbol", "datetime"))
setorder(ohlcv, symbol, datetime)
ohlcv[, returns := close / shift(close) - 1]

# DEBUGG
# plot(ohlcv$close)


# FILTER BARS -------------------------------------------------------------


# simple and fast filters
# ohlcv[, filter_roll_sd := roll_sd_filter(close, 60 * 4, 4, 1), by = symbol]
# table(ohlcv$filter_roll_sd)

# loop through symbols, because RAM overhead is possiblle if sopy big daa to every node
symbols_ <- unique(ohlcv$symbol)
filters_l <- list()
for (i in seq_along(symbols_)) {

  # cuurent symbol
  s <- symbols_[[i]]

  # debugging
  print(s)

  # import from Azure if exists
  azure_filter_files <- list_blobs(CONTFILTERS)
  azure_filter_files$symbols <- gsub("-.*", "", azure_filter_files$name)
  if (s %in% azure_filter_files$symbols) {
    azure_filter_file <- azure_filter_files[azure_filter_files$symbols == s, "name"]
    filters <- storage_read_csv(CONTFILTERS, azure_filter_file)
    filters_l[[i]] <- filters
    next()
  }

  # sample data only for one symbol
  sample_ <- ohlcv[symbol == s]
  # sample_ <- sample_[1:1000] # for test

  # calculate backcusum
  OhlcvInst <- Ohlcv$new(sample_, date_col = "datetime")
  RollingBackcusumInit = RollingBackcusum$new(windows = c(60 * 4),
                                              workers = 8L,
                                              at = 1:nrow(sample_),
                                              lag = 0L,
                                              na_pad = TRUE,
                                              simplify = FALSE)
  BackCusmFeatures = RollingBackcusumInit$get_rolling_features(OhlcvInst)
  gc()

  # calculate exuber
  RollingExuberInit = RollingExuber$new(windows = c(400),
                                        workers = 8L,
                                        at = 1:nrow(sample_),
                                        lag = 0L,
                                        na_pad = TRUE,
                                        simplify = FALSE)
  ExuberFeatures = RollingExuberInit$get_rolling_features(OhlcvInst)
  gc()

  # merge all features
  filters <- Reduce(function(x, y) merge(x, y, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE),
                    list(BackCusmFeatures, ExuberFeatures))

  # save to list
  filters_l[[i]] <- filters


  # save to blob
  storage_write_csv(filters, CONTFILTERS, paste0(s, "-", format.POSIXct(Sys.time(),format = "%Y%m%d%H%M%S"), ".csv"))

}
filters <- rbindlist(filters_l)

# merge ohlcv and ohlcv_filters
ohlcv_filters <- merge(ohlcv, filters, by.x = c("symbol", "datetime"), by.y = c("symbol", "date"), all.x = TRUE, all.y = FALSE)
# ohlcv_filters <- na.omit(ohlcv_filters)
ohlcv_filters <- unique(ohlcv_filters, by = c("symbol", "datetime"))



# LABELING ----------------------------------------------------------------

# meta labeling
symbols_ <- unique(ohlcv$symbol)
tb_lists <- list()
ohlcv_filters[, lapply(.SD, function(x) sum(x > 0, na.rm = TRUE)), .SDcols = 10:17] # check
filter_column <- "backcusum_rejections_1_240" # 0.1 0.05 0.01 0.001
nrow(ohlcv_filters[backcusum_rejections_1_240 == TRUE])
for (i in seq_along(symbols_)) {

  # cuurent symbol
  s <- symbols_[[i]]

  # debugging
  print(s)

  # sample data only for one symbol
  sample_ <- ohlcv[symbol == s]

  # close seires for python packages
  close_series <- reticulate::r_to_py(sample_[, .(datetime, close)])
  close_series <- close_series$set_index(close_series$columns[0])
  close_series$index = pd$to_datetime(close_series$index)
  close_series = close_series$squeeze()
  close_series = close_series$tz_localize(tz='UTC')
  close_series = close_series$tz_convert(tz='EST')
  check_dates(py_to_r(close_series))

  # filter events
  events_ <- ohlcv_filters[get(filter_column) == TRUE, .SD, .SDcols = c("symbol", "datetime")]
  events_ = events_[symbol == s, .(datetime)]
  events_ = reticulate::r_to_py(events_)
  events_ = events_$set_index(events_$columns[0])
  events_ = pd$Series(index = events_$index)
  events_ = events_$tz_localize(tz='UTC')
  events_ = events_$tz_convert(tz='EST')
  events_ = events_$index
  # events_ = pd$Series(events_$index$values, index=events_$index)

  # # Compute volatility
  daily_vol = mlfinlab$util$get_daily_vol(close = close_series)

  # # Apply Symmetric CUSUM Filter and get timestamps for events
  # cusum_events = mlfinlab$filters$cusum_filter(close_series, r_to_py(daily_vol)) # OVO U PRETHODNI KORAK
  # filtered_events_py = reticulate::r_to_py(filtered_events_)                          # OVO U PRETHODNI KORAK

  # Compute vertical barrier
  vertical_barriers = mlfinlab$labeling$add_vertical_barrier(
    t_events=events_,
    close=close_series,
    num_days=3)

  # tripple barier events
  triple_barrier_events = mlfinlab$labeling$get_events(
    close=close_series,
    t_events=pd$Series(events_), # evnts
    pt_sl=r_to_py(c(1L, 1L)),
    target=daily_vol$multiply(1),
    min_ret=r_to_py(FALSE),
    num_threads=r_to_py(1L),
    vertical_barrier_times=vertical_barriers)

  # labels
  labels = mlfinlab$labeling$get_bins(triple_barrier_events, close_series)
  labels = mlfinlab$labeling$drop_labels(labels, 0.05)
  labels_summary <- py_to_r(labels)
  table(labels_summary$bin)

  # min(format.POSIXct(as.POSIXct(rownames(labels_summary), tz = "EST"), format = "%H:%M:%S"))
  # max(format.POSIXct(as.POSIXct(rownames(labels_summary), tz = "EST"), format = "%H:%M:%S"))


  # tripple barrier info
  triple_barrier_info = pd$concat(c(triple_barrier_events$t1, labels), axis=1)
  triple_barrier_info$dropna(inplace=TRUE)
  # triple_barrier_info$t1$tz_convert('US/Eastern')
  # triple_barrier_info$index$tz_convert('US/Eastern')
  triple_barrier_info <- py_to_r(triple_barrier_info)
  attr(triple_barrier_info$t1, "tzone") <- "UTC"
  attr(triple_barrier_info$t1, "tzone") <- "EST"
  triple_barrier_info$datetime <- as.POSIXct(rownames(triple_barrier_info), tz = "EST")
  rownames(triple_barrier_info) <- NULL
  triple_barrier_info <- as.data.table(triple_barrier_info)
  head(triple_barrier_info, 100)

  # DEBUG
  # min(format.POSIXct(triple_barrier_info$datetime, format = "%H:%M:%S"))
  # max(format.POSIXct(triple_barrier_info$datetime, format = "%H:%M:%S"))

  # merge market data and labels
  result <- merge(triple_barrier_info, sample_, by = "datetime", all.x = TRUE, all.y = FALSE)

  tb_lists[[i]] <- result
}
names(tb_lists) <- symbols_
ohlcv_meta <- rbindlist(tb_lists, idcol = "symbol")
ohlcv_meta$symbol <- NULL # TODO FIX THIS ABOVE
setnames(ohlcv_meta, "datetime", "date")  # TODO FIX THIS ABOVE

# plot triple barrier
sample_index <- sample(1:nrow(ohlcv_meta), 4)
gs <- list()
for (i in seq_along(sample_index)) {
  meta_ <- ohlcv_meta[sample_index[i]]
  h_lines <- ohlcv[symbol == meta_$symbol & datetime %between% c(meta_[, date], meta_[, t1])]
  h_lines <- h_lines[, pt := head(close, 1) + (head(close, 1) * meta_$trgt)]
  h_lines <- h_lines[, sl := head(close, 1) - (head(close, 1) * meta_$trgt)]
  gs[[i]] <- ggplot(h_lines, aes(x = datetime, y = close)) +
    geom_line() +
    geom_point(data = h_lines[datetime == meta_$date], aes(x = datetime, y = close), color = "red", size = 3) +
    geom_point(data = h_lines[datetime == meta_$t1], aes(x = datetime, y = close), color = "red", size = 3) +
    geom_line(data = h_lines, aes(x = datetime, y = sl), color = "red") +
    geom_line(data = h_lines, aes(x = datetime, y = pt), color = "green")
}
patchwork::wrap_plots(gs)



# FEATURES ----------------------------------------------------------------

# index for generating features
at_ <- which(ohlcv_filters$backcusum_rejections_10_240 == TRUE)
lag_ = 0

# make OHLCV instance
OhlcvInstance = Ohlcv$new(ohlcv, date_col = "datetime")

# Features from OHLLCV
print("Calculate Ohlcv features.")
OhlcvFeaturesInit = OhlcvFeatures$new(windows = c(5, 10, 22, 22 * 3, 22 * 6, 22 * 12),
                                      quantile_divergence_window =  c(22, 22*3, 22*6, 22*12))
OhlcvFeaturesSet = OhlcvFeaturesInit$get_ohlcv_features(OhlcvInstance)
cols_keep <- colnames(OhlcvFeaturesSet)[grep("changes_", colnames(OhlcvFeaturesSet))]
cols_keep <- setdiff(colnames(OhlcvFeaturesSet), cols_keep)
OhlcvFeaturesSet = OhlcvFeaturesSet[, ..cols_keep] # REMOVE THIS IN FINFEATURES PACKAGE
OhlcvFeaturesSet_sample <- OhlcvFeaturesSet[at_ - lag_]
# DEBUGGING
# tail(OhlcvFeaturesSet_sample[symbol == "AAPL", 1:5], 10) # FOR TEST, DELTE LATER+
# tail(ohlcv_meta[symbol == "AAPL", 1:5], 10) # FOR TEST, DELTE LATER+

# BidAsk features
RollingBidAskInstance <- RollingBidAsk$new(c(30, 60), 8L, at = at_, lag = lag_, na_pad = TRUE, simplify = FALSE)
RollingBidAskFeatures = RollingBidAskInstance$get_rolling_features(OhlcvInstance)
# DEBUGGING
# tail(RollingBidAskFeatures[symbol == "AAPL", 1:5], 10) # FOR TEST, DELTE LATER+
# tail(ohlcv_meta[symbol == "AAPL", 1:5], 10) # FOR TEST, DELTE LATER+
RollingBackcusumInit = RollingBackcusum$new(c(60 * 8), 8L, at = at_,lag = lag_, na_pad = TRUE, simplify = FALSE)
RollingBackCusumFeatures = RollingBackcusumInit$get_rolling_features(OhlcvInstance)
RollingExuberInit = RollingExuber$new(c(200, 600), 8L, at = at_, lag = lag_, na_pad = TRUE, simplify = FALSE, exuber_lag = 1L)
RollingExuberFeatures = RollingExuberInit$get_rolling_features(OhlcvInstance)
RollingForecatsInstance = RollingForecats$new(c(200), 8L, lag = lag_, at = at_, na_pad = TRUE, simplify = FALSE,
                                              forecast_type = "autoarima", h = 22)
RollingForecatsAutoarimaFeatures = RollingForecatsInstance$get_rolling_features(OhlcvInstance)
##################### COMP INTENSIVE #####################
RollingForecatsInstance = RollingForecats$new(c(200), 10L, lag = lag_, at = at_, na_pad = TRUE, simplify = FALSE,
                                              forecast_type = "nnetar", h = 22)
RollingForecatNnetarFeatures = RollingForecatsInstance$get_rolling_features(OhlcvInstance)
RollingGasInit = RollingGas$new(c(60 * 8), 10L, at = at_, lag = lag_, na_pad = TRUE, simplify = FALSE,
                                gas_dist = "sstd", gas_scaling = "Identity", prediction_horizont = 10)
RollingGasFeatures = RollingGasInit$get_rolling_features(OhlcvInstance)
RollingGpdInit = RollingGpd$new(c(60 * 8), 10L, at = at_, lag = lag_, na_pad = TRUE, simplify = FALSE, threshold = 0.05)
RollingGpdFeatures = RollingGpdInit$get_rolling_features(OhlcvInstance)
##################### COMP INTENSIVE #####################
# CHECK !
# c(60, 60 * 4, 60 * 8, 60 * 8 * 5) THIS WINDOW VECTOR DOESNT WORK
RollingTheftInit = RollingTheft$new(c(60 * 4, 60 * 8), workers = 8L, at = at_, lag = lag_, na_pad = TRUE, simplify = FALSE,
                                    features_set = "catch22")
RollingTheftCatch22Features = RollingTheftInit$get_rolling_features(OhlcvInstance)
RollingTheftInit = RollingTheft$new(c(60 * 6, 60 * 8 * 5), 8L, at = at_, lag = lag_, na_pad = TRUE, simplify = FALSE,
                                    features_set = "feasts")
RollingTheftFeastsFatures = RollingTheftInit$get_rolling_features(OhlcvInstance)
RollingTheftInit = RollingTheft$new(windows = c(60 * 4), workers = 1L, at = at_, lag = lag_, na_pad = TRUE, simplify = FALSE,
                                    features_set = "tsfel")
RollingTheftTsfelFeatures = suppressMessages(RollingTheftInit$get_rolling_features(OhlcvInstance))

# merge all features
features <- Reduce(function(x, y) merge(x, y, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE),
                   list(OhlcvFeaturesSet_sample, RollingBidAskFeatures, RollingBackCusumFeatures,
                        RollingExuberFeatures, RollingForecatsAutoarimaFeatures, RollingForecatNnetarFeatures,
                        RollingGasFeatures, RollingGpdFeatures,
                        RollingTheftCatch22Features, RollingTheftFeastsFatures, RollingTheftTsfelFeatures))


########### MERGE FEATURES FROM DIFFERENT SESSIONS #############
# # import last features
# features <- storage_read_csv(CONTFEATURES, "HFT-features-20220119105326.csv")
# features <- Reduce(function(x, y) merge(x, y, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE),
#                    list(features, RollingTheftFeastsFatures))
########### MERGE FEATURES FROM DIFFERENT SESSIONS #############

# save features to azure blob
# time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
# file_name_ <- paste0("HFT-features-", time_, ".csv")
# storage_write_csv(features, CONTFEATURES, file_name_)

# merge features and ohcv with bins
features <- storage_read_csv(CONTFEATURES, "HFT-features-20220119105326.csv")
features <- as.data.table(features)
attr(features$date, "tzone") <- "EST"

# join labels to features
features[, date_join_features := date]
ohlcv_meta[, date_join_ohlcv_meta := date]
cols_features_keep <- c("symbol", "date", "date_join_features", colnames(features)[which(colnames(features) == "close_ath"):ncol(features)])
clf_data <- features[, ..cols_features_keep][ohlcv_meta, on = c("symbol", "date"), roll = Inf]
clf_data[, .(symbol, date, date_join_features, date_join_ohlcv_meta)]

# remove columns with manz NA
keep_cols <- names(which(colMeans(!is.na(clf_data)) > 0.8))
setdiff(colnames(clf_data), keep_cols)
clf_data <- clf_data[, .SD, .SDcols = keep_cols]

# select relevant columns
feature_cols <- setdiff(colnames(clf_data)[which(colnames(clf_data) %in% cols_features_keep)], c("symbol", "date", "date_join_features"))
clf_data <- clf_data[, .SD, .SDcols = c(feature_cols, "bin")]

#  winsorization (remove ooutliers)
clf_data <- na.omit(clf_data)
library(DescTools)
clf_data[, (feature_cols) := lapply(.SD, Winsorize, probs = c(0.03, 0.97), na.rm = TRUE), .SDcols = feature_cols] # winsorize across dates
clf_data <- clf_data[is.finite(rowSums(clf_data)),]

# remove cols with inf values
cols <- vapply(clf_data, is.numeric, FUN.VALUE = logical(1), USE.NAMES = FALSE)
df <- as.data.frame(clf_data)
df <- df[!is.infinite(rowSums(df[, cols])), ]
cols <- vapply(df, is.numeric, FUN.VALUE = logical(1), USE.NAMES = FALSE)
# df[, cols] <- df[!is.nan(rowSums(df[, cols])), cols]
clf_data <- as.data.table(df)

# define task
clf_data[, bin := as.factor(bin)]
colnames(clf_data) <- gsub(" |-", "_", colnames(clf_data))
task = TaskClassif$new(id = "hft", backend = clf_data, target = 'bin')

# tests
all(sapply(task$data(), function(x) all(is.finite(x)))) # check for infinity values
all(sapply(task$data(), function(x) all(!is.nan(x)))) # check NAN
all(sapply(task$data(), function(x) all(is.numeric(x) | is.logical(x) | is.factor(x))))

# inspect data
skimr::skim(task$data()[, 1:50])




################ USE THIS PART ONLY IF DATA I S HOLD OUT SET ################
# import model
mlr3model <- storage_load_rds(CONTMODELS, "fml-bestmodel-hft-20220203141243.rds")
colnames(clf_data) <- gsub(" |-", "_", colnames(clf_data))
clf_data <- na.omit(clf_data)
"TSFEL_0_Absolute_energy_240" %in% colnames(clf_data)
preds_fb <- mlr3model$predict_newdata(clf_data[, 3:(ncol(clf_data) - 12)])
preds_fb

preds_fb <- mlr3model$predict(task)
preds_fb$confusion
preds_fb$score(msr("classif.acc"))
preds_fb_prob <- as.data.table(preds_fb)
preds_fb_prob <- preds_fb_prob[prob.1 > 0.65]
sum(preds_fb_prob$truth == 1) / nrow(preds_fb_prob)

################ USE THIS PART ONLY IF DATA I S HOLD OUT SET ################

# FEATURE IMPORTANCE ------------------------------------------------------
# rpart tree
library(rpart.plot)
learner = lrn("classif.rpart", maxdepth = 4, predict_type = "prob")
task_ <- task$clone()
learner$train(task_)
predictins = learner$predict(task_)
predictins$score(msr("classif.acc"))
learner$importance()
rpart_model <- learner$model
rpart.plot(rpart_model)

# ranger learner: https://arxiv.org/pdf/1804.03515.pdf section 2.1
learner_ranger = lrn("classif.ranger")
learner_ranger$param_set$values = list(num.trees = 5000)
at_rf = AutoFSelector$new(
  learner = learner_ranger,
  resampling = rsmp("holdout"),
  measure = msr("classif.ce"),
  terminator = trm("evals", n_evals = 25),
  fselector = fs("random_search")
)
at_rpart = AutoFSelector$new(
  learner = lrn("classif.rpart"),
  resampling = rsmp("holdout"),
  measure = msr("classif.ce"),
  terminator = trm("evals", n_evals = 25),
  fselector = fs("sequential")
)
at_rpart_genetic = AutoFSelector$new(
  learner = lrn("classif.rpart"),
  resampling = rsmp("holdout"),
  measure = msr("classif.ce"),
  terminator = trm("evals", n_evals = 25),
  fselector = fs("genetic_search")
)
grid = benchmark_grid(
  task = task,
  learner = list(at_rf, at_rpart, at_rpart_genetic),
  resampling = rsmp("cv", folds = 5)
)
bmr_fs = benchmark(grid, store_models = TRUE)
bmr_fs$aggregate(msrs(c("classif.ce", "time_train")))

# exract features
important_features <- lapply(as.data.table(bmr_fs)$learner, function(x) unlist(x$fselect_result$features))
important_features <- as.data.table(table(unlist(important_features)))
important_features <- important_features[order(N, decreasing = TRUE), ]
important_features_n <- head(important_features[, V1], 20) # top n features selected through ML
important_features_most_important <- important_features[N == max(N, na.rm = TRUE), V1] # most importnatn feature(s)
features_short <- unique(c(important_features_most_important, important_features_n))
features_long <- unique(c(important_features_most_important, important_features_n))
features_long <- features_long[!grepl("lm_", features_long)]

# inspect best var
summary(clf_data$TSFEL_0_Centroid_240)
plot(clf_data$TSFEL_0_Centroid_240, type = "l")
task_ <- task$clone()
learner$train(task_$select(important_features_most_important))
predictins = learner$predict(task_)
predictins$score(msr("classif.acc"))
learner$importance()
rpart_model <- learner$model
rpart.plot(rpart_model)

# save localy feature importance results
save_path_mlr3models = "D:/mlfin/mlr3_models"
time_ <- format.POSIXct(Sys.time(), "%Y%m%d-%H%M%S")
file_name <- file.path(save_path_mlr3models, paste0('fi-', task$id, "-", time_, '.rds'))
saveRDS(bmr_fs, file = file_name)

# save to blob same data
storage_save_rds(bmr_fs, CONTMODELS, file_name)




# ML PLAY -----------------------------------------------------------------


learner = lrn("classif.ranger")
learner$predict_sets = c("train", "test")
learner$predict_type = "prob"
learner$param_set
learner$param_set$values$max.depth = 6
learner$param_set$values$num.trees = 3000
# learner$param_set$values$num.threads = 2L
# EXTRATREES
# learner$param_set$values$ntree = 600L
# learner$param_set$values$mtry = 0.5 * task_extreme$ncol
# learner$param_set$values$nodesize = 50
# learner$param_set$values$numRandomCuts = 2
# FNN
# learner$param_set$values$k = 5
# learner$param_set$values$algorithm = "cover_tree"
# IBK
# learner$param_set$values$K = 10
# J48
# learner$param_set$values$M = 10
# kknn
# learner$param_set$values$k = 10
# lightgbm
# learner$param_set$values$num_leaves = 10
# learner$param_set$values$max_depth = 6
# learner$param_set$values$min_data_in_leaf = 100
# nnet
# learner$param_set$values$size = 2
# learner$param_set$values$MaxNWts  = 5000
# PART
# learner$param_set$values$M  = 50
# erandomtnomd fores
# learner$param_set$values$mtry = 100
# learner$param_set$values$nodesize  = 50
# rfsrc
# learner$param_set$values$mtry.ratio = 0.9
# learner$param_set$values$nodedepth = 20

fm = learner$train(task)
preds = fm$predict(task)
preds$confusion
preds$score(msrs(c("classif.acc", "classif.recall", "classif.precision")))

# holdout prediction
preds_holdout <- fm$predict(task_extreme_holdout)
preds_holdout$confusion
preds_holdout$score(msrs(c("classif.acc", "classif.recall", "classif.precision")))
prediciotns_extreme_holdout <- as.data.table(preds_holdout)
prediciotns_extreme_holdout <- prediciotns_extreme_holdout[prob.1 > 0.60]
nrow(prediciotns_extreme_holdout)
mlr3measures::acc(prediciotns_extreme_holdout$truth, prediciotns_extreme_holdout$response)


# important features before extraction
learner_fi = lrn("classif.ranger")
learner_fi$predict_sets = c("test")
learner_fi$predict_type = "prob"
learner_fi$param_set$values$max.depth = 6
learner_fi$param_set$values$num.trees = 3000

#
f_n <- 5
lrn = lrn("classif.ranger")
lrn$param_set$values = list(importance = "impurity")
graph = po("removeconstants", ratio = 0.03) %>>%
  po("filter", flt("find_correlation", method = "spearman"), filter.cutoff = 0.01) %>>%
  gunion(list(
    po("filter", mlr3filters::flt("disr"), filter.nfeat = f_n, param_vals = list(affect_columns = selector_type("numeric"))),
    po("filter", mlr3filters::flt("jmim"), filter.nfeat = f_n, param_vals = list(affect_columns = selector_type("numeric"))),
    po("filter", mlr3filters::flt("jmi"), filter.nfeat = f_n, param_vals = list(affect_columns = selector_type("numeric"))),
    po("filter", mlr3filters::flt("mim"), filter.nfeat = f_n, param_vals = list(affect_columns = selector_type("numeric"))),
    po("filter", mlr3filters::flt("mrmr"), filter.nfeat = f_n, param_vals = list(affect_columns = selector_type("numeric"))),
    po("filter", mlr3filters::flt("njmim"), filter.nfeat = f_n, param_vals = list(affect_columns = selector_type("numeric"))),
    po("filter", mlr3filters::flt("cmim"), filter.nfeat = f_n, param_vals = list(affect_columns = selector_type("numeric"))),
    po("filter", mlr3filters::flt("importance", learner = mlr3::lrn("classif.rpart")), filter.nfeat = f_n, id = "rpart_imp"),
    po("filter", mlr3filters::flt("importance", learner = lrn), filter.nfeat = f_n, id = "ranger_imp")
    # po("filter", mlr3filters::flt("importance", learner = lrn("classif.xgboost")), filter.nfeat = 2, id = "xgboost_imp")
  )) %>>%
  po("featureunion", 9, id = "union_filters") %>>%
  learner_fi
plot(graph)
graph_learner = as_learner(graph)
graph_learner$train(task)
preds_2 = graph_learner$predict(task)
preds_2$confusion
preds_2$score(msrs(c("classif.acc", "classif.recall", "classif.precision")))
graph_learner$state$model$ranger_imp
graph_learner$state$model$disr

#  save model
time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
file_name <- paste0('fml-bestmodel-', task$id, "-", time_, '.rds')
storage_save_rds(graph_learner, CONTMODELS, file_name)



# AUTOML ------------------------------------------------------------------

# estimate automl
task_ <- task$clone()
bmr_automl = FinAutoML(tasks = task_$filter(1:5000), workers = 2L, outer_folds = 2L, inner_evals = 5)

# inspect results
autoplot(bmr_automl)
bmr_automl$aggregate(msr("classif.acc"))
bmr_automl$score(msr("classif.acc"))
extract_inner_tuning_results(bmr_automl) # xtract inner tuning results of nested resampling
mlr3tuning::extract_inner_tuning_archives(bmr_automl) # Extract inner tuning archives of nested resampling.



# HOLDOUT SET -------------------------------------------------------------

#
symbols <- c("FB")
azure_files <- paste0(symbols, ".csv")
market_data_l <- lapply(azure_files, function(x) cbind(symbol = gsub(".csv", "", x), storage_read_csv(container=CONT, x)))
market_data <- rbindlist(market_data_l)
