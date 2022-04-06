library(data.table)
library(finfeatures)
library(highfrequency)
library(AzureStor)
library(mlfinance)
library(xts)



# globals
DATAPATH = "D:/bulk/usa/equity"
SAVEPATH = "D:/filters/equity/usa/tick"
BLOBENDPOINT = "https://contentiobatch.blob.core.windows.net/"
BLOBKEY = "qdTsMJMGbnbQ5rK1mG/9R1fzfRnejKNIuOv3X3PzxoBqc1wwTxMyUuxNVSxNxEasCotuzHxwXECo79BLv71rPw=="
BLOBENDKEY <- storage_endpoint(BLOBENDPOINT, key=BLOBKEY)

# import factor files
cont <- storage_container(BLOBENDKEY, "factor-files")
cont_blob_file <- AzureStor::list_blobs(cont)
cont_blob_file <- cont_blob_file$name
factor_files_l <- lapply(cont_blob_file, function(x) {
  # x <- gsub("-csv", "\\.csv", gsub("\\.", "-", x))
  print(x)
  y <- storage_read_csv(cont, x, col_names = FALSE)
  if (nrow(y) > 0) {
    y$symbol <- toupper(gsub("\\.csv", "", x))
  }
  y
})
factor_files <- rbindlist(factor_files_l)
setnames(factor_files, colnames(factor_files), c("date", "price_factor", "split_factor", "previous_price", "symbol"))
factor_files[, symbol := toupper(symbol)]
factor_files[, date := as.Date(as.character(date), "%Y%m%d")]
setnames(factor_files, c("symbol"), c("SYMBOL"))

# calcualte filters
# tick_data_files <- list.files("D:/tick_data", full.names = TRUE)

# create bulk data
# tick_data <- list()
# for (i in seq_along(tick_data_files)) {
#
#   # debugging
#   print(tick_data_files[i])
#
#   # get market data and save in bulk folder
#   market_data_minute <- lapply(list.files(tick_data_files[i], full.names = TRUE), function(x) {
#     fread(paste0('unzip -p ', x))
#   })
#   market_data_minute <- rbindlist(market_data_minute)
#   tick_data[[i]] <- cbind(symbol = toupper(gsub(".*/", "", tick_data_files[i])), market_data_minute)
# }
# df <- rbindlist(tick_data)
# fwrite(df, file.path(DATAPATH, "tick.csv"))

# import data
tick_data <- fread(file.path(DATAPATH, "tick.csv"))
setnames(tick_data, c("SYMBOL", "DT", "PRICE", "SIZE"))

# clean raw tick data
tick_data <- tradesCleanup(tDataRaw = tick_data)
print(tick_data$report)
tick_data <- tick_data$tData
tick_data <- tick_data[SYMBOL %in% c("BAC", "AAPL", "PG")]

# create bars
time_bars <- lapply(unique(tick_data$SYMBOL),
                    function(s) aggregateTrades(tick_data[SYMBOL == s],
                                                alignBy = "minutes",
                                                alignPeriod = 1))
time_bars <- rbindlist(time_bars)

# adjust for dividends and splits
time_bars <- time_bars[symbol %in% unique(factor_files$SYMBOL)]
time_bars[, date:= as.Date(DT)]
time_bars <- merge(time_bars, factor_files, by = c("SYMBOL", "date"), all.x = TRUE, all.y = FALSE)
time_bars[, `:=`(split_factor = na.locf(split_factor, na.rm = FALSE, rev = TRUE),
          price_factor = na.locf(price_factor, na.rm = FALSE, rev = TRUE)), by = SYMBOL]
time_bars[, `:=`(split_factor = ifelse(is.na(split_factor), 1, split_factor),
          price_factor = ifelse(is.na(price_factor), 1, price_factor))]
cols_change <- c("PRICE", "VWPRICE")
time_bars[, (cols_change) := lapply(.SD, function(x) {x * price_factor * split_factor}), .SDcols = cols_change]
cols_change <- c("NUMTRADES", "SIZE")
time_bars[, (cols_change) := lapply(.SD, function(x) {x / split_factor}), .SDcols = cols_change]
time_bars <- time_bars[, .(SYMBOL, DT, PRICE, NUMTRADES, SIZE, VWPRICE)]
setorder(time_bars, SYMBOL, DT)
time_bars <- unique(time_bars)
# plot(time_bars[SYMBOL == "AAPL", NUMTRADES])



# FILTERS -----------------------------------------------------------------

#  calculate filters LATER MOVE THIS TO VM
for (symbol in unique(time_bars$SYMBOL)) {

  # TEST
  # symbol <- unique(time_bars$SYMBOL)[1]

  # log
  print(symbol)

  # sample
  sample_ <- time_bars[SYMBOL == symbol]

  # read existing
  file_name <- paste0(symbol, ".csv")
  cont <- storage_container(BLOBENDKEY, "filters-equity-usa-tick")


  # BACKCUSUM ---------------------------------------------------------------

  # check backcusum
  # if (blob_exists(cont, file_name)) {
  #   history <- fread(file.path(SAVEPATH, file_name))
  #   if (nrow(history) > 0 & any(grepl("back", colnames(history)))) {
  #     sample_symbol_date <- sample_[, .(SYMBOL, DT)]
  #     setnames(sample_symbol_date, c("symbol", "date"))
  #     new_data <- fsetdiff(history[, .(symbol, date)], sample_symbol_date)
  #     if (nrow(new_data) == 0) next()
  #   }
  # }

  # Ins Backcusum
  OhlcvInstance = Ohlcv$new(sample_, id_col = "SYMBOL", date_col = "DT", price = "VWPRICE", ohlcv = NULL)
  RollingBackcusumInit = RollingBackcusum$new(windows = c(60 * 4),
                                              workers = 10L,
                                              at = 1:nrow(OhlcvInstance$X),
                                              lag = 1L,
                                              na_pad = TRUE,
                                              simplify = FALSE)
  RollingBackcusumResults = RollingBackcusumInit$get_rolling_features(OhlcvInstance)


  # EXUBER ------------------------------------------------------------------

  # check backcusum
  # if (blob_exists(cont, file_name)) {
  #   history <- fread(file.path(SAVEPATH, file_name))
  #   if (nrow(history) > 0 & any(grepl("exuber", colnames(history)))) {
  #     sample_symbol_date <- sample_[, .(SYMBOL, DT)]
  #     setnames(sample_symbol_date, c("symbol", "date"))
  #     new_data <- fsetdiff(history[, .(symbol, date)], sample_symbol_date)
  #     if (nrow(new_data) == 0) next()
  #   }
  # }

  # Ins Backcusum
  RollingExuberInit = RollingExuber$new(windows = c(600),
                                        workers = 8L,
                                        at = 1:nrow(OhlcvInstance$X),
                                        lag = 1L,
                                        na_pad = TRUE,
                                        simplify = FALSE,
                                        exuber_lag = 1L)
  RollingExuberResults = RollingExuberInit$get_rolling_features(OhlcvInstance)




  # SAVE --------------------------------------------------------------------

  # save localy and to Azure blob
  fwrite(RollingBackcusumResults, file.path(SAVEPATH, file_name))
  upload_blob(cont, file.path(SAVEPATH, file_name))
}

# import filters
cont <- storage_container(BLOBENDKEY, "filters-equity-usa-tick")
filters <- lapply(unique(time_bars$SYMBOL), function(x) storage_read_csv(cont, paste0(x, ".csv")))
filters <- rbindlist(filters)

# filter events
backcusum_column <- "backcusum_rejections_1_240" # 0.1 0.05 0.01 0.001
filtered_events <- filters[get(backcusum_column) == 1, .SD, .SDcols = c("symbol", "date")]


# LABELING ----------------------------------------------------------------

# define vertical bars
symbols <- unique(filtered_events$symbol)
vertical_barriers <- lapply(symbols, function(s) {
  sample_ <- filtered_events[symbol == s]
  y <- add_vertical_barrier(sample_[, .(date)], time_bars[SYMBOL == s, DT])
  y$symbol <- s
  y
})
vertical_barriers <- rbindlist(vertical_barriers)

# tripple barrier labeling
min_return <- 0.001
pt_sl <- c(1, 1)
events <- lapply(symbols, function(s) {
  sample_ <- time_bars[SYMBOL == s, .(DT, PRICE)]
  filtered_events_ <- filtered_events[symbol == s, .SD, .SDcols = "date"]
  daily_vol <- cbind(sample_[, .(Datetime = DT)], Value = 0.03)
  vertical_barriers_ <- vertical_barriers[symbol == s, .(Datetime, t1)]
  events <- get_events(price = sample_,                           # close data.table with index and value
                       t_events = filtered_events_,               # bars to look at for trades
                       pt_sl = pt_sl,                             # multiple target argument to get width of the barriers
                       target = daily_vol,                        # values that are used to determine the width of the barrier
                       min_ret = min_return,                      # minimum return between events
                       vertical_barrier_times=vertical_barriers_, # vertical barriers timestamps
                       side_prediction=NA)                        # prediction from primary model (in this case no primary model)
  return(events)
})
labels <- lapply(1:length(events), function(i) {
  y <- get_bins(events[[i]], time_bars[SYMBOL == symbol[i], .(DT, PRICE)])
  y <- drop_labels(y, 0.05)
  y$SYMBOL <- symbol[i]
  y
})
labels <- rbindlist(labels)
setnames(labels, c("Datetime"), c("DT"))
labels <- labels[, .SD, .SDcols = c("SYMBOL", "DT", "ret", "trgt", "bin")]
table(labels$bin)

# RV
oneMinute <- sampleOneMinuteData[as.Date(DT) > "2001-08-30"]
RV1 <- rCov(oneMinute[, list(DT, MARKET)], makeReturns = TRUE, alignBy = "minutes", alignPeriod = 1)
RV1 <- rCov(time_bars[symbol == "AAPL", list(DT, VWPRICE)], makeReturns = TRUE, alignBy = "hours", alignPeriod = 6)
plot(RV1$RV)
BPV1 <- rBPCov(time_bars[symbol == "AAPL", list(DT, VWPRICE)], makeReturns = TRUE)
plot(BPV1$BPV)


