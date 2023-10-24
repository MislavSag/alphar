library(data.table)
library(lubridate)
library(portsort)
library(finfeatures)



# import daily data
dt = fread("F:/lean_root/data/all_stocks_daily.csv")

# this want be necessary after update
setnames(dt, c("date", "open", "high", "low", "close", "volume", "close_adj", "symbol"))

# remove duplicates
dt = unique(dt, by = c("symbol", "date"))

# create help colummns
dt[, year_month_id := ceiling_date(date, unit = "month") - days(1)]

# remove negative prices
dt = dt[close > 0 & close_adj > 0]

# add variables
dt[, dollar_volume := volume * close]

# adjust all columns
dt[, ratio := close_adj / close]
dt[, `:=`(
  open_adj = open * ratio,
  high_adj = high * ratio,
  low_adj = low * ratio
)]
dt[symbol == "aapl"]

# keep only adjusted columns and raw close
dt = dt[, .(symbol, date, open = open_adj, high = high_adj, low = low_adj,
            close = close_adj, close_raw = close, volume, dollar_volume, year_month_id)]

# downsample
dtm = dt[, .(
  date = tail(date, 1),
  open = head(open, 1),
  high = max(high),
  low = min(low),
  close = tail(close, 1),
  volume_mean = mean(volume, na.rm = TRUE),
  dollar_volume = sum(dollar_volume, na.rm = TRUE),
  dollar_volume_mean = mean(dollar_volume, na.rm = TRUE),
  volume = sum(volume, na.rm = TRUE)
), by = c("symbol", "year_month_id")]
setorder(dtm, symbol, year_month_id)

# create forward returns
dtm[, ret_forward := shift(close, -1, type = "shift") / close - 1, by = symbol]

# # predictors
# ohlcv = Ohlcv$new(dtm, date_col = "year_month_id")
# OhlcvFeaturesInstance = OhlcvFeatures$new(windows = c(6, 12), quantile_divergence_window = c(6, 12))
# predictors_ohlcv = predictors_ohlcv$get_ohlcv_features(OhlcvFeaturesInstance)
#
# dtm[, mom11 := shift(close, 11, "lag") / close - 1]
# dtm[, mom6 := shift(close, 6, "lag") / close - 1]
# predictors = c("mom6", "mom11")

# select cols
cols_ = c("symbol", "year_month_id", "ret_forward", "dollar_volume_mean", predictors)
dtm = dtm[, ..cols_]
dtm = na.omit(dtm)

# remove assets with price < x$
x = x[, .SD[close > min_price], by = year_month_id]

# filter 500 with highest volume
filter_var = "dollar_volume_mean" # PARAMETER
num_coarse = 500                  # PARAMETER
setorderv(dtm, c("year_month_id", filter_var), order = 1L)
dtm = dtm[, tail(.SD, num_coarse), by = year_month_id]

# make function that returns results for every predictor
portsort_results_l = lapply(predictors, function(p) {
  # debug
  # p = "mom11"
  print(p)

  # sample only relevant data
  cols = c("symbol", "year_month_id", "ret_forward", p)
  dtm_sample = dtm[, ..cols]

  # predictors matrix
  Fa = dcast(dtm_sample, year_month_id ~ symbol, value.var = p)
  setorder(Fa, year_month_id)
  Fa = as.xts.data.table(Fa)
  cols_with_na <- apply(Fa, MARGIN = 2, FUN = function(x) sum(is.na(x)) > as.integer(nrow(Fa) * 0.8))
  Fa = Fa[, !cols_with_na]

  # Forward returns
  R_forward = as.xts.data.table(dcast(dtm, year_month_id ~ symbol), value.var = "ret_forward")
  R_forward = R_forward[, !cols_with_na]

  # uncondtitional sorting
  dimA = 0:10/10
  sort.output.uncon = unconditional.sort(Fa,
                                         Fb=NULL,
                                         Fc=NULL,
                                         R_forward,
                                         dimA=dimA,
                                         dimB=NULL,
                                         dimC=NULL,
                                         type = 7)
  return(sort.output.uncon)
})

# extract CAR an SR
portsort_l = lapply(portsort_results_l, function(x) table.AnnualizedReturns(x$returns))
portsort_l = lapply(portsort_l, function(x) as.data.table(x, keep.rownames = TRUE))
names(portsort_l) = predictors
portsort_dt = rbindlist(portsort_l, idcol = "predictor")

as.data.table(portsort_l[[1]], keep.rownames = TRUE)

# # predictors matrix
# Fa = dcast(dtm[, .(symbol, year_month_id, mom)], year_month_id ~ symbol)
# setorder(Fa, year_month_id)
# # Fa = Fa[year_month_id > as.Date("2015-01-01")]
# Fa = as.xts.data.table(Fa)
# cols_with_na <- apply(Fa, MARGIN = 2, FUN = function(x) sum(is.na(x)) > as.integer(nrow(Fa) * 0.8))
# Fa = Fa[, !cols_with_na]
#
# # Forward returns
# R_forward = as.xts.data.table(dcast(dtm[, .(symbol, year_month_id, ret_forward)], year_month_id ~ symbol))
# R_forward = R_forward[, !cols_with_na]
#
# # uncondtitional sorting
# dimA = 0:10/10
# sort.output.uncon = unconditional.sort(Fa, Fb=NULL, Fc=NULL,
#                                        R_forward,
#                                        dimA=dimA, dimB=NULL, dimC=NULL,
#                                        type = 7)
#
# # Set the scale to 365 (Cryptocurreny markets have no close) and geometric to FALSE (we are using log returns)
# table.AnnualizedReturns(sort.output.uncon$returns, scale = 12, geometric = TRUE, digits = 3)
