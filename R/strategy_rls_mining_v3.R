library(data.table)
library(arrow)
library(lubridate)
library(portsort)
library(PerformanceAnalytics)
library(fs)
library(finfeatures)
library(TTR)


# SETUP -------------------------------------------------------------------
# Paths
PATH     = "F:/data/equity/us/predictors_daily"
PAHDAILY = "F:/lean_root/data/all_stocks_daily.csv"
PATHFUND = "F:/data/equity/us/predictors_daily/factors/fundamental_factors.parquet"


# UTILS -------------------------------------------------------------------
# Function to check if more than 50% of a column's values are zeroes
check_zero_proportion <- function(column, prop = 0.5) {
  zero_count <- sum(column == 0, na.rm = TRUE)
  total_count <- length(column)
  return(zero_count / total_count > prop)
}


# IMPORT DATA -------------------------------------------------------------
# import daily data
col = c("date", "open", "high", "low", "close", "volume", "close_adj", "symbol")
dt = fread(PAHDAILY, col.names = col)
dt = unique(dt, by = c("symbol", "date"))
unadjustd_cols = c("open", "high", "low")
dt[, (unadjustd_cols) := lapply(.SD, function(x) (close_adj / close) * x), .SDcols = unadjustd_cols]
dt = na.omit(dt)
setorder(dt, symbol, date)
setnames(dt, c("close", "close_adj"), c("close_raw", "close"))
dt = dt[open > 0.00003 & high > 0.00003 & low > 0.00003 & close > 0.00003]

# remove symbols with less than 2 years of data
dt_n = dt[, .N, by = symbol]
symbols_keep = dt_n[N > 22*12*2, symbol]
dim(dt)
dt = dt[symbol %chin% symbols_keep]
dim(dt)

# keep 100 most liquid at every date
dt[, dollar_volume := close * volume]
setorder(dt, date, -dollar_volume)
liquid_symbols = dt[, .(symbol = head(symbol, 100)), by = date]
liquid_symbols = liquid_symbols[, unique(symbol)]
length(liquid_symbols)
dt[, length(unique(symbol))]
dim(dt)
dt = dt[symbol %chin% liquid_symbols]
dim(dt)
dt[, dollar_volume := NULL]

# sort
setorder(dt, symbol, date)

# create features from OHLCV
ohlcv_features_daily = OhlcvFeaturesDaily$new(
  at = NULL,
  windows = c(5, 10, 22, 22 * 3, 22 * 6, 22 * 12, 22 * 12 * 2),
  quantile_divergence_window = c(50, 100)
)
dt = ohlcv_features_daily$get_ohlcv_features(dt)

# # import exuber data
# exuber = lapply(dir_ls(fs::path(PATH, "exuber")), read_parquet)
# names(exuber) = path_ext_remove(path_file(dir_ls(fs::path(PATH, "exuber"))))
# exuber = rbindlist(exuber, fill = TRUE, idcol = "symbol")
# exuber[, date := as.IDate(date)]
# exuber[, exuber_128_radf_sum_log := exuber_128_1_adf_log + exuber_128_1_sadf_log +
#          exuber_128_1_gsadf_log + exuber_128_1_badf_log + exuber_128_1_bsadf_log]
# exuber[, exuber_252_radf_sum_log := exuber_252_1_adf_log + exuber_252_1_sadf_log +
#          exuber_252_1_gsadf_log + exuber_252_1_badf_log + exuber_252_1_bsadf_log]
# exuber[, exuber_504_radf_sum_log := exuber_504_1_adf_log + exuber_504_1_sadf_log +
#          exuber_504_1_gsadf_log + exuber_504_1_badf_log + exuber_504_1_bsadf_log]
#
# # merge dt and exuber and delete exuber
# dt = merge(dt, exuber, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE)
#
# # free memory
# rm(exuber)
# rm(dt_n)
# gc()

# # import fundamnetal data
# fundamentals = read_parquet(PATHFUND)
# fundamentals[, symbol := tolower(symbol)]
# fundamentals[, date_fund := date]
# fundamentals[, date := NULL]
# cols_ = colnames(fundamentals)
# predictors_fund = cols_[which(cols_ == "cashAndCashEquivalents"):which(cols_ == "sp")]


# PREPARE DATA FOR RLS ----------------------------------------------------
# create month colummn
dt[, year_month_id := ceiling_date(date, unit = "month") - days(1)]

# add variables
dt[, dollar_volume := volume * close]

# predictor columns when using exuber
# predictors = colnames(dt)[grepl("exuber", colnames(dt))]

# predictors columns when using OHLCV daily featuers
predictors = colnames(dt)[which(colnames(dt) == "returns_1"):ncol(dt)]
predictors = setdiff(predictors, "year_month_id")

# downsample to monthly frequency
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

# convert ohlcv daily predictors to monthly freq by moments
predictorsm = unique(dt, by = c("symbol", "year_month_id"), fromLast = TRUE)
predictorsm = predictorsm[, .SD, .SDcols = c("symbol", "year_month_id",
                                             setdiff(predictors, "dollar_volume"))]
predictorsm_sd = dt[, lapply(.SD, sd, na.rm = TRUE),
                    .SDcols = setdiff(predictors, "dollar_volume"),
                    by = c("symbol", "year_month_id")]
setnames(predictorsm_sd, setdiff(predictors, "dollar_volume"), paste0(setdiff(predictors, "dollar_volume"), "_sd"))
# predictorsm_mean = dt[, lapply(.SD, mean, na.rm = TRUE), .SDcols = predictors, by = c("symbol", "year_month_id")]
# setnames(predictorsm_mean, predictors, paste0(predictors, "_mean"))
predictorsm = Reduce(function(x, y) merge(x, y, by = c("symbol", "year_month_id")),
                     list(predictorsm, predictorsm_sd)) # , predictorsm_mean
cols = c("symbol", "year_month_id",
         setdiff(predictors, "dollar_volume"),
         paste0(setdiff(predictors, "dollar_volume"), "_sd")) # ,
         # paste0(predictors, "_mean"))
predictorsm = predictorsm[, ..cols]
# predictors = colnames(predictorsm)[3:ncol(predictorsm)]

# # convert exuber predictors to monthly freq by moments
# predictorsm = unique(dt, by = c("symbol", "year_month_id"), fromLast = TRUE)
# predictorsm_sd = dt[, lapply(.SD, sd, na.rm = TRUE), .SDcols = predictors, by = c("symbol", "year_month_id")]
# setnames(predictorsm_sd, predictors, paste0(predictors, "_sd"))
# predictorsm_mean = dt[, lapply(.SD, mean, na.rm = TRUE), .SDcols = predictors, by = c("symbol", "year_month_id")]
# setnames(predictorsm_mean, predictors, paste0(predictors, "_mean"))
# predictorsm = Reduce(function(x, y) merge(x, y, by = c("symbol", "year_month_id")),
#                      list(predictorsm, predictorsm_sd, predictorsm_mean))
# cols = c("symbol", "year_month_id",
#          predictors,
#          paste0(predictors, "_sd"),
#          paste0(predictors, "_mean"))
# predictorsm = predictorsm[, ..cols]
# predictors = colnames(predictorsm)[3:ncol(predictorsm)]

# merge predictors by month and prices_m
dtm = merge(dtm, predictorsm, by = c("symbol", "year_month_id"))

# add fundamentals
# dtm[, year_month_id_ := year_month_id]
# dtm[, symbol_ := symbol]s
# dtm[, ticker := gsub("\\.1|\\.2", "", symbol)]
# dtm[, symbol := NULL]
# fundamentals[, fillingDate_ := fillingDate]
# dtm = fundamentals[dtm, on = c("symbol" = "ticker", "fillingDate_" = "year_month_id_"), roll = Inf]
# dtm[, .(symbol, date, fillingDate, year_month_id)]
# dtm[, fillingDate_ := NULL]
# predictors = c(predictors, predictors_fund)

# create forward returns
dtm[, ret_forward := shift(close, -1, type = "shift") / close - 1, by = symbol]
dtm[, .(symbol, year_month_id, close, ret_forward)]

# check for duplicates
dup_index = which(dtm[, duplicated(.SD[, .(symbol, year_month_id)])])
dtm = unique(dtm, by = c("symbol", "year_month_id"))

# select cols
cols_ = c("symbol", "year_month_id", "ret_forward", "dollar_volume_mean", predictors)
dtm = dtm[, ..cols_]
# dtm = na.omit(dtm)

# # remove assets with price < x$
# min_price = 1
# x = x[, .SD[close > min_price], by = year_month_id]

# filter n with highest volume
filter_var = "dollar_volume_mean" # PARAMETER
num_coarse = 4000                  # PARAMETER
setorderv(dtm, c("year_month_id", filter_var), order = 1L)
dtm_filter = dtm[, tail(.SD, num_coarse), by = year_month_id]

# remove observations with many NA's
threshold = 0.1
na_cols <- sapply(dtm_filter, function(x) sum(is.na(x))/length(x) > threshold)
print(na_cols[na_cols == TRUE])
print(na_cols[na_cols == FALSE])
predictors_cleaned = setdiff(predictors, names(na_cols[na_cols == TRUE]))
cols = c("symbol", "year_month_id", "ret_forward", predictors_cleaned)
dtm_filter = dtm_filter[, ..cols]

# remove NA values for target
# dtm_filter = na.omit(dtm_filter, cols = "ret_forward")
dtm_filter = na.omit(dtm_filter)

# winsorize extreme values
predictors_reduced = intersect(predictors, colnames(dtm_filter))
# dtm_filter[, (predictors_reduced) := lapply(.SD, as.numeric), .SDcols = predictors_reduced]
# dtm_filter[, (predictors_reduced) := lapply(.SD, function(x) DescTools::Winsorize(x, probs = c(0.01, 0.99), na.rm = TRUE)),
#            by = year_month_id, .SDcols = predictors_reduced]

# visualize some vars
var_ = sample(predictors_cleaned, 1)
# var_ = "freeCashFlowYield"
date_ = dtm_filter[, sample(year_month_id, 1)]
x = dtm_filter[year_month_id == date_, ..var_]
hist(unlist(x), main=var_, xlab="dots", ylab=var_)

# make function that returns results for every predictor
dtm_filter_sample = dtm_filter[year_month_id > as.Date("2001-01-01")]
dim(dtm_filter_sample)
portsort_results_l = lapply(predictors_cleaned, function(p) {
  # debug
  # p = "priceSalesRatio"
  print(p)

  # sample only relevant data
  cols = c("symbol", "year_month_id", "ret_forward", p)
  dtm_filter_sample_ = dtm_filter_sample[, ..cols]

  # predictors matrix
  Fa = dcast(dtm_filter_sample_, year_month_id ~ symbol, value.var = p)
  setorder(Fa, year_month_id)
  Fa = as.xts.data.table(Fa)
  cols_with_na <- apply(Fa, MARGIN = 2, FUN = function(x) sum(is.na(x)) > as.integer(nrow(Fa) * 0.6))
  # dim(Fa)
  Fa = Fa[, !cols_with_na]

  # remove all NA values
  rows_with_na <- apply(Fa, MARGIN = 1, FUN = function(x) sum(is.na(x)) > as.integer(ncol(Fa) * 0.99))
  Fa = Fa[!rows_with_na, ]

  # remove zero values
  columns_to_remove <- apply(Fa, 2, check_zero_proportion, 0.2)
  Fa = Fa[, !columns_to_remove]

  # Forward returns
  R_forward = as.xts.data.table(dcast(dtm_filter_sample_, year_month_id ~ symbol, value.var = "ret_forward"))
  R_forward = R_forward[, !cols_with_na]
  R_forward = R_forward[!rows_with_na, ]
  R_forward = R_forward[, !columns_to_remove]

  # uncondtitional sorting
  dimA = 0:10/10
  # dimA = c(0.1, 0.02, 0.01, 0.2, 0.5, 0.7, 0.9, 0.98, 1)
  psort_out = tryCatch({
    sort.output.uncon = unconditional.sort(Fa,
                                           Fb=NULL,
                                           Fc=NULL,
                                           R_forward,
                                           dimA=dimA,
                                           dimB=NULL,
                                           dimC=NULL,
                                           type = 7)

  }, error = function(e) NULL)
  if (is.null(psort_out)) {
    print(paste0("Remove variable ", p))
    return(NULL)
  } else {
    return(sort.output.uncon)
  }
  # table.AnnualizedReturns(sort.output.uncon$returns)
})

# extract CAR an SR
index_keep = sapply(portsort_results_l, function(x) !is.null(x))
portsort_l <- portsort_results_l[index_keep]
portsort_l = lapply(portsort_l, function(x) table.AnnualizedReturns(x$returns))
portsort_l = lapply(portsort_l, function(x) as.data.table(x, keep.rownames = TRUE))
names(portsort_l) = predictors_cleaned[index_keep]
portsort_dt = rbindlist(portsort_l, idcol = "predictor")

# save data
fwrite(portsort_dt, "data/port_sort.csv")

# import resulsts
# portsort_dt = fread("data/port_sort.csv")
# setnames(portsort_dt, c("V1", "V2"), c("predictor", "rn"))
# portsort_dt = portsort_dt[-1]

# check for exact monotinicity
sr_meta = portsort_dt[rn == "Annualized Sharpe (Rf=0%)"]
sr_meta[apply(sr_meta[, 3:ncol(portsort_dt)], 1, function(x) any(x > .9))]
sr = sr_meta[rn == "Annualized Sharpe (Rf=0%)", 3:ncol(sr_meta)]
apply(sr, 1, function(x) all(x == cummax(x)))
sr_meta[apply(sr, 1, function(x) all(x == cummax(x)))]
apply(sr, 1, function(x) all(x == cummin(x)))
sr_meta[apply(sr, 1, function(x) all(x == cummin(x)))]
sr_meta[apply(sr, 1, function(x) all(cummax(x) - x < 0.05))]
sr_meta[apply(sr, 1, function(x) all(x - cummin(x) < 0.05))]


# PORTSORT 2 PREDICTORS ---------------------------------------------------
# make function that returns results for every predictor
dtm_filter_sample = dtm_filter[year_month_id > as.Date("2001-01-01")]

# make 2R predictors space
predictors_2d = c(sr_meta[apply(sr, 1, function(x) all(cummax(x) - x < 0.05)), "predictor"],
                  sr_meta[apply(sr, 1, function(x) all(x - cummin(x) < 0.05)), "predictor"],
                  sr_meta[apply(sr_meta[, 3:ncol(portsort_dt)], 1, function(x) any(x > .9)), "predictor"])
predictors_2d = unique(unlist(predictors_2d, use.names = FALSE))
predictors_2d = c(setdiff(predictors_2d, predictors_2d[grep("sar_", predictors_2d)]),
                  "sar_5")
predictors_2d = t(combn(predictors_2d, 2))
# predictors_2d = predictors_2d[predictors_2d[, 1] != predictors_2d[, 2]]
# predictors_cleaned_use_2d = expand.grid(predictors_2d,
#                                         predictors_2d,
#                                         stringsAsFactors = FALSE)
portsort_results_2d_l = lapply(1:nrow(predictors_2d), function(i) {
  # debug
  # i = 38
  print(i)

  # sample only relevant data
  p1 = predictors_2d[i, 1]
  # p1 = "purchasesOfInvestments"
  p2 = predictors_2d[i, 2]
  # p2 = "cashFlowCoverageRatios"
  cols = c("symbol", "year_month_id", "ret_forward", p1, p2)
  dtm_filter_sample_ = dtm_filter_sample[, ..cols]

  # predictors matrix
  Fa = dcast(dtm_filter_sample_, year_month_id ~ symbol, value.var = p1)
  Fb = dcast(dtm_filter_sample_, year_month_id ~ symbol, value.var = p2)
  setorder(Fa, year_month_id)
  setorder(Fb, year_month_id)
  Fa = as.xts.data.table(Fa)
  Fb = as.xts.data.table(Fb)

  # remove all na values from both
  cols_with_na_fa <- apply(Fa, MARGIN = 2, FUN = function(x) sum(is.na(x)) > as.integer(nrow(Fa) * 0.6))
  cols_with_na_fb <- apply(Fb, MARGIN = 2, FUN = function(x) sum(is.na(x)) > as.integer(nrow(Fb) * 0.6))
  cols_with_na = cols_with_na_fa | cols_with_na_fb
  Fa = Fa[, !cols_with_na]
  Fb = Fb[, !cols_with_na]

  # remove all NA values
  rows_with_na_fa <- apply(Fa, MARGIN = 1, FUN = function(x) sum(is.na(x)) > as.integer(ncol(Fa) * 0.99))
  rows_with_na_fb <- apply(Fb, MARGIN = 1, FUN = function(x) sum(is.na(x)) > as.integer(ncol(Fb) * 0.99))
  rows_with_na = rows_with_na_fa | rows_with_na_fb
  Fa = Fa[!rows_with_na, ]
  Fb = Fb[!rows_with_na, ]

  # remove zero values
  columns_to_remove_fa <- apply(Fa, 2, check_zero_proportion, 0.2)
  columns_to_remove_fb <- apply(Fb, 2, check_zero_proportion, 0.2)
  columns_to_remove = columns_to_remove_fa | columns_to_remove_fb
  Fa = Fa[, !columns_to_remove]
  Fb = Fb[, !columns_to_remove]

  # Forward returns
  R_forward = as.xts.data.table(dcast(dtm_filter_sample_, year_month_id ~ symbol, value.var = "ret_forward"))
  R_forward = R_forward[, !cols_with_na]
  R_forward = R_forward[!rows_with_na, ]
  R_forward = R_forward[, !columns_to_remove]

  # uncondtitional sorting
  dimA = 0:5/5
  dimB = 0:5/5
  psort_out = tryCatch({
    sort.output.uncon = unconditional.sort(Fa,
                                           Fb=Fb,
                                           Fc=NULL,
                                           R_forward,
                                           dimA=dimA,
                                           dimB=dimB,
                                           dimC=NULL,
                                           type = 7)

  }, error = function(e) NULL)
  if (is.null(psort_out)) {
    print(paste0("Remove variable ", p1, " and ", p2))
    return(NULL)
  } else {
    return(sort.output.uncon)
  }
  # table.AnnualizedReturns(sort.output.uncon$returns)
})

# extract CAR an SR
portsort_2d_l = lapply(portsort_results_2d_l, function(x) table.AnnualizedReturns(x$returns))
portsort_2d_l = lapply(portsort_2d_l, function(x) as.data.table(x, keep.rownames = TRUE))
names_ = paste(predictors_2d[, 1], predictors_2d[, 2], sep =  "_")
names(portsort_2d_l) = names_
portsort_2d_dt = rbindlist(portsort_2d_l, idcol = "predictor")

# save 2D data
fwrite(portsort_2d_dt, "data/port_sort_2d.csv")

# # import resulsts
# portsort_2d_dt = fread("data/port_sort_2d.csv")
# setnames(portsort_2d_dt, c("V1", "V2"), c("predictor", "rn"))
# portsort_2d_dt = portsort_2d_dt[-1]

# check for monotinicity
sr_meta_2d = portsort_2d_dt[rn == "Annualized Sharpe (Rf=0%)"]
sr_threshold = 1.1
index_best = which(apply(sr_meta_2d[, 3:ncol(portsort_2d_dt)], 1,
                         function(x) any(x > sr_threshold | x < -sr_threshold)))
sr_meta_2d[index_best]


intersect(c(0.5, 0.8), c(0.1, 0.9))

#
portsort_results_2d_l[[36]]
portsort_results_2d_l[[36]]$portfolios
portsort_results_2d_l[[36]]$returns
portsort_results_2d_l[[36]]$portfolios
portsort_results_2d_l[[36]]$portfolios[[1]]


# ANALYSE SPECIFIC VARIABLES ----------------------------------------------
# BBANDS
new_cols <- expand.grid("bbands", c("dn", "mavg", "up", "pctB"))
new_cols <- paste(new_cols$Var1, new_cols$Var2, sep = "_")
dt[, (new_cols) := as.data.frame(BBands(close, n = 5)), by = symbol]
new_cols_change <- new_cols[grep("bbands.*up|bbands.*mavg|bbands.*dn", new_cols)]
dt[, (new_cols_change) := lapply(.SD, function(x) close / x), .SDcols = new_cols_change]

