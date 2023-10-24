library(data.table)
library(arrow)
library(lubridate)
library(portsort)
library(PerformanceAnalytics)



# SETUP -------------------------------------------------------------------
# Paths
URIFACTORS = "F:/equity/usa/predictors-daily/factors"


# UTILS -------------------------------------------------------------------
# Function to check if more than 50% of a column's values are zeroes
check_zero_proportion <- function(column, prop = 0.5) {
  zero_count <- sum(column == 0, na.rm = TRUE)
  total_count <- length(column)
  return(zero_count / total_count > prop)
}

# MARKET DATA AND FUNDAMENTALS ---------------------------------------------
# import factors
fundamentals = read_parquet(file.path(URIFACTORS, "fundamental_factors.parquet"))
prices = read_parquet(file.path(URIFACTORS, "prices_factors.parquet"))
macros = read_parquet(file.path(URIFACTORS, "macro_factors.parquet"))

# create month colummn
prices[, year_month_id := ceiling_date(date, unit = "month") - days(1)]
colnames(prices)

# add variables
prices[, dollar_volume := volume * close]

# predictor columns
predictors_prices = colnames(prices)[c(3:37, 44:99)]
predictors_prices = setdiff(predictors_prices,
                            c("sector", "industry", "market_returns",
                              "dollar_volume"))
predictors_fund = colnames(fundamentals)
predictors_fund = setdiff(predictors_fund, c("symbol", "date", "reportedCurrency",
                                             "fillingDate", "cik", "acceptedDate",
                                             "calendarYear", "period"))
predictors = c(predictors_prices, predictors_fund)

# downsample to monthly frequency
prices_m = prices[, .(
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

# keep lat observation by symbol and date for all predictors
prices[, .(symbol, date)] # ordered as expected
prices_predictors = unique(prices, by = c("symbol", "year_month_id"), fromLast = TRUE)
cols = c("symbol", "year_month_id", predictors_prices)
prices_predictors = prices_predictors[, ..cols]

# merge predictors by month and prices_m
proces_m_all = merge(prices_m, prices_predictors, by = c("symbol", "year_month_id"))

# add fundamentals
proces_m_all[, year_month_id_ := year_month_id]
fundamentals[, fillingDate_ := fillingDate]
proces_m_all = fundamentals[proces_m_all, on = c("symbol", "fillingDate_" = "year_month_id_"), roll = Inf]
proces_m_all[, fillingDate_ := NULL]
proces_m_all[, .(symbol, date, fillingDate, year_month_id)]

# create forward returns
proces_m_all[, ret_forward := shift(close, -1, type = "shift") / close - 1, by = symbol]
proces_m_all[, .(symbol, year_month_id, close, ret_forward)]

# checl for duplicates
dup_index = which(proces_m_all[, duplicated(.SD[, .(symbol, year_month_id)])])
proces_m_all = unique(proces_m_all, by = c("symbol", "year_month_id"))

# select cols
cols_ = c("symbol", "year_month_id", "ret_forward", "dollar_volume_mean", predictors)
proces_m_all = proces_m_all[, ..cols_]
# dtm = na.omit(dtm)

# # remove assets with price < x$
# min_price = 1
# x = x[, .SD[close > min_price], by = year_month_id]

# filter 500 with highest volume
filter_var = "dollar_volume_mean" # PARAMETER
num_coarse = 2000                  # PARAMETER
setorderv(proces_m_all, c("year_month_id", filter_var), order = 1L)
proces_m_all_filter = proces_m_all[, tail(.SD, num_coarse), by = year_month_id]

# remove observations with many NA's
threshold = 0.1
na_cols <- sapply(proces_m_all_filter, function(x) sum(is.na(x))/length(x) > threshold)
print(na_cols[na_cols == TRUE])
print(na_cols[na_cols == FALSE])
predictors_cleaned = setdiff(predictors, names(na_cols[na_cols == TRUE]))
cols = c("symbol", "year_month_id", "ret_forward", predictors_cleaned)
prices_cleaned = proces_m_all[, ..cols]

# remove NA values for target
prices_cleaned = na.omit(prices_cleaned, cols = "ret_forward")

# winsorize extreme values
predictors_reduced = intersect(predictors, colnames(prices_cleaned))
prices_cleaned[, (predictors_reduced) := lapply(.SD, as.numeric), .SDcols = predictors_reduced]
prices_cleaned[, (predictors_reduced) := lapply(.SD, function(x) DescTools::Winsorize(x, probs = c(0.01, 0.99), na.rm = TRUE)),
               by = year_month_id, .SDcols = predictors_reduced]

# visualize some vars
var_ = sample(predictors_cleaned, 1)
# var_ = "illiquidity"
date_ = prices_cleaned[, sample(year_month_id, 1)]
x = prices_cleaned[year_month_id == date_, ..var_]
hist(unlist(x), main=var_, xlab="dots", ylab=var_)

# make function that returns results for every predictor
prices_cleaned_sample = prices_cleaned[year_month_id > as.Date("2005-01-01")]
portsort_results_l = lapply(predictors_cleaned, function(p) {
  # debug
  # p = "mom12m_lag"
  print(p)

  # sample only relevant data
  cols = c("symbol", "year_month_id", "ret_forward", p)
  prices_cleaned_sample_ = prices_cleaned_sample[, ..cols]

  # predictors matrix
  Fa = dcast(prices_cleaned_sample_, year_month_id ~ symbol, value.var = p)
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
  R_forward = as.xts.data.table(dcast(prices_cleaned_sample_, year_month_id ~ symbol, value.var = "ret_forward"))
  R_forward = R_forward[, !cols_with_na]
  R_forward = R_forward[!rows_with_na, ]
  R_forward = R_forward[, !columns_to_remove]

  # uncondtitional sorting
  dimA = 0:6/6
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
portsort_dt

# check for exact monotinicity
sr_meta = portsort_dt[rn == "Annualized Sharpe (Rf=0%)"]
sr_meta[apply(sr_meta[, 3:ncol(portsort_dt)], 1, function(x) any(x > 1))]
sr = sr_meta[rn == "Annualized Sharpe (Rf=0%)", 3:ncol(sr_meta)]
apply(sr, 1, function(x) all(x == cummax(x)))
sr_meta[apply(sr, 1, function(x) all(x == cummax(x)))]
apply(sr, 1, function(x) all(x == cummin(x)))
sr_meta[apply(sr, 1, function(x) all(x == cummin(x)))]
sr_meta[apply(sr, 1, function(x) all(cummax(x) - x < 0.1))]
sr_meta[apply(sr, 1, function(x) all(x - cummin(x) < 0.1))]

# test
# labelsA <- seq(1, length(dimA) - 1, 1)
# t = 1
# for (t in 1:nrow(Fa)) {
#   print(t)
#   breaks = c(quantile(Fa[t, ], probs = dimA, na.rm = TRUE, type = 7))
# }
# for (t in 1:nrow(Fa)) {
#   print(t)
#   Hold.Fa = t(as.matrix(as.numeric(cut(Fa[t, ],
#                                        breaks = c(quantile(Fa[t, ], probs = dimA, na.rm = TRUE, type = 7)), labels = labelsA,
#                                        include.lowest = TRUE))))
# }
# dim(Fa[t, ])
# Fa[t, ][, !is.na(Fa[t, ])]
# x = Fa[t, ][, !is.na(Fa[t, ])]
# x[, 1:58]



# PORTSORT 2 PREDICTORS ---------------------------------------------------
# make function that returns results for every predictor
prices_cleaned_sample = prices_cleaned[year_month_id > as.Date("2005-01-01")]

# make 2R predictors space
predictors_2d = c(sr_meta[apply(sr, 1, function(x) all(cummax(x) - x < 0.1)), "predictor"],
                  sr_meta[apply(sr, 1, function(x) all(x - cummin(x) < 0.1)), "predictor"])
predictors_2d = unlist(predictors_2d, use.names = FALSE)
predictors_2d = t(combn(predictors_2d, 2))
# predictors_2d = predictors_2d[predictors_2d[, 1] != predictors_2d[, 2]]
# predictors_cleaned_use_2d = expand.grid(predictors_2d,
#                                         predictors_2d,
#                                         stringsAsFactors = FALSE)
portsort_results_2d_l = lapply(1:nrow(predictors_2d), function(i) {
  # debug
  # i = 1
  print(i)

  # sample only relevant data
  p1 = predictors_2d[i, 1]
  # p1 = "purchasesOfInvestments"
  p2 = predictors_2d[i, 2]
  # p2 = "cashFlowCoverageRatios"
  cols = c("symbol", "year_month_id", "ret_forward", p1, p2)
  prices_cleaned_sample_ = prices_cleaned_sample[, ..cols]

  # predictors matrix
  Fa = dcast(prices_cleaned_sample_, year_month_id ~ symbol, value.var = p1)
  Fb = dcast(prices_cleaned_sample_, year_month_id ~ symbol, value.var = p2)
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
  R_forward = as.xts.data.table(dcast(prices_cleaned_sample_, year_month_id ~ symbol, value.var = "ret_forward"))
  R_forward = R_forward[, !cols_with_na]
  R_forward = R_forward[!rows_with_na, ]
  R_forward = R_forward[, !columns_to_remove]

  # uncondtitional sorting
  dimA = 0:6/6
  dimB = 0:6/6
  sort.output.uncon = unconditional.sort(Fa,
                                         Fb=Fb,
                                         Fc=NULL,
                                         R_forward,
                                         dimA=dimA,
                                         dimB=dimB,
                                         dimC=NULL,
                                         type = 7)
  # table.AnnualizedReturns(sort.output.uncon$returns)
  return(sort.output.uncon)
})

# extract CAR an SR
portsort_2d_l = lapply(portsort_results_2d_l, function(x) table.AnnualizedReturns(x$returns))
portsort_2d_l = lapply(portsort_2d_l, function(x) as.data.table(x, keep.rownames = TRUE))
names_ = paste(predictors_2d[, 1], predictors_2d[, 2], sep =  "_")
names(portsort_2d_l) = names_
portsort_2d_dt = rbindlist(portsort_2d_l, idcol = "predictor")

# check for monotinicity
sr_meta = portsort_2d_dt[rn == "Annualized Sharpe (Rf=0%)"]
index_best = which(apply(sr_meta[, 3:ncol(portsort_dt)], 1, function(x) any(x > 2)))
sr_meta[index_best]

#
portsort_results_2d_l[[36]]
portsort_results_2d_l[[36]]$portfolios
