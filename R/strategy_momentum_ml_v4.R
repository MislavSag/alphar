library(data.table)
library(lubridate)
library(QuantTools)
library(mlr3verse)



# SET UP ------------------------------------------------------------------
# globals
DATAPATH     = "F:/lean_root/data/all_stocks_daily.csv"


# IMPORT DATA -------------------------------------------------------------
# import QC daily data
col = c("date", "open", "high", "low", "close", "volume", "close_adj", "symbol")
dt = fread(DATAPATH, col.names = col)
dt <- unique(dt, by = c("symbol", "date"))
unadjustd_cols = c("open", "high", "low")
adjusted_cols = paste0(unadjustd_cols, "_adj")
dt[, (adjusted_cols) := lapply(.SD, function(x) (close_adj / close) * x), .SDcols = unadjustd_cols]
dt = na.omit(dt)
setorder(dt, symbol, date)

# free resources
gc()


# PREDICTORS --------------------------------------------------------------
# create help colummns
dt[, year_month_id := ceiling_date(date, unit = "month") - days(1)]

# remove negative prices
dt = dt[close > 0 & close_adj > 0]

# add variables
dt[, dollar_volume := volume * close]

# PRA window sizes
windows_ = c(5, 22, 22 * 3, 22 * 6, 252, 252 * 2, 252 * 4)

# calculate PRA predictors
pra_predictors = paste0("pra_", windows_)
dt[, (pra_predictors) := lapply(windows_,
                                function(w) roll_percent_rank(close_adj, w)),
   by = symbol]

# calculate momentum indicators
setorder(dt, symbol, date)
moms = seq(21, 21 * 12, 21)
mom_cols = paste0("mom", 1:12)
dt[, (mom_cols) := lapply(moms, function(x) close_adj  / shift(close_adj , x) - 1), by = symbol]
moms = seq(21 * 2, 21 * 12, 21)
mom_cols_lag = paste0("mom_lag", 2:12)
dt[, (mom_cols_lag) := lapply(moms, function(x) shift(close_adj, 21) / shift(close_adj , x) - 1), by = symbol]

# create target var
moms_target = c(5, 10, 21, 42, 63)
moms_target_cols = paste0("mom_target_", c("w", "2w", "m", "2m", "3m"))
dt[, (moms_target_cols) := lapply(moms_target, function(x) (shift(close_adj , -x, "shift") / close_adj ) - 1),
   by = symbol]

# order data
setorder(dt, symbol, date)


# TASKS --------------------------------------------------------------------
# convert date to PosixCt because it is requireed by mlr3
dt[, date := as.POSIXct(date, tz = "UTC")]
dt[, year_month_id := as.numeric(year_month_id)]
dt[, dollar_volume := as.numeric(dollar_volume)]
dt[, volume := as.numeric(volume)]

# help to choose columns
id_cols = c("symbol", "date", "year_month_id")
cols_features = colnames(dt)[grep("mom\\d+|mom_lag|dollar|close|pra", colnames(dt))]
print(colnames(dt)[grep("^mom_target", colnames(dt))])

# task with future week returns as target
print(colnames(dt)[grep("^mom_target", colnames(dt))])
target_ = "mom_target_m"
cols_ = c(id_cols, target_, cols_features)
task = na.omit(dt, cols = cols_)
task <- as_task_regr(task,
                     id = "task",
                     target = target_)

# set roles for symbol, date and yearmonth_id
task$col_roles$feature = setdiff(task$col_roles$feature, id_cols)


# CROSS VALIDATIONS -------------------------------------------------------
print("Cross validations")

# create train, tune and test set
nested_cv_split = function(task,
                           train_length = 12,
                           tune_length = 1,
                           test_length = 1) {

  # create cusom CV's for inner and outer sampling
  custom_inner = rsmp("custom")
  custom_outer = rsmp("custom")

  # get year month id data
  # task = task_ret_week$clone()
  task_ = task$clone()
  yearmonthid_ = task_$backend$data(cols = c("yearmonthid", "..row_id"),
                                    rows = 1:task_$nrow)
  stopifnot(all(task_$row_ids == yearmonthid_$`..row_id`))
  groups_v = yearmonthid_[, unlist(unique(yearmonthid))]

  # util vars
  start_folds = 1:(length(groups_v)-train_length-tune_length-test_length)
  get_row_ids = function(mid) unlist(yearmonthid_[yearmonthid %in% mid, 2], use.names = FALSE)

  # create train data
  train_groups <- lapply(start_folds,
                         function(x) groups_v[x:(x+train_length-1)])
  train_sets <- lapply(train_groups, get_row_ids)

  # create tune set
  tune_groups <- lapply(start_folds,
                        function(x) groups_v[(x+train_length):(x+train_length+tune_length-1)])
  tune_sets <- lapply(tune_groups, get_row_ids)

  # test train and tune
  test_1 = vapply(seq_along(train_groups), function(i) {
    mondf(
      tail(as.Date(train_groups[[i]], origin = "1970-01-01"), 1),
      head(as.Date(tune_groups[[i]], origin = "1970-01-01"), 1)
    )
  }, FUN.VALUE = numeric(1L))
  stopifnot(all(test_1 == 1))
  test_2 = vapply(seq_along(train_groups), function(i) {
    unlist(head(tune_sets[[i]], 1) - tail(train_sets[[i]], 1))
  }, FUN.VALUE = numeric(1L))
  stopifnot(all(test_2 == 1))

  # create test sets
  insample_length = train_length + tune_length
  test_groups <- lapply(start_folds,
                        function(x) groups_v[(x+insample_length):(x+insample_length+test_length-1)])
  test_sets <- lapply(test_groups, get_row_ids)

  # test tune and test
  test_3 = vapply(seq_along(train_groups), function(i) {
    mondf(
      tail(as.Date(tune_groups[[i]], origin = "1970-01-01"), 1),
      head(as.Date(test_groups[[i]], origin = "1970-01-01"), 1)
    )
  }, FUN.VALUE = numeric(1L))
  stopifnot(all(test_1 == 1))
  test_4 = vapply(seq_along(train_groups), function(i) {
    unlist(head(test_sets[[i]], 1) - tail(tune_sets[[i]], 1))
  }, FUN.VALUE = numeric(1L))
  stopifnot(all(test_2 == 1))

  # create inner and outer resamplings
  custom_inner$instantiate(task, train_sets, tune_sets)
  inner_sets = lapply(seq_along(train_groups), function(i) {
    c(train_sets[[i]], tune_sets[[i]])
  })
  custom_outer$instantiate(task, inner_sets, test_sets)
  return(list(custom_inner = custom_inner, custom_outer = custom_outer))
}

# generate cv's
train_sets = seq(12, 12 * 3, 12)
validation_sets = train_sets / 12
custom_cvs = list()
for (i in seq_along(train_sets)) {
  print(i)
  custom_cvs[[i]] = nested_cv_split(task_ret_week,
                                    train_sets[[i]],
                                    validation_sets[[i]],
                                    1)
}
