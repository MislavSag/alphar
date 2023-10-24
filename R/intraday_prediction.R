library(data.table)
library(arrow)
library(rvest)
library(lubridate)
library(rtsplot)
library(paradox)
library(mlr3)
library(mlr3pipelines)
library(mlr3viz)
library(mlr3tuning)
library(mlr3misc)


# SET UP ------------------------------------------------------------------
# globals
NASPATH      = "C:/Users/Mislav/SynologyDrive/equity/usa/minute-adjusted"
MLR3SAVEPATH = "F:/mlr3output/intraday"


# IMPORT DATA -------------------------------------------------------------
# minute files
files = list.files(NASPATH, full.names = TRUE)

# sample files
files_sample = sample(files, 20)
# files_sample = c(files[grep("SPY", files)], files_sample)

# create symbols from the path
symbols = gsub(".*/|.parquet", "", files_sample)

# import sample minute data
ohlcv = lapply(files_sample, read_parquet)
names(ohlcv) = symbols
ohlcv = rbindlist(ohlcv, idcol = "symbol")

# change time zone
ohlcv[, date := with_tz(date, "America/New_York")]

# unique by symbol and date
ohlcv <- unique(ohlcv, by = c("symbol", "date"))

# remove NA values
ohlcv = na.omit(ohlcv)

# order
setorder(ohlcv, symbol, date)

# create group variable
ohlcv[, yearmonthid := as.IDate(date)]
ohlcv[, yearmonthid := round(yearmonthid, digits = "month")]
ohlcv[, yearmonthid := as.integer(yearmonthid)]

# sample visualizations
for (s in symbols) {
  rtsplot(as.xts.data.table(ohlcv[symbol == s, .(date, close)][seq(1, .N, 60)]), main = s)
  Sys.sleep(2L)
}


# LABELING ----------------------------------------------------------------
# create simple one minute forward one step return
ohlcv[, target_1 := shift(close, -1, type = "shift") / close - 1, by = symbol]


# GENERATE PREDICTORS -----------------------------------------------------
# . For each of the individual stocks, the predictors zt are the lagged returns
# of the S&P 500 and its constituents; the same set of predictors used
# for forecasting the market portfolio.
ohlcv[, returns := log(close) - shift(log(close)), by = symbol]

# cross aseets returns as predictors
dataset = ohlcv[, .(symbol, date, returns)]
dataset = dcast(dataset, date ~ symbol, value.var = "returns")

# merge returns predictors and id data
dataset = dataset[ohlcv[, .(symbol, date, yearmonthid, target_1)], on = "date"]

# define predictor columns
predictors = colnames(dataset)[2:(which(colnames(dataset) == "symbol") - 1)]

# rearrange columns
cols = c("symbol", "date", "yearmonthid", "target_1", predictors)
dataset = dataset[, ..cols]


# CLEAN DATA --------------------------------------------------------------
# sort
setorder(dataset, date, symbol)

# remove observations with all NA
# TODO : optimize this line, maybe with rowsums
all_na = dataset[, apply(.SD, 1, function(x) all(is.na(x))), .SDcols = predictors]
dataset = dataset[!all_na]

# sort
setorder(dataset, date, symbol)



# TASKS -------------------------------------------------------------------
print("Tasks")

# id coluns we always keep
id_cols = c("symbol", "date", "yearmonthid")

# task with future week returns as target
target_ = colnames(dataset)[grep("target", colnames(dataset))]
cols_ = c(id_cols, target_, predictors)
task_minute <- as_task_regr(dataset[, ..cols_],
                            id = "task_minute",
                            target = target_)

# set roles for symbol, date and yearmonth_id
task_minute$col_roles$feature = setdiff(task_minute$col_roles$feature,
                                        id_cols)


# CROSS VALIDATIONS -------------------------------------------------------
print("Cross validations")

# utils https://stackoverflow.com/questions/1995933/number-of-months-between-two-dates
monnb <- function(d) {
  lt <- as.POSIXlt(as.Date(d, origin="1900-01-01"))
  lt$year*12 + lt$mon }
mondf <- function(d1, d2) { monnb(d2) - monnb(d1) }

# create train, tune and test set
nested_cv_split = function(task,
                           train_length = 10,
                           tune_length = 1,
                           test_length = 1) {

    # create cusom CV's for inner and outer sampling
  custom_inner = rsmp("custom")
  custom_outer = rsmp("custom")

  # get year month id data
  # task = task_minute$clone()
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

# create custom cross validation
custom_cvs = nested_cv_split(task_minute)
custom_inner = custom_cvs$custom_inner
custom_outer = custom_cvs$custom_outer

# test set start after train set
test1 = all(vapply(1:custom_inner$iters, function(i) {
  (tail(custom_inner$train_set(i), 1) + 1) == custom_inner$test_set(i)[1]
}, FUN.VALUE = logical(1L)))

# train set in outersample contains ids in innersample 1
test2  = all(vapply(1:custom_inner$iters, function(i) {
  all(c(custom_inner$train_set(i),
        custom_inner$test_set(i)) == custom_outer$train_set(i))
}, FUN.VALUE = logical(1L)))
c(test1, test2)


# ADD PIPELINES -----------------------------------------------------------
# source pipes, filters and other
source("R/mlr3_winsorization.R")
source("R/mlr3_uniformization.R")
source("R/mlr3_gausscov_f1st.R")
source("R/mlr3_gausscov_f3st.R")
source("R/mlr3_dropna.R")
source("R/mlr3_dropnacol.R")
source("R/mlr3_filter_drop_corr.R")
source("R/mlr3_winsorizationsimple.R")
source("R/mlr3_winsorizationsimplegroup.R")
source("R/PipeOpPCAExplained.R")
# measures
source("R/Linex.R")
source("R/PortfolioRet.R")
source("R/AdjLoss2.R")

# add my pipes to mlr dictionary
mlr_pipeops$add("uniformization", PipeOpUniform)
mlr_pipeops$add("winsorize", PipeOpWinsorize)
mlr_pipeops$add("winsorizesimple", PipeOpWinsorizeSimple)
mlr_pipeops$add("winsorizesimplegroup", PipeOpWinsorizeSimpleGroup)
mlr_pipeops$add("dropna", PipeOpDropNA)
mlr_pipeops$add("dropnacol", PipeOpDropNACol)
mlr_pipeops$add("dropcorr", PipeOpDropCorr)
mlr_pipeops$add("pca_explained", PipeOpPCAExplained)
mlr_filters$add("gausscov_f1st", FilterGausscovF1st)
mlr_filters$add("gausscov_f3st", FilterGausscovF3st)
mlr_measures$add("linex", Linex)
mlr_measures$add("portfolio_ret", PortfolioRet)
mlr_measures$add("adjloss2", AdjLoss2)


# GRAPH ----------------------------------------------------------------
# graph template
graph_template =
  po("subsample") %>>% # uncomment this for hyperparameter tuning
  po("dropnacol", id = "dropnacol", cutoff = 0.05) %>>%
  po("dropna", id = "dropna") %>>%
  po("removeconstants", id = "removeconstants_1", ratio = 0)  %>>%
  po("fixfactors", id = "fixfactors") %>>%
  # po("winsorizesimple", id = "winsorizesimple", probs_low = 0.01, probs_high = 0.99, na.rm = TRUE) %>>%
  po("winsorizesimplegroup", group_var = "yearmonthid", id = "winsorizesimplegroup", probs_low = 0.01, probs_high = 0.99, na.rm = TRUE) %>>%
  po("removeconstants", id = "removeconstants_2", ratio = 0)  %>>%
  po("dropcorr", id = "dropcorr", cutoff = 0.99) %>>%
  # po("uniformization") %>>%
  po("dropna", id = "dropna_v2") %>>%
  # filters
  # po("branch", options = c("jmi", "gausscov", "nop_filter"), id = "filter_branch") %>>%
  # gunion(list(po("filter", filter = flt("jmi"), filter.frac = 0.05),
  #             po("filter", filter = flt("gausscov_f1st"), filter.cutoff = 0),
  #             po("nop", id = "nop_filter"))) %>>%
  # po("unbranch", id = "filter_unbranch") %>>%
  # modelmatrix
  po("removeconstants", id = "removeconstants_3", ratio = 0)

# hyperparameters template
search_space_template = ps(
  # subsample for hyperband
  subsample.frac = p_dbl(0.3, 1, tags = "budget"), # unccoment this if we want to use hyperband optimization
  # preprocessing
  dropcorr.cutoff = p_fct(
    levels = c("0.80", "0.90", "0.95", "0.99"),
    trafo = function(x, param_set) {
      switch(x,
             "0.80" = 0.80,
             "0.90" = 0.90,
             "0.95" = 0.95,
             "0.99" = 0.99)
    }
  ),
  # dropcorr.cutoff = p_fct(levels = c(0.8, 0.9, 0.95, 0.99)),
  winsorizesimplegroup.probs_high = p_fct(levels = c(0.999, 0.99, 0.98, 0.97, 0.90, 0.8)),
  winsorizesimplegroup.probs_low = p_fct(levels = c(0.001, 0.01, 0.02, 0.03, 0.1, 0.2))
)

# random forest graph
graph_rf = graph_template %>>%
  po("learner", learner = lrn("regr.ranger"))
plot(graph_rf)
graph_rf = as_learner(graph_rf)
as.data.table(graph_rf$param_set)[, .(id, class, lower, upper, levels)]
search_space_rf = search_space_template$clone()
search_space_rf$add(
  ps(regr.ranger.max.depth  = p_int(1, 40),
     regr.ranger.replace    = p_lgl(),
     regr.ranger.mtry.ratio = p_dbl(0.1, 1))
)

# xgboost graph
graph_xgboost = graph_template %>>%
  po("learner", learner = lrn("regr.xgboost"))
plot(graph_xgboost)
graph_xgboost = as_learner(graph_xgboost)
as.data.table(graph_xgboost$param_set)[grep("depth", id), .(id, class, lower, upper, levels)]
search_space_xgboost = ps(
  # subsample for hyperband
  subsample.frac = p_dbl(0.3, 1, tags = "budget"), # unccoment this if we want to use hyperband optimization
  # preprocessing
  dropcorr.cutoff = p_fct(
    levels = c("0.80", "0.90", "0.95", "0.99"),
    trafo = function(x, param_set) {
      switch(x,
             "0.80" = 0.80,
             "0.90" = 0.90,
             "0.95" = 0.95,
             "0.99" = 0.99)
    }
  ),
  # dropcorr.cutoff = p_fct(levels = c(0.8, 0.9, 0.95, 0.99)),
  winsorizesimplegroup.probs_high = p_fct(levels = c(0.999, 0.99, 0.98, 0.97, 0.90, 0.8)),
  winsorizesimplegroup.probs_low = p_fct(levels = c(0.001, 0.01, 0.02, 0.03, 0.1, 0.2)),

    # learner
  regr.xgboost.alpha     = p_dbl(0.001, 100, logscale = TRUE),
  regr.xgboost.max_depth = p_int(1, 20),
  regr.xgboost.eta       = p_dbl(0.0001, 1, logscale = TRUE),
  regr.xgboost.nrounds   = p_int(1, 5000)
)

# BART graph
graph_bart = graph_template %>>%
  po("learner", learner = lrn("regr.bart"))
graph_bart = as_learner(graph_bart)
as.data.table(graph_bart$param_set)[, .(id, class, lower, upper, levels)]
search_space_bart = search_space_template$clone()
search_space_bart$add(
  ps(regr.bart.k = p_int(lower = 1, upper = 10),
     regr.bart.nu = p_dbl(lower = 0.1, upper = 10),
     regr.bart.n_trees = p_int(lower = 10, upper = 100))
)
# chatgpt returns this
# n_trees = p_int(lower = 10, upper = 100),
# n_chains = p_int(lower = 1, upper = 5),
# k = p_int(lower = 1, upper = 10),
# m_try = p_int(lower = 1, upper = 13),
# nu = p_dbl(lower = 0.1, upper = 10),
# alpha = p_dbl(lower = 0.01, upper = 1),
# beta = p_dbl(lower = 0.01, upper = 1),
# burn = p_int(lower = 10, upper = 100),
# iter = p_int(lower = 100, upper = 1000)

# nnet graph
graph_nnet = graph_template %>>%
  po("learner", learner = lrn("regr.nnet"))
graph_nnet = as_learner(graph_nnet)
as.data.table(graph_nnet$param_set)[, .(id, class, lower, upper, levels)]
search_space_nnet = search_space_template$clone()
search_space_nnet$add(
  ps(regr.nnet.size = p_int(lower = 5, upper = 50),
     regr.nnet.decay = p_dbl(lower = 0.0001, upper = 0.1),
     regr.nnet.maxit = p_int(lower = 50, upper = 500))
)

# lightgbm graph
graph_lightgbm = graph_template %>>%
  po("learner", learner = lrn("regr.lightgbm"))
graph_lightgbm = as_learner(graph_lightgbm)
as.data.table(graph_lightgbm$param_set)[grep("sample", id), .(id, class, lower, upper, levels)]
search_space_lightgbm = search_space_template$clone()
search_space_lightgbm$add(
  ps(regr.lightgbm.max_depth     = p_int(lower = 2, upper = 10),
     regr.lightgbm.learning_rate = p_dbl(lower = 0.001, upper = 0.3),
     regr.lightgbm.num_leaves    = p_int(lower = 10, upper = 100))
)

# # threads
# threads = as.integer(Sys.getenv("NCPUS"))
# set_threads(graph_rf, n = threads)
# set_threads(graph_xgboost, n = threads)
# set_threads(graph_bart, n = threads)
# set_threads(graph_nnet, n = threads)
# set_threads(graph_lightgbm, n = threads)


# NESTED CV BENCHMARK -----------------------------------------------------
print("Benchmark")
for (i in 104:custom_inner$iters) {

  # debug
  # i = 1
  print(i)

  # inner resampling
  custom_ = rsmp("custom")
  custom_$instantiate(task_minute,
                      list(custom_inner$train_set(i)),
                      list(custom_inner$test_set(i)))

  # auto tuner rf
  at_rf = auto_tuner(
    tuner = tnr("hyperband", eta = 5),
    learner = graph_rf,
    resampling = custom_,
    measure = msr("adjloss2"),
    search_space = search_space_rf,
    terminator = trm("none")
  )

  # auto tuner xgboost
  at_xgboost = auto_tuner(
    tuner = tnr("hyperband", eta = 5),
    learner = graph_xgboost,
    resampling = custom_,
    measure = msr("adjloss2"),
    search_space = search_space_xgboost,
    terminator = trm("none")
  )

  # outer resampling
  customo_ = rsmp("custom")
  customo_$instantiate(task_minute, list(custom_outer$train_set(i)), list(custom_outer$test_set(i)))

  # nested CV for one round
  design = benchmark_grid(
    tasks = list(task_minute),
    learners = list(at_rf, at_xgboost),
    resamplings = customo_
  )
  bmr = benchmark(design, store_models = TRUE)

  # save locally and to list
  time_ = format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
  saveRDS(bmr, file.path(MLR3SAVEPATH, paste0(i, "-", time_, ".rds")))
}
