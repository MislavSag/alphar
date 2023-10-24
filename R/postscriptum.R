library(mlr3)
library(data.table)
library(AzureStor)



# SET UP ------------------------------------------------------------------
# globals
DATAPATH     = "F:/lean_root/data/all_stocks_daily.csv"
URIEXUBER    = "F:/equity-usa-hour-exuber"
NASPATH      = "C:/Users/Mislav/SynologyDrive/trading_data"
MLR3SAVEPATH = "D:/mlfin/benchmark-momentum-v1"
blob_key = "0M4WRlV0/1b6b3ZpFKJvevg4xbC/gaNBcdtVZW+zOZcRi0ZLfOm1v/j2FZ4v+o8lycJLu1wVE6HT+ASt0DdAPQ=="
endpoint = "https://snpmarketdata.blob.core.windows.net/"
BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)

# utils
id_cols = c("symbol", "date", "yearmonthid", "..row_id")

# set files with benchmarks
results_files = file.info(list.files(MLR3SAVEPATH, full.names = TRUE))
bmr_files = rownames(results_files)
bmr_files = bmr_files[grep("278-", bmr_files)]

# if duplicated lst first, vice versa
if (any(duplicated(gsub("-\\d+\\.rds", "", bmr_files)))) {
  results_files$path_fold = gsub("-\\d+\\.rds", "", rownames(results_files))
  results_files = results_files[order(results_files$path_fold, results_files$ctime), ]
  bmr_files = results_files[(!duplicated(results_files[, c("path_fold")], fromLast = TRUE)), ]
  bmr_files = rownames(bmr_files)
}

# extract needed information from banchmark objects
predictions_l = list()
imp_features_gausscov_l = list()
imp_features_corr_l = list()
for (i in 1:length(bmr_files)) {
  # debug
  print(i)

  # get bmr object
  bmr = readRDS(bmr_files[i])
  bmr_dt = as.data.table(bmr)

  # get backends
  task_names = unlist(lapply(bmr_dt$task, function(x) x$id))
  learner_names = unlist(lapply(bmr_dt$learner, function(x) gsub(".*removeconstants_3.regr.|.tuned", "", x$id)))
  backs = lapply(bmr_dt$task, function(task) {
    x = task$backend$data(cols = c(id_cols, "eps_diff", "nincr", "nincr_2y", "nincr_3y"),
                      rows = 1:bmr_dt$task[[1]]$nrow)
  })
  backs = lapply(seq_along(backs), function(i) cbind(task = task_names[[i]],
                                                     learner = learner_names[[1]],
                                                     backs[[i]]))
  lapply(backs, setnames, "..row_id", "row_ids")

  # get predictions
  predictions = lapply(bmr_dt$prediction, function(x) as.data.table(x))
  names(predictions) <- task_names

  # merge backs and predictions
  predictions <- lapply(seq_along(task_names), function(i) {
    y = backs[[i]][predictions[[i]], on = "row_ids"]
    y[, date := as.Date(date, origin = "1970-01-01")]
    cbind(task_name = x, y)
  })
  predictions = rbindlist(predictions)

  # add meta
  predictions = cbind(cv = stringr::str_extract(gsub(".*/", "", bmr_files[i]), "cv-\\d+"),
                      predictions)

  # # best params
  # best_params = lapply(bmr_dt$learner, function(x) x$tuning_instance$result_x_domain)
  # archives = lapply(bmr_dt$learner, function(x) x$tuning_instance$archive)
  #
  # most important variables
  imp_features_gausscov_l[[i]] = bmr_dt$learner[[1]]$state$model$learner$state$model$gausscov_f1st$features
  # imp_features_corr_l[[i]] = bmr_dt$learner[[1]]$state$model$learner$state$model$correlation$features

  # best models
  # bmr_dt$learner[[1]]$tuning_instance$archive
  # bmr_dt$learner[[1]]$state$model$learner$state$model$

  # predictions
  predictions_l[[i]] = predictions
}

# important variables
sort(table(unlist(imp_features_gausscov_l)))
sort(table(unlist(imp_features_corr_l)))

# hit ratio
# predictions_dt = rbindlist(lapply(bmrs, function(x) x$predictions), idcol = "fold")
predictions_dt = rbindlist(predictions_l)
predictions_dt[, `:=`(
  truth_sign = as.factor(sign(truth)),
  response_sign = as.factor(sign(response))
)]
setorderv(predictions_dt, c("cv", "task_name", "date"))
predictions_dt[, mlr3measures::acc(truth_sign, response_sign), by = c("cv", "task_name")]
predictions_dt[response > 0.1, mlr3measures::acc(truth_sign, response_sign), by = c("cv", "task_name")]
predictions_dt[response > 0.2, mlr3measures::acc(truth_sign, response_sign), by = c("cv", "task_name")]
predictions_dt[response > 0.5, mlr3measures::acc(truth_sign, response_sign), by = c("cv", "task_name")]
predictions_dt[response > 1, mlr3measures::acc(truth_sign, response_sign), by = c("cv", "task_name")]

# hit ratio with positive surprise
predictions_dt[, mlr3measures::acc(truth_sign, response_sign), by = .(cv, task_name, eps_diff > 0)]
predictions_dt[response > 0.1, mlr3measures::acc(truth_sign, response_sign), by = .(cv, task_name, eps_diff > 0)]
predictions_dt[response > 0.5, mlr3measures::acc(truth_sign, response_sign), by = .(cv, task_name, eps_diff > 0)]
predictions_dt[response > 1, mlr3measures::acc(truth_sign, response_sign), by = .(cv, task_name, eps_diff > 0)]

# hit ratio with positive surprise nincr
predictions_dt[, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr) > 1)]
predictions_dt[response > 0.1, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr) > 1)]
predictions_dt[response > 0.3, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr) > 1)]
predictions_dt[response > 0.5, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr) > 1)]
predictions_dt[response > 1, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr) > 1)]

# hit ratio with positive surprise nincr_2y
predictions_dt[, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr_2y) < 2)]
predictions_dt[response > 0.1, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr_2y) < 2)]
predictions_dt[response > 0.3, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr_2y) < 2)]
predictions_dt[response > 0.5, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr_2y)  > 1)]
predictions_dt[response > 1, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr_2y) > 1)]

# hit ratio with positive surprise nincr_2y
predictions_dt[, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr_3y) > 2)]
predictions_dt[response > 0.1, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr_3y) > 2)]
predictions_dt[response > 0.3, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr_3y) > 2)]
predictions_dt[response > 0.5, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr_3y) > 2)]
predictions_dt[response > 1, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr) > 2)]

# hit ratio with positive surprise nincr
predictions_dt[as.integer(nincr_3y) > 2 & eps_diff == TRUE]
table(predictions_dt$eps_diff > 0)
predictions_dt[, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr_3y) > 12 & eps_diff == TRUE)]
predictions_dt[response > 0.1, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr_3y) < 4 & eps_diff == TRUE)]
predictions_dt[response > 0.3, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr_3y) < 4 & eps_diff == TRUE)]
predictions_dt[response > 0.5, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr_3y) < 4 & eps_diff == TRUE)]
predictions_dt[response > 1, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr_3y) < 4 & eps_diff == TRUE)]


# hit ratio for ensamble
predictions_dt_ensemble = predictions_dt[, .(mean_response = mean(response),
                                             median_response = median(response),
                                             truth = mean(truth)),
                                         by = c("task_name", "symbol", "date")]
predictions_dt_ensemble[, `:=`(
  truth_sign = as.factor(sign(truth)),
  response_sign_median = as.factor(sign(median_response)),
  response_sign_mean = as.factor(sign(mean_response))
)]
predictions_dt_ensemble[, mlr3measures::acc(truth_sign, response_sign_median), by = c("task_name")]
predictions_dt_ensemble[, mlr3measures::acc(truth_sign, response_sign_mean), by = c("task_name")]
predictions_dt_ensemble[median_response > 0.1, mlr3measures::acc(truth_sign, response_sign_median), by = c("task_name")]
predictions_dt_ensemble[mean_response > 0.1, mlr3measures::acc(truth_sign, response_sign_mean), by = c("task_name")]
predictions_dt_ensemble[median_response > 0.5, mlr3measures::acc(truth_sign, response_sign_median), by = c("task_name")]
predictions_dt_ensemble[mean_response > 0.5, mlr3measures::acc(truth_sign, response_sign_mean), by = c("task_name")]
predictions_dt_ensemble[median_response > 1, mlr3measures::acc(truth_sign, response_sign_median), by = c("task_name")]
predictions_dt_ensemble[mean_response > 1, mlr3measures::acc(truth_sign, response_sign_mean), by = c("task_name")]

# save to azure for QC backtest
cont = storage_container(BLOBENDPOINT, "qc-backtest")
lapply(unique(predictions_dt$task_name), function(x) {
  # prepare data
  y = predictions_dt[task_name == x]
  y = y[, .(symbol, date, response, eps_diff)]
  y = y[, .(
    symbol = paste0(symbol, collapse = "|"),
    response = paste0(response, collapse = "|"),
    epsdiff = paste0(eps_diff, collapse = "|")
  ), by = date]
  y[, date := as.character(date)]

  # save to azure blob
  print(y)
  file_name_ =  paste0("pead-", x, "-v5.csv")
  storage_write_csv(y, cont, file_name_)
  # universe = y[, .(date, symbol)]
  # storage_write_csv(universe, cont, "pead_task_ret_week_universe.csv", col_names = FALSE)
})




# OLD ---------------------------------------------------------------------
# # linex measure
# source("Linex.R")
# mlr_measures$add("linex", Linex)
#
# # setup
# blob_key = "0M4WRlV0/1b6b3ZpFKJvevg4xbC/gaNBcdtVZW+zOZcRi0ZLfOm1v/j2FZ4v+o8lycJLu1wVE6HT+ASt0DdAPQ=="
# endpoint = "https://snpmarketdata.blob.core.windows.net/"
# BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)
# mlr3_save_path = "D:/mlfin/cvresults-pead"
#
# # read predictors
# DT <- fread("D:/features/pead-predictors.csv")
#
# # create group variable
# DT[, monthid := paste0(data.table::year(as.Date(date, origin = "1970-01-01")),
#                        data.table::month(as.Date(date, origin = "1970-01-01")))]
# DT[, monthid := as.integer(monthid)]
# setorder(DT, monthid)
#
# # define predictors
# cols_non_features <- c("symbol", "date", "time", "right_time",
#                        "bmo_return", "amc_return",
#                        "open", "high", "low", "close", "volume", "returns",
#                        "monthid"
# )
# targets <- c(colnames(DT)[grep("ret_excess", colnames(DT))])
# cols_features <- setdiff(colnames(DT), c(cols_non_features, targets))
#
# # convert columns to numeric. This is important only if we import existing features
# chr_to_num_cols <- setdiff(colnames(DT[, .SD, .SDcols = is.character]), c("symbol", "time", "right_time"))
# print(chr_to_num_cols)
# DT <- DT[, (chr_to_num_cols) := lapply(.SD, as.numeric), .SDcols = chr_to_num_cols]
#
# # remove constant columns in set and remove same columns in test set
# features_ <- DT[, ..cols_features]
# remove_cols <- colnames(features_)[apply(features_, 2, var, na.rm=TRUE) == 0]
# print(paste0("Removing feature with 0 standard deviation: ", remove_cols))
# cols_features <- setdiff(cols_features, remove_cols)
#
# # convert variables with low number of unique values to factors
# int_numbers = DT[, ..cols_features][, lapply(.SD, function(x) all(as.integer(x)==x) & x > 0.99)]
# int_cols = na.omit(colnames(DT[, ..cols_features])[as.matrix(int_numbers)[1,]])
# factor_cols = DT[, ..int_cols][, lapply(.SD, function(x) length(unique(x)))]
# factor_cols = as.matrix(factor_cols)[1, ]
# factor_cols = factor_cols[factor_cols <= 100]
# DT = DT[, (names(factor_cols)) := lapply(.SD, as.factor), .SD = names(factor_cols)]
#
# # remove observations with missing target
# DT = na.omit(DT, cols = setdiff(targets, colnames(DT)[grep("extreme", colnames(DT))]))
#
# # sort
# setorder(DT, date)
#
# # add rowid column
# DT[, row_ids := 1:.N]
#
# ### REGRESSION
# # task with future week returns as target
# target_ = colnames(DT)[grep("^ret_excess_stand_5", colnames(DT))]
# cols_ = c(target_, "symbol", "monthid", cols_features)
# task_ret_week <- as_task_regr(DT[, ..cols_],
#                               id = "task_ret_week",
#                               target = target_)
#
# # outer custom rolling window resampling
# outer_split <- function(task, train_length = 36, test_length = 2, test_length_out = 1) {
#   customo = rsmp("custom")
#   task_ <- task$clone()
#   groups = cbind(id = 1:task_$nrow, task_$data(cols = "monthid"))
#   groups_v = groups[, unique(monthid)]
#   rm(task_)
#   insample_length = train_length + test_length
#   train_groups_out <- lapply(1:(length(groups_v)-train_length-test_length), function(x) groups_v[x:(x+insample_length)])
#   test_groups_out <- lapply(1:(length(groups_v)-train_length-test_length),
#                             function(x) groups_v[(x+insample_length):(x+insample_length+test_length_out-1)])
#   train_sets_out <- lapply(train_groups_out, function(mid) groups[monthid %in% mid, id])
#   test_sets_out <- lapply(test_groups_out, function(mid) groups[monthid %in% mid, id])
#   customo$instantiate(task, train_sets_out, test_sets_out)
# }
# customo = outer_split(task_ret_week)
#
# # download objects from azure
# cont = storage_container(BLOBENDPOINT, "peadcv")
# azure_files = unlist(list_storage_files(cont)["name"], use.names = FALSE)
# for (azf in azure_files) {
#   # azf = azure_files[1]
#   if (azf %in% list.files(mlr3_save_path)) {
#     print(azf)
#     next
#   }
#   storage_download(cont,
#                    src = azf,
#                    dest = file.path(mlr3_save_path, azf))
# }
#
# # get predictions function
# res_files = file.info(list.files(mlr3_save_path, full.names = TRUE))
# res_files = res_files[order(res_files$ctime), ]
# bmr_files = rownames(res_files[2:nrow(res_files),])
# get_predictions_by_task = function(bmr_file, DT) {
#   # bmr_file = bmr_files[39]
#   print(bmr_file)
#   bmr = readRDS(bmr_file)
#   bmr_dt = as.data.table(bmr)
#   task_names = lapply(bmr_dt$task, function(x) x$id)
#   # task_names = lapply(bmr_dt$task, function(x) x$id)
#   predictions = lapply(bmr_dt$prediction, function(x) as.data.table(x))
#   names(predictions) <- task_names
#   predictions <- lapply(predictions, function(x) {
#     x = DT[, .(row_ids, symbol, date)][x, on = "row_ids"]
#     x[, date := as.Date(date, origin = "1970-01-01")]
#   })
#   return(predictions)
# }
# predictions = lapply(bmr_files, get_predictions_by_task, DT = DT)
#
# # choose task
# task_name = "task_ret_week"
# predictions_task = lapply(predictions, function(x) {
#   x[[task_name]]
# })
# predictions_task <- rbindlist(predictions_task)
#
# # save to azure for QC backtest
# predictions_qc <- predictions_task[, .(symbol, date, response)]
# predictions_qc = predictions_qc[, .(symbol = paste0(symbol, collapse = "|"),
#                                     response = paste0(response, collapse = "|")), by = date]
# predictions_qc[, date := as.character(date)]
# cont = storage_container(BLOBENDPOINT, "qc-backtest")
# storage_write_csv(predictions_qc, cont, "pead_task_ret_week.csv")
# universe = predictions_qc[, .(date, symbol)]
# storage_write_csv(universe, cont, "pead_task_ret_week_universe.csv", col_names = FALSE)
#
# # performance by varioues measures
# bmr_$aggregate(msrs(c("regr.mse", "linex", "regr.mae")))
# predicitons = as.data.table(as.data.table(bmr_)[, "prediction"][1][[1]][[1]])
#
# # prrformance by hit ratio
# predicitons[, `:=`(
#   truth_sign = as.factor(sign(truth)),
#   response_sign = as.factor(sign(response))
# )]
# mlr3measures::acc(predicitons$truth_sign, predicitons$response_sign)
#
# # hiy ratio for high predicted returns
# predicitons_sample = predicitons[response > 0.1]
# mlr3measures::acc(predicitons_sample$truth_sign, predicitons_sample$response_sign)
#
# # cumulative returns for same sample
# predicitons_sample[, .(
#   benchmark = mean(predicitons$truth),
#   strategy  = mean(truth)
# )]
#
# # important predictors
# bmr_ = bmrs[[18]]
# lapply(1:2, function(i) {
#   resample_res = as.data.table(bmr_$resample_result(i))
#   resample_res$learner[[1]]$state$model$learner$state$model$gausscov_f1st$features
# })
#
# # performance for every learner
# resample_res$learner[[1]]$state$model$learner$state$model$ranger.ranger$model$predictions
# as.data.table(bmr_)




# ADD PIPELINES -----------------------------------------------------------
# # source pipes, filters and other
# source("mlr3_winsorization.R")
# source("mlr3_uniformization.R")
# source("mlr3_gausscov_f1st.R")
# source("mlr3_dropna.R")
# source("mlr3_dropnacol.R")
# source("mlr3_filter_drop_corr.R")
# source("mlr3_winsorizationsimple.R")
# source("PipeOpPCAExplained.R")
# # measures
# source("Linex.R")
# source("PortfolioRet.R")
#
# # add my pipes to mlr dictionary
# mlr_pipeops$add("uniformization", PipeOpUniform)
# mlr_pipeops$add("winsorize", PipeOpWinsorize)
# mlr_pipeops$add("winsorizesimple", PipeOpWinsorizeSimple)
# mlr_pipeops$add("dropna", PipeOpDropNA)
# mlr_pipeops$add("dropnacol", PipeOpDropNACol)
# mlr_pipeops$add("dropcorr", PipeOpDropCorr)
# mlr_pipeops$add("pca_explained", PipeOpPCAExplained)
# mlr_filters$add("gausscov_f1st", FilterGausscovF1st)
# mlr_measures$add("linex", Linex)
# mlr_measures$add("portfolio_ret", PortfolioRet)
