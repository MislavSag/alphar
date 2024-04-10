library(data.table)
library(fs)
library(mlr3)
library(mlr3cluster)
library(mlr3misc)
library(AzureStor)


# Set up
PATH = "F:/predictors/fi_clustering"
col_ids = c("symbol", "date", "month", "close", "close_raw", paste0("mom", c(1, 3, 6, 12)))

# Import feature importance results
files = dir_ls(PATH, regexp = "\\.rds")
tasks = lapply(files, function(f) {
  # f = files[1]
  # f = "F:/predictors/fi_clustering/2001-03-01.rds"
  print(f)
  dt_ = readRDS(f)
  dt_ = rcbind(dt_$backend$data(dt_$row_ids, col_ids), dt_$data())
  setorder(dt_, symbol, month)
  dt_[, mom1 := close / shift(close, 1L) - 1, by = symbol]
  dt_[, mom3 := close / shift(close, 3L) - 1, by = symbol]
  dt_[, mom6 := close / shift(close, 6L) - 1, by = symbol]
  dt_[, mom12 := close / shift(close, 12L) - 1, by = symbol]
  dt_ = dt_[month == max(month, na.rm = TRUE)]
  task_ = as_task_clust(dt_, id = dt_[, as.character(round(unique(month), 2))])
  task_$col_roles$feature = setdiff(task_$col_roles$feature, c(col_ids, "target"))
  return(task_)
})

# Clustering benchmark
# ERRORS:
# 1) kkmeans:
# Error in (function (classes, fdef, mtable)  :
#             unable to find an inherited method for function ‘affinMult’ for signature ‘"rbfkernel", "numeric"’
#           In addition: There were 14 warnings (use warnings() to see them)
lapply(tasks, function(x) nrow(x$data()))
design = benchmark_grid(
  tasks = tasks,
  learners = list(
    lrn("clust.kmeans", centers = 600L, id = "kmeans_600", predict_type = "partition"),
    lrn("clust.cmeans", centers = 600L, id = "cmeans_600", predict_type = "partition"),
    lrn("clust.agnes", k = 600L, id = "agnes_600", predict_type = "partition")

    # lrn("clust.kmeans", centers = 5L, id = "kmeans_5", predict_type = "partition"),
    # lrn("clust.kmeans", centers = 10L, id = "kmeans_10", predict_type = "partition"),
    # lrn("clust.kmeans", centers = 50L, id = "kmeans_50", predict_type = "partition"),
    # lrn("clust.kmeans", centers = 100L, id = "kmeans_100", predict_type = "partition"),
    # lrn("clust.kmeans", centers = 200L, id = "kmeans_200", predict_type = "partition"),
    # lrn("clust.kmeans", centers = 400L, id = "kmeans_400", predict_type = "partition"),
    # lrn("clust.cmeans", centers = 5L, id = "cmeans_5", predict_type = "partition"),
    # lrn("clust.cmeans", centers = 50L, id = "cmeans_50", predict_type = "partition"),
    # lrn("clust.cmeans", centers = 100L, id = "cmeans_100", predict_type = "partition"),
    # lrn("clust.cmeans", centers = 200L, id = "cmeans_200", predict_type = "partition"),
    # lrn("clust.cmeans", centers = 400L, id = "cmeans_400", predict_type = "partition"),
    # lrn("clust.agnes", k = 5L, id = "agnes_5", predict_type = "partition"),
    # lrn("clust.agnes", k = 50L, id = "agnes_50", predict_type = "partition"),
    # lrn("clust.agnes", k = 100L, id = "agnes_100", predict_type = "partition"),
    # lrn("clust.agnes", k = 200L, id = "agnes_200", predict_type = "partition"),
    # lrn("clust.agnes", k = 400L, id = "agnes_400", predict_type = "partition")

    # lrn("clust.ap", id = "ap", predict_type = "partition") # SLOW BUT NOT BETTER
    # lrn("clust.agnes", k = 10, id = "agnes_10"),rs = 10L, id = "cmeans_10", predict_type = "partition"),
    # lrn("clust.pam", k = 3L),
    # lrn("clust.hclust"),
    # lrn("clust.diana", k = 5L, id = "diana_5"),
    # lrn("clust.diana", k = 10L, id = "diana_10"),
    # lrn("clust.diana", k
    ),
  resamplings = rsmp("insample"))
bmrs = benchmark(design, store_models = TRUE)
bmrs_dt =  as.data.table(bmrs)

# get predictions for all tasks and learners
bmrs_res = bmrs_dt[, .(predictions = lapply(prediction, function(dt_) {
  dt_ = as.data.table(dt_)
  dt_[, 1:2]
  }),
  ids = lapply(task, function(x) x$backend$data(x$row_ids, c(col_ids, "target"))),
  task_id = vapply(task, function(x) x$id, FUN.VALUE = character(1)),
  learner_id = vapply(learner, function(x) x$id, FUN.VALUE = character(1)))]
bmrs_res = bmrs_res[, cbind(rbindlist(predictions), rbindlist(ids)),
                    by = c("task_id", "learner_id")]

# Remove duplicates
bmrs_res = unique(bmrs_res)
bmrs_res[, unique(learner_id)]

# Filter by raw price
bmrs_res = bmrs_res[close_raw > 1]


# RESULTS -----------------------------------------------------------------
# Calculate mom differences between pairs
mom_diff = function(dt, mom_var) {
  # dt = copy(bmrs_res)
  # mom_var = "mom1"

  setorderv(dt, c("task_id", "learner_id", "partition", mom_var), order = -1)
  var_name = paste0(mom_var, "_diff")
  dt[, (var_name) := unlist(lapply(0:(length(mom_var) - 1), function(x)
    mom_var[x + 1] - mom_var[length(mom_var) - x])),
    by = c("task_id", "learner_id", "partition"),
    env = list(mom_var = mom_var)]
  sd_name = paste0(var_name, "_sd")
  dt[, (sd_name) := sd(var_name, na.rm = TRUE), by = c("task_id", "learner_id", "partition"),
      env = list(var_name = var_name)]
}
mom_diff(bmrs_res, "mom1")
mom_diff(bmrs_res, "mom3")
mom_diff(bmrs_res, "mom6")
mom_diff(bmrs_res, "mom12")

# Check
setorderv(bmrs_res, c("task_id", "learner_id", "partition", "mom1"), order = -1)
bmrs_res[task_id == "2023.92" & learner_id == "kmeans_10" & partition == 1,
         .(task_id, learner_id, partition, mom1, mom1_diff, mom1_diff_sd)]

# Universes
universes = lapply(c("mom1_diff", "mom3_diff", "mom6_diff", "mom12_diff"), function(x) {
  var_name = paste0(x, "_sd")
  universe = bmrs_res[abs(x) >= var_name, env = list(x = x, var_name = var_name)]
  setorderv(universe, c("task_id", "learner_id", "partition", x), c(1, 1, 1, -1))
  # universe[, .N, by = c("task_id", "partition", "learner_id")]
  return(universe)
})

# Pairs
pairs = lapply(seq_along(universes), function(i) {
  pairs = universes[[i]][, let(
    pair_short = symbol,
    pair_long = rev(symbol),
    momret_short = target,
    momret_long = rev(target)
    ),
    by = c("task_id", "learner_id", "partition")]
  if (i == 1) {
    var_ = paste0("mom", 1, "_diff")
  } else if (i == 2) {
    var_ = paste0("mom", 3, "_diff")
  } else if (i == 3) {
    var_ = paste0("mom", 6, "_diff")
  } else if (i == 4) {
    var_ = paste0("mom", 12, "_diff")
  }
  # pairs[task_id == "2023.92" & learner_id == "kmeans_10" & partition == 1,]
  pairs = pairs[var_ > 0, env = list(var_ = var_)]
})
pairs[[1]][task_id == "2023.92" & learner_id == "kmeans_50" & partition == 2,]

# Cumulative returns by learners
returns_long = lapply(pairs, function(p) {
  # p = pairs[[1]]
  x = p[month > 2015, .(cum_return = sum(momret_long * (1 / length(momret_long)))),
    by = c("task_id", "learner_id")]
  # x = x[, .(cum_return = mean(cum_return)), by = c("task_id", "learner_id")]
  x
})

# Test
# returns_long[[1]]
# pairs[[1]][month > 2015 & task_id == "2015.08" & learner_id == "agnes_5",
#            .(
#              # task_id, learner_id, partition, date,
#              symbol, close, close_raw,
#              mom1, target, mom1_diff, pair_short, pair_long, momret_short, momret_long)]
# pairs[[1]][month > 2015 & task_id == "2015.08" & learner_id == "agnes_5",
#            .(symbol, close, close_raw,
#              mom1, target, mom1_diff, pair_short, pair_long, momret_short,
#              momret_long)][,
#                mean(momret_long)
#              ]


lapply(returns_long, function(r) {
  r[, PerformanceAnalytics::Return.cumulative(cum_return), by = c("learner_id")]
})


returns_short = lapply(pairs, function(p) {
  p[month > 2015, .(cum_return = sum(momret_short * -(1 / length(momret_short)))),
    by = c("task_id", "learner_id")]
})
lapply(returns_short, function(r) {
  r[, PerformanceAnalytics::Return.cumulative(cum_return), by = c("learner_id")]
})

# equity curve for best
eqc = pairs[[3]][learner_id == "cmeans_100",
                 .(date = lubridate::ceiling_date(as.Date(date), "month"),
                   partition,
                   pair_long,
                   target = momret_long)]
eqc = eqc[date > as.Date("2015-01-01")]
# setorder(eqc, target)
eqc = unique(eqc, by = c("date", "pair_long"))
eqc = dcast(eqc, date ~ pair_long, value.var = "target")
setnafill(eqc, fill = 0)
dim(eqc)
eqc[1:10, 1:10]
portfolio_return = PerformanceAnalytics::Return.portfolio(eqc, rebalance_on = "months")
PerformanceAnalytics::charts.PerformanceSummary(portfolio_return, plot.engine = "ggplot2")


# SAVE FOR QC BACKTEST ----------------------------------------------------
# extract dtaa for best clustering method
pairs_qc_dt = pairs[[1]][learner_id == "agnes_400",
                         .(date = lubridate::ceiling_date(date, "month"),
                           pair_short,
                           pair_long)]
pairs_qc = pairs_qc_dt[, .(
  pairs_short = paste0(pair_short, collapse = "|"),
  pairs_long = paste0(pair_long, collapse = "|")
),
by = date]
pairs_qc[, date := as.Date(date)]
setorder(pairs_qc, "date")
seq_date = data.table(date = seq.Date(min(pairs_qc$date), max(pairs_qc$date), by = 1))
pairs_qc = pairs_qc[seq_date, on = "date", roll = Inf]
pairs_qc[, date := as.character(date)]
endpoint = storage_endpoint(Sys.getenv("BLOB-ENDPOINT-SNP"),
                            key=Sys.getenv("BLOB-KEY-SNP"))
cont = storage_container(endpoint, "qc-backtest")
storage_write_csv(pairs_qc, cont, "fi_cluster_statarb.csv", col_names = FALSE)

# inspect difference between R code and QC code
dt_ = pairs[[4]][learner_id == "agnes_50",
                 .(date = lubridate::ceiling_date(date, "month"),
                   pair_short,
                   pair_long,
                   target)]
dt_[date %between% c("2015-01-01", "2015-01-05")]
dt_[date %between% c("2015-01-01", "2015-01-05")][order(target)]
dt_["tsla" == pair_short][date >= "2015-01-01"]
