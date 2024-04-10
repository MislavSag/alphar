library(data.table)
library(fs)
library(gausscov)
library(paradox)
library(mlr3pipelines)
library(mlr3verse)
library(doParallel)
library(foreach)


# import predictors

# prepare data for gausscov
dates = predictors[, sort(unique(month))]
rolling_window = 24
dates_start = dates[1:(length(dates)-rolling_window)]
dates_end = dates[(rolling_window+1):length(dates)]
length(dates_start) == length(dates_end)
c_ = colnames(predictors)
nonfeature_cols = c_[which(c_ == "symbol"):which(c_ == "returns")]

# Import pipes
# TODO: Add this to finautoml package
FilterGausscovF3st = R6::R6Class(
  "FilterGausscovF3st",
  inherit = mlr3filters::Filter,

  public = list(

    #' @description Create a GaussCov object.
    initialize = function() {
      param_set = ps(
        m      = p_int(lower = 1, upper = 10, default = 1),
        kexmx	 = p_int(lower = 0, upper = 500, default = 0),
        p0     = p_dbl(lower = 0.0001, upper = 0.99, default = 0.01),
        kmn  = p_int(lower = 0, default = 0),
        kmx  = p_int(lower = 0, default = 0),
        mx   = p_int(lower = 1, default = 21),
        lm   = p_int(lower = 1, default = 100),
        kex  = p_int(lower = 0, default = 0),
        sub  = p_lgl(default = TRUE),
        inr  = p_lgl(default = TRUE),
        xinr = p_lgl(default = FALSE),
        qq   = p_int(lower = 0, default = 0)
      )

      super$initialize(
        id = "gausscov_f3st",
        task_types = c("classif", "regr"),
        param_set = param_set,
        feature_types = c("integer", "numeric"),
        packages = "gausscov",
        label = "Gauss Covariance f3st",
        man = "mlr3filters::mlr_filters_gausscov_f3st"
      )
    }
  ),

  private = list(
    .calculate = function(task, nfeat) {
      # debug
      # pv = list(
      #   p0   = 0.01,
      #   kmn  = 0,
      #   kmx  = 0,
      #   mx   = 21,
      #   kex  = 0,
      #   sub  = TRUE,
      #   inr  = TRUE,
      #   xinr = FALSE,
      #   qq   = 0
      # )

      # empty vector with variable names as vector names
      scores = rep(-1, length(task$feature_names))
      scores = mlr3misc::set_names(scores, task$feature_names)

      # calculate gausscov pvalues
      pv = self$param_set$values
      x = as.matrix(task$data(cols = task$feature_names))
      if (task$task_type == "classif") {
        y = as.matrix(as.integer(task$truth()))
      } else {
        y = as.matrix(task$truth())
      }
      res = mlr3misc::invoke(gausscov::f3st, y = y, x = x, .args = pv)
      res_index <- tryCatch({unique(as.integer(res[[1]][1, ]))[-1]}, error = function(e) NULL)
      while (is.null(res_index)) {
        pv$p0 = pv$p0 + 0.01
        print(pv)
        res = mlr3misc::invoke(gausscov::f3st, y = y, x = x, .args = pv)
        res_index <- tryCatch({unique(as.integer(res[[1]][1, ]))[-1]}, error = function(e) NULL)
      }
      res_index = res_index [res_index  != 0]

      scores[res_index] = 1

      # return scores
      sort(scores, decreasing = TRUE)
    }
  )
)
FilterGausscovF1st = R6::R6Class(
  "FilterGausscovF1st",
  inherit = mlr3filters::Filter,

  public = list(

    #' @description Create a GaussCov object.
    initialize = function() {
      param_set = ps(
        p0   = p_dbl(lower = 0, upper = 1, default = 0.01),
        kmn  = p_int(lower = 0, default = 0),
        kmx  = p_int(lower = 0, default = 0),
        mx   = p_int(lower = 1, default = 35),
        kex  = p_int(lower = 0, default = 0),
        sub  = p_lgl(default = TRUE),
        inr  = p_lgl(default = TRUE),
        xinr = p_lgl(default = FALSE),
        qq   = p_int(lower = 0, default = 0)
      )

      super$initialize(
        id = "gausscov_f1st",
        task_types = c("classif", "regr"),
        param_set = param_set,
        feature_types = c("integer", "numeric"),
        packages = "gausscov",
        label = "Gauss Covariance f1st",
        man = "mlr3filters::mlr_filters_gausscov_f1st"
      )
    }
  ),

  private = list(
    gausscov_ = function(x, y, pv) {
      res = mlr3misc::invoke(gausscov::f1st, y = y, x = x, .args = pv)
      res_1 = res[[1]]
      res_1 = res_1[res_1[, 1] != 0, , drop = FALSE]
      return(res_1)
    },

    .calculate = function(task, nfeat) {
      # debug
      # pv = list(
      #   p0   = 0.01,
      #   kmn  = 0,
      #   kmx  = 0,
      #   mx   = 21,
      #   kex  = 0,
      #   sub  = TRUE,
      #   inr  = TRUE,
      #   xinr = FALSE,
      #   qq   = 0
      # )

      # mlr_tasks
      # task = tsk("mtcars")
      # pv = list(); pv$p0 = 0.05

      # empty vector with variable names as vector names
      scores = rep(0, length(task$feature_names))
      scores = mlr3misc::set_names(scores, task$feature_names)

      # calculate gausscov pvalues
      pv = self$param_set$values
      x = as.matrix(task$data(cols = task$feature_names))
      if (task$task_type == "classif") {
        y = as.matrix(as.integer(task$truth()))
      } else {
        y = as.matrix(task$truth())
      }

      gausscov_res = private$gausscov_(x, y, pv)
      while (nrow(gausscov_res) == 1) {
        pv$p0 = pv$p0 + 0.01
        print(pv$p0)
        gausscov_res = private$gausscov_(x, y, pv)
      }

      scores[gausscov_res[, 1]] = ceiling(abs(gausscov_res[, 4]))
      sort(scores, decreasing = TRUE)
    }
  )
)

mlr_pipeops$add("dropna", finautoml::PipeOpDropNA)
mlr_pipeops$add("dropnacol", finautoml::PipeOpDropNACol)
mlr_pipeops$add("dropcorr", finautoml::PipeOpDropCorr)
mlr_pipeops$add("uniformization", finautoml::PipeOpUniform)
mlr_pipeops$add("winsorizesimple", finautoml::PipeOpWinsorizeSimple)
mlr_filters$add("gausscov_f1st", FilterGausscovF1st)
mlr_filters$add("gausscov_f3st", FilterGausscovF3st)

# Create pipeline
pipe = po("dropnacol", id = "dropnacol", cutoff = 0.05) %>>%
  po("dropna", id = "dropna_1") %>>%
  po("removeconstants", id = "removeconstants_1", ratio = 0)  %>>%
  po("fixfactors", id = "fixfactors") %>>%
  po("winsorizesimple", id = "winsorizesimple", probs_low = 0.01, probs_high = 0.99, na.rm = TRUE) %>>%
  po("removeconstants", id = "removeconstants_2", ratio = 0)  %>>%
  po("dropcorr", id = "dropcorr", cutoff = 0.99) %>>%
  po("uniformization") %>>%
  po("filter", filter = flt("gausscov_f1st"), p0 = 0.01, filter.cutoff = 0.5)
# po("filter", filter = flt("relief"), filter.nfeat = 10)

# define columns roles
nonfeature_cols = colnames(predictors)
nonfeature_cols = nonfeature_cols[1:which(nonfeature_cols == "returns")]
dt = na.omit(predictors, cols = "target")
dt[, date := as.POSIXct(date)]

# Preprocessing test
tsk_ = as_task_regr(dt[1:20000], target = "target", id = "ficlust")
tsk_$col_roles$feature = setdiff(tsk_$col_roles$feature,
                                 nonfeature_cols)
dt_out = pipe$train(tsk_)
dt_out$gausscov_f1st.output
dt_out$gausscov_f1st.output$nrow
dt_out$gausscov_f1st.output$ncol
dt_out$gausscov_f1st.output$data()

# set path
path_ = "F:/predictors/fi_clustering"

# use R runner package to make expanding window with minmum of length 24
cl = makeCluster(2L)
clusterExport(cl, c("dates_start", "dates_end", "dt", "cols", "pipe", "nonfeature_cols"))
pckgs = c("stringr", "mlr3pipelines", "gausscov", "data.table", "fs", "mlr3verse")
registerDoParallel(cl)
foreach(i = seq_along(dates_start), .packages = pckgs) %dopar% {
  # debug
  # i = 1
  print(i)
  start_ = dates_start[i]
  end_   = dates_end[i]

  # create file
  year_  = as.integer(floor(end_))
  month_ = round((end_ - year_) * 12) + 1
  month_ = ifelse(month_ == 0, "01",
                  stringr::str_pad(month_, width = 2, side = "left", pad = "0"))
  file_name = file.path(path_, paste0(as.Date(paste0(year_, "-", month_, "-01")), ".rds"))

  # Check if filename exists
  if (file.exists(file_name)) return(NULL)
  # Create task
  task_ = as_task_regr(dt[month %between% c(start_, end_)],
                       target = "target",
                       id = "ficlust")
  task_$col_roles$feature = setdiff(task_$col_roles$feature,
                                    nonfeature_cols)

  # Make pipe
  x = pipe$train(task_)

  # save results
  saveRDS(x$gausscov_f1st.output, file = file_name)
}
stopCluster(cl)
