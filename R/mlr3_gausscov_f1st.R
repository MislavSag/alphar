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
        mx   = p_int(lower = 1, default = 21),
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
      res = mlr3misc::invoke(gausscov::f1st, y = y, x = x, .args = pv)
      res_1 = res[[1]]
      res_1 = res_1[res_1[, 1] != 0, , drop = FALSE]
      scores[res_1[, 1]] = abs(res_1[, 4])
      sort(scores, decreasing = TRUE)
    }
  )
)

# mlr_filters$add("gausscov_f1st", FilterGausscovF1st)

# # no group variable
# task = tsk("iris")
# # task = tsk("mtcars")
# task$data()
# task$target_names
# filter = flt("gausscov_f1st")
# filter$param_set
# filter$calculate(task)
# as.data.table(filter)
# task$data()$Species
# as.numeric(task$data()$Species)
#
# # graph
# graph = po("filter", filter = flt("gausscov_f1st"), filter.cutoff = 0) %>>%
#   po("learner", lrn("classif.rpart"))
# learner = as_learner(graph)
# learner$param_set
# result = learner$train(task)
# result$model$gausscov_f1st
# learner$predict(task)

