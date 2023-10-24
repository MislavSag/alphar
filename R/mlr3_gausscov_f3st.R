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

      res_index <- unique(as.integer(res[[1]][1, ]))[-1]
      res_index  <- res_index [res_index  != 0]

      scores[res_index] = 1
      sort(scores, decreasing = TRUE)
    }
  )
)

# # no group variable
# library(mlr3verse)
# task = tsk("iris")
# # task = tsk("mtcars")
# task$data()
# task$target_names
# mlr_filters$add("gausscov_f3st", FilterGausscovF3st)
# filter = flt("gausscov_f3st", m = 2)
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

