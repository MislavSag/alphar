PipeOpDropCorr = R6::R6Class(
  "PipeOpDropCorr",
  inherit = mlr3pipelines::PipeOpTaskPreprocSimple,
  public = list(
    initialize = function(id = "drop.const", param_vals = list()) {
      ps = ParamSet$new(list(
        ParamFct$new("use", c("everything", "all.obs", "complete.obs", "na.or.complete", "pairwise.complete.obs"), default = "everything"),
        ParamFct$new("method", c("pearson", "kendall", "spearman"), default = "pearson"),
        ParamDbl$new("cutoff", lower = 0, upper = 1, default = 0.99)
      ))
      ps$values = list(use = "everything", method = "pearson", cutoff = 0.99)
      super$initialize(id = id, param_set = ps, param_vals = param_vals, feature_types = c("numeric"))
    }
  ),

  private = list(
    .get_state = function(task) {
      # debug
      # pv = list(
      #   use = "everything",
      #   method = "pearson",
      #   cutoff = 0.9
      # )

      fn = task$feature_types[type == self$feature_types, id]
      data = task$data(cols = fn)
      pv = self$param_set$values

      cm = mlr3misc::invoke(stats::cor, x = data, use = pv$use, method = pv$method)
      cm[upper.tri(cm)] <- 0
      diag(cm) <- 0
      cm <- abs(cm)
      remove_cols <- colnames(data)[apply(cm, 2, function(x) any(x > pv$cutoff))]
      keep_cols <- setdiff(fn, remove_cols)
      list(cnames = keep_cols)
    },

    .transform = function(task) {
      task$select(self$state$cnames)
    }
  )
)

# # no group variable
# task = tsk("iris")
# gr = Graph$new()
# gr$add_pipeop(PipeOpDropCorr$new(param_vals = list(cutoff = 0.9)))
# result = gr$train(task)
# result[[1]]$data()
# predres = gr$predict(task)
