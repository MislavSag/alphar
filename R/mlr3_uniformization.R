PipeOpUniform = R6::R6Class(
  "PipeOpUniform",
  inherit = mlr3pipelines::PipeOpTaskPreproc,
  public = list(
    groups = NULL,
    initialize = function(id = "uniformization", param_vals = list()) {
      super$initialize(id, param_vals = param_vals, feature_types = c("numeric", "integer"))
    }
  ),

  private = list(

    .select_cols = function(task) {
      self$groups = task$groups
      task$feature_names
    },

    .train_dt = function(dt, levels, target) {
      # state variables
      if (!(is.null(self$groups))) {
        row_ids  = self$groups[group == self$groups[nrow(self$groups), group], row_id]
        ecdf_ = mlr3misc::map(dt[row_ids], ecdf)
      } else {
        ecdf_ = mlr3misc::map(dt, ecdf)
      }
      self$state = list(
        ecdf_ = ecdf_
      )

      # dt object train
      if (!(is.null(self$groups))) {
        dt = dt[, lapply(.SD, function(x) as.vector(ecdf(x)(x))), by = self$groups[, group]]
        dt = dt[, -1]
      } else {
        dt = dt[, lapply(.SD, function(x) ecdf(x)(x))]
      }
      dt
    },

    .predict_dt = function(dt, levels) {
      dt[, Map(function(a, b) b(a), .SD, self$state$ecdf_)]
    }
  )
)


# library("mlr3")
#
# # no group variable
# task = tsk("iris")
# gr = Graph$new()
# gr$add_pipeop(PipeOpUniform$new())
# result = gr$train(task)
# result[[1]]$data()
# gr$predict(task)
#
# # group variable
# dt = tsk("iris")$data()
# dt[, monthid := c(rep(1, 50), rep(2, 50), rep(3, 50))]
# task = as_task_classif(dt, target = "Species")
# task$set_col_roles("monthid", "group")
# gr = Graph$new()
# gr$add_pipeop(PipeOpUniform$new())
# result = gr$train(task)
# result[[1]]$data()
# preds = gr$predict(task)
# preds$uniformization.output$data()
