library(mlr3pipelines)
library(mlr3verse)
library(mlr3misc)
library(R6)
library(paradox)

PipeOpWinsorizeSimple = R6::R6Class(
  "PipeOpWinsorizeSimple",
  inherit = mlr3pipelines::PipeOpTaskPreprocSimple,
  public = list(
    groups = NULL,
    initialize = function(id = "winsorization", param_vals = list()) {
      ps = ParamSet$new(list(
        ParamDbl$new("probs_low", lower = 0, upper = 1, default = 0.05, tags = c("winsorize_tag")),
        ParamDbl$new("probs_high", lower = 0, upper = 1, default = 0.95, tags = c("winsorize_tag")),
        ParamLgl$new("na.rm", default = TRUE, tags = c("winsorize_tag")),
        ParamInt$new("qtype", lower = 1L, upper = 9L, default = 7L, tags = c("winsorize_tag"))
      ))
      ps$values = list(qtype = 7L)
      super$initialize(id, param_set = ps, param_vals = param_vals, feature_types = c("numeric"))
    }
  ),

  private = list(

    .get_state_dt = function(dt, levels, target) {
      # debug
      # task = copy(tsk_aroundzero_month)
      # dt = tsk_aroundzero_month$data()
      # cols = tsk_aroundzero_month$feature_types[type %in% c("numeric", "integer"), id]
      # dt = dt[, ..cols]
      # pv = list(
      #   probs_low = 0.01, probs_high = 0.99, na.rm = TRUE, qtype = 7
      # )
      # self = list()

      # params
      pv = self$param_set$get_values(tags = "winsorize_tag")

      # state variables
      q = dt[, lapply(.SD,
                      quantile,
                      probs = c(pv$probs_low, pv$probs_high),
                      na.rm = pv$na.rm,
                      type = pv$qtype)]
      list(
        minvals = q[1],
        maxvals = q[2]
      )
    },

    .transform_dt  = function(dt, levels) {
      dt = dt[, Map(function(a, b) ifelse(a < b, b, a), .SD, self$state$minvals)]
      dt = dt[, Map(function(a, b) ifelse(a > b, b, a), .SD, self$state$maxvals)]
      dt
    }
  )
)


# library("mlr3")
#
# # no group variable
# task = tsk("iris")
# task$col_roles
# task$row_roles
# gr = Graph$new()
# gr$add_pipeop(PipeOpWinsorize$new(param_vals = list(probs_low = 0.2, probs_high = 0.8, na.rm = TRUE)))
# result = gr$train(task)
# result[[1]]$data()
# predres = gr$predict(task)
#
# # group variable
# dt = tsk("iris")$data()
# dt[, monthid := c(rep(1, 50), rep(2, 50), rep(3, 50))]
# task = as_task_classif(dt, target = "Species")
# task$set_col_roles("monthid", "group")
# gr = Graph$new()
# gr$add_pipeop(PipeOpWinsorize$new(param_vals = list(probs_low = 0.2, probs_high = 0.8, na.rm = TRUE)))
# result = gr$train(task)
# result[[1]]$data()
# predres = gr$predict(task)
