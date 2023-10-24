# In addition: Warning messages:
#   1: PipeOp PipeOpWinsorizeSimpleGroup has construction arguments besides 'id'
# and 'param_vals' but does not overload the private '.additional_phash_input()' function.
#
# The hash and phash of a PipeOp must differ when it represents a different operation; since PipeOpWinsorizeSimpleGroup has construction arguments that could change the
# operation that is performed by it, it is necessary for the $hash and $phash to reflect this. `.additional_phash_input()`
# should return all the information (e.g. hashes of encapsulated items) that should additionally be hashed; read the help of ?PipeOp for more information.

# library(mlr3pipelines)
# library(mlr3verse)
# library(mlr3misc)
# library(R6)
# library(paradox)
#
PipeOpWinsorizeSimpleGroup = R6::R6Class(
  "PipeOpWinsorizeSimpleGroup",
  inherit = mlr3pipelines::PipeOpTaskPreprocSimple,
  public = list(
    group_var = NULL,
    initialize = function(group_var, id = "winsorization", param_vals = list()) {
      self$group_var = group_var
      ps = ParamSet$new(list(
        ParamDbl$new("probs_low", lower = 0, upper = 1, default = 0.05, tags = c("winsorize_tag")),
        ParamDbl$new("probs_high", lower = 0, upper = 1, default = 0.95, tags = c("winsorize_tag")),
        ParamLgl$new("na.rm", default = TRUE, tags = c("winsorize_tag")),
        ParamInt$new("qtype", lower = 1L, upper = 9L, default = 7L, tags = c("winsorize_tag"))
      ))
      ps$values = list(qtype = 7L)
      super$initialize(id, param_set = ps, param_vals = param_vals) # , feature_types = c("numeric", "integer")
    }
  ),

  private = list(

    .select_cols = function(task) {
      task$feature_types[type %in% c("numeric"), id]
    },

    .get_state = function(task) {
      # debug
      # task = copy(tsk_aroundzero_month)
      # dt = tsk_aroundzero_month$data()
      # cols = tsk_aroundzero_month$feature_types[type %in% c("numeric", "integer"), id]
      # dt = dt[, ..cols]
      # pv = list(
      #   probs_low = 0.01, probs_high = 0.99, na.rm = TRUE, qtype = 7
      # )
      # self = list()

      # need this part to use group
      dt_columns = private$.select_cols(task)
      cols = dt_columns
      if (!length(cols)) {
        return(list(dt_columns = dt_columns))
      }
      dt = task$data(cols = cols)

      # extract data and backend
      group_ = task$backend$data(cols = c(self$group_var, "..row_id"),
                                 rows = task$row_ids)
      stopifnot(all(task$row_ids == group_$`..row_id`))
      # print(group_[, .SD, .SDcols = self$group_var])
      dt = cbind(dt, group_[, .SD, .SDcols = self$group_var])

      # params
      pv = self$param_set$get_values(tags = "winsorize_tag")

      # state variables
      group_id = self$group_var
      q = dt[, lapply(
        .SD,
        quantile,
        probs = c(pv$probs_low, pv$probs_high),
        na.rm = pv$na.rm,
        type = pv$qtype
      ),
      by = c(group_id)]
      q = q[, -1, with = FALSE]
      # print(q)
      dt[, (group_id) := NULL]
      stopifnot(all(names(q) == names(dt[])))
      list(
        minvals = q[1],
        maxvals = q[2],
        dt_columns = dt_columns
      )
    },

    .transform  = function(task) {
      # from https://github.com/mlr-org/mlr3pipelines/blob/HEAD/R/PipeOpTaskPreproc.R
      cols = self$state$dt_columns
      if (!length(cols)) {
        return(task)
      }
      dt = task$data(cols = cols)
      # print(dim(dt))
      # print(dim(self$state$minvals))
      dt = dt[, Map(function(a, b) ifelse(a < b, b, a), .SD, self$state$minvals)]
      dt = dt[, Map(function(a, b) ifelse(a > b, b, a), .SD, self$state$maxvals)]
      # print(colnames(dt))
      # print(dim(dt))

      task$select(setdiff(task$feature_names, cols))$cbind(dt)
    },

    .additional_phash_input = function() self$group_var

    # .get_state_dt = function(dt, levels, target) {
    #   # debug
    #   # task = copy(tsk_aroundzero_month)
    #   # dt = tsk_aroundzero_month$data()
    #   # cols = tsk_aroundzero_month$feature_types[type %in% c("numeric", "integer"), id]
    #   # dt = dt[, ..cols]
    #   # pv = list(
    #   #   probs_low = 0.01, probs_high = 0.99, na.rm = TRUE, qtype = 7
    #   # )
    #   # self = list()
    #
    #   # params
    #   pv = self$param_set$get_values(tags = "winsorize_tag")
    #
    #   # state variables
    #   q = dt[, lapply(.SD,
    #                   quantile,
    #                   probs = c(pv$probs_low, pv$probs_high),
    #                   na.rm = pv$na.rm,
    #                   type = pv$qtype)]
    #   list(
    #     minvals = q[1],
    #     maxvals = q[2]
    #   )
    # },
    #
    # .transform_dt  = function(dt, levels) {
    #   dt = dt[, Map(function(a, b) ifelse(a < b, b, a), .SD, self$state$minvals)]
    #   dt = dt[, Map(function(a, b) ifelse(a > b, b, a), .SD, self$state$maxvals)]
    #   dt
    # }
  )
)



# library("mlr3")
#
# no group variable
# task = tsk("iris")
# task$col_roles
# task$row_roles
# gr = Graph$new()
# gr$add_pipeop(PipeOpWinsorizeSimpleGroup$new(param_vals = list(probs_low = 0.2, probs_high = 0.8, na.rm = TRUE)))
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
#
# # my data
# sample_ = task_ret_week$filter(1:10000)
# sample_pred = task_ret_week$filter(10000:20000)
# gr = Graph$new()
# gr$add_pipeop(PipeOpWinsorizeSimpleGroup$new("yearmonthid", param_vals = list(probs_low = 0.2, probs_high = 0.8, na.rm = TRUE)))
# result = gr$train(sample_)
# predres = gr$predict(sample_pred)
