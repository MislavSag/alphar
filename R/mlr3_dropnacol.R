library(mlr3pipelines)
library(mlr3verse)
library(mlr3misc)
library(R6)

PipeOpDropNACol = R6::R6Class(
  "PipeOpDropNACol",
  inherit = mlr3pipelines::PipeOpTaskPreprocSimple,
  public = list(
    initialize = function(id = "drop.nacol", param_vals = list()) {
      ps = ParamSet$new(list(
        ParamDbl$new("cutoff", lower = 0, upper = 1, default = 0.05, tags = c("dropnacol_tag"))
      ))
      ps$values = list(cutoff = 0.2)
      super$initialize(id, param_set = ps, param_vals = param_vals)
    }
  ),

  private = list(
    .get_state = function(task) {
      pv = self$param_set$get_values(tags = "dropnacol_tag")
      features_names = task$feature_names
      data = task$data(cols = features_names)
      keep = sapply(data, function(column) (sum(is.na(column))) / length(column) < pv$cutoff)
      list(cnames = colnames(data)[keep])
    },

    .transform = function(task) {
      task$select(self$state$cnames)
    }
  )
)

# # no group variable
# task = tsk("iris")
# dt = task$data()
# dt[1:50, Sepal.Width := NA]
# task = as_task_classif(dt, target = "Species")
#
# gr = Graph$new()
# gr$add_pipeop(PipeOpDropNACol$new())
# result = gr$train(task)
# result[[1]]$data()
# gr$predict(task)
