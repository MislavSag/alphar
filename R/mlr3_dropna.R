library(mlr3pipelines)
library(mlr3verse)
library(mlr3misc)
library(R6)

PipeOpDropNA = R6::R6Class(
  "PipeOpDropNA",
  inherit = mlr3pipelines::PipeOpTaskPreproc,
  public = list(
    initialize = function(id = "drop.na") {
      super$initialize(id)
    }
  ),

  private = list(
    .train_task = function(task) {
      self$state = list()
      featuredata = task$data(cols = task$feature_names)
      exclude = apply(is.na(featuredata), 1, any)
      task$filter(task$row_ids[!exclude])
    },

    .predict_task = function(task) {
      featuredata = task$data(cols = task$feature_names)
      exclude = apply(is.na(featuredata), 1, any)
      task$filter(task$row_ids[!exclude])
    }
  )
)
#
# # no group variable
# task = tsk("iris")
# new_dt = data.table("setosa", 1,2, 3, NA)
# setnames(new_dt, names(task$data()))
# task$rbind(new_dt)
# task$data()
# gr = Graph$new()
# gr$add_pipeop(PipeOpDropNA$new())
# result = gr$train(task)
# result[[1]]$data()
# gr$predict(task)
