AdjLoss2 = R6::R6Class(
  "AdjLoss2",
  inherit = mlr3::MeasureRegr,
  public = list(
    initialize = function() {
      super$initialize(
        # custom id for the measure
        id = "adjloss2",

        # additional packages required to calculate this measure
        packages = character(),

        # properties, see below
        properties = character(),

        # required predict type of the learner
        predict_type = "response",

        # feasible range of values
        range = c(0, Inf),

        # minimize during tuning?
        minimize = TRUE
      )
    }
  ),

  private = list(
    # custom scoring function operating on the prediction object
    .score = function(prediction, ...) {
      adjloss2 = function(truth, response, alpha = 2.5) {
        mlr3measures:::assert_regr(truth, response = response)
        se = (truth - response)^2
        mean(ifelse((truth * response) < 0, se * alpha, se))
      }
      adjloss2(prediction$truth, prediction$response)
    }
  )
)

# x = runif(100)
# y = runif(100) * sample(c(1, -1), 100, replace = TRUE)
# alpha = 2.5
# se = (y - x)^2
# cond = y * x
# ifelse(cond < 0, se * alpha, se)


# linex(x, y)
# linex(x, y, a1 = 1, a2 = -2)
# root_mse = function(truth, response) {
#   mse = mean((truth - response)^2)
#   sqrt(mse)
# }
#
# root_mse(prediction$truth, prediction$response)
# root_mse(c(0, 0.5, 1), c(0.5, 0.5, 0.5))
