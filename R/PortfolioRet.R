PortfolioRet = R6::R6Class(
  "PortfolioRet",
  inherit = mlr3::MeasureRegr,
  public = list(
    initialize = function() {
      super$initialize(
        # custom id for the measure
        id = "portfolio_ret",

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
      portfolio_ret_div = function(truth, response) {
        mlr3measures:::assert_regr(truth, response = response)

        # index responses greater than zero
        responses_positive_index = which(response > 0)
        response_positive = response[responses_positive_index]

        # create portfolio weigths
        # print(response_positive)
        weigths_ = response_positive / sum(response_positive)

        # get truth responses
        truth_resp = truth[responses_positive_index]

        # calculate portfolio return
        # print(truth_resp)
        # print(weigths_)
        -sum(truth_resp * weigths_)
      }

      portfolio_ret_div(prediction$truth, prediction$response)
      # linex(c(0, 0.5, 1), c(0.5, 0.5, 0.5))
      # root_mse = function(truth, response) {
      #   mse = mean((truth - response)^2)
      #   sqrt(mse)
      # }
      #
      # root_mse(prediction$truth, prediction$response)
      # root_mse(c(0, 0.5, 1), c(0.5, 0.5, 0.5))
    }
  )
)

# # example
# library(mlr3verse)
# task = tsk("boston_housing")
# split = partition(task)
# learner = lrn("regr.featureless")$train(task, split$train)
# prediction = learner$predict(task, split$test)
# prediction$score(PortfolioRet$new())
