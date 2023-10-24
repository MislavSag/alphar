#' @title Linear-exponential Loss
#'
#' @details
#' Linear-exponential, or Linex, loss takes the form \deqn{
#'   L(e)  =  a{1}(exp(a a{2}e)−a{2}e −1).
#' }
#'
#' @templateVar mid linex
#' @template regr_template
#'
#' @inheritParams regr_params
#' @template regr_example
#' @export
linex = function(truth, response, a1 = 1, a2 = -1) {
  mlr3measures:::assert_regr(truth, response = response)
  if (a2 == 0) stop("Argument a2 can't be 0.")
  if (a1 <= 0) stop("Argument a1 must be greater than 0.")
  e = truth - response
  a1 * (exp(-a2*e) - a2*e - 1)
}

#' @include measures.R
mlr3measures:::add_measure(linex, "Linear-exponential Loss", "regr", 0, Inf, TRUE)
