prepare_strategy <- function(
  market_data,
  rolling_window,
  strategy = c('dpseg'),
  ...) {
  if (strategy == 'dpseg') {
    results <- lapply(market_data, function(x) {
      dpseg_roll(x, rolling_window, ...)
    })
  }
}



#' Alpha for DPSEG strategy
#'
#' @param data data.frame that contains price and time columns
#' @param p break-point penalty, increase to get longer segments with lower scores (eg. higher residual variance)
#' @param type type of scoring function: available are "var" for "variance of residuals", "cor" for Pearson correlation, or "r2" for r-squared; see the package vignette("dpseg") for details.
#'
#' @return last slopeof the regresion
#'
#' @examples
dpseg_last <- function(data, p = NA, type = c('var', 'cor')) {
  if (is.na(p)) {
    p <- estimateP(x=data$time, y=data$price, plot=FALSE)
  }
  segs <- dpseg(data$time, data$price, jumps=FALSE, P=p, type=type, store.matrix=TRUE, verb=FALSE)
  slope_last <- segs$segments$slope[length(segs$segments$slope)]
  return(slope_last)
}


#' Calculate roll dpseg
#'
#' @param data data is list of OHLCV dataframe
#' @param alpha_type type in dpseg package
#' @param rolling_window windows length
#'
#' @return data frame with original and strategy returns
#'
#' @examples
dpseg_roll <- function(data, alpha_type, rolling_window = 600) {
  dpseg_data <- cbind.data.frame(time = zoo::index(data), price = Cl(data))
  colnames(dpseg_data) <- c('time', 'price')
  DT <- as.data.table(dpseg_data)
  DT[, time := as.numeric(time)]
  DT[,
     slope := runner(
       x = .SD,
       f = function(x) {
         dpseg_last(x, type = alpha_type)
       },
       k = rolling_window,
       na_pad = TRUE
     )]
  results <- xts::xts(DT[, c(2, 3)], order.by = dpseg_data$time)
  results <- na.omit(results)
  return(results)
}
