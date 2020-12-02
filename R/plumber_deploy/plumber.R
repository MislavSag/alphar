#
# This is a Plumber API. In RStudio 1.2 or newer you can run the API by
# clicking the 'Run API' button above.
#
# In RStudio 1.1 or older, see the Plumber documentation for details
# on running the API.
#
# Find out more about building APIs with Plumber here:
#
#    https://www.rplumber.io/
#

library(plumber)
library(exuber)
library(fracdiff)
library(dpseg)

#* @apiTitle Plumber Example API

#* Echo back the input
#* @param msg The message to echo
#* @get /echo
function(msg=""){
  list(msg = paste0("The message is: '", msg, "'"))
}

#* Plot a histogram
#* @serializer png
#* @get /plot
function(){
  rand <- rnorm(100)
  hist(rand)
}

#* Return the sum of two numbers
#* @param a The first number to add
#* @param b The second number to add
#* @post /sum
function(a, b){
  as.numeric(a) + as.numeric(b)
}

#* Recursive Augmented Dickey-Fuller Test
#* @param x Vector of values (prices)
#* @param adf_lag The lag length of the Augmented Dickey-Fuller regression (default = 0L)
#* @post /radf
function(x, adf_lag){
  radf(x, minw= 50, lag = adf_lag)
}

#* Fin min_d for fracdiff
#* @param x vector of values
#* @post /mind
function(x) {
  min_d_fdGPH <- fdGPH(x)
  min_d_fdSperio <- fdSperio(x)
  min_d <- mean(c(min_d_fdGPH$d, min_d_fdSperio$d))
  min_d
}

#* Diffseries
#* @param x vector of values
#* @param min_d minimal d
#* @post /fracdiff
function(x, min_d) {
  diffseries(x, min_d)
}

#* Alpha dpseg
#* @param time Time se float
#* @param price vector of prices
#* @param p break-point penalty
#* @param type_ type of scoring function
#* @post /dpseg
function(time, price, type_, p = NA) {
  if (is.na(p) | is.null(p)) {
    p <- estimateP(x=time, y=price, plot=FALSE)
  }
  segs <- dpseg(time, price, jumps=FALSE, P=p, type=type_, store.matrix=TRUE, verb=FALSE)
  slope_last <- segs$segments$slope[length(segs$segments$slope)]
  return(slope_last)
}

#* Alpha backcusum volatility
#* @param time Time se float
#* @param price vector of prices
#* @param critical_value Critical value for backCUSUM detector
#* @param method Look help for spotVol in highfrequency package
#* @param marketOpen_ Look help for spotVol in highfrequency package
#* @param marketClose_ Look help for spotVol in highfrequency package
#* @param tz_ Look help for spotVol in highfrequency package
#* @post /backcusumvol
function(time, price, critical_value = 0.4, method_ = 'detPer', k_time = 10,
         marketOpen_ = '09:30:00', marketClose_ = '16:00:00', tz_ = 'America/New_York') {

  # construct xts xts
  time <- anytime::anytime(time)
  x <- xts::xts(unlist(price), order.by = unlist(time), tzone = tz_)

  # volatility
  vol1 <- tryCatch(
    highfrequency::spotVol(
      x,
      method = method_,
      on = 'minutes',
      k = k_time,
      marketOpen = marketOpen_,
      marketClose = marketClose_,
      tz = tz_),
    error = function(e) print(e)
    )
  if (exists('message', vol1)) {
    return(vol1$message)
  } else {
    vol1 <- vol1$spot
    vol1 <- na.omit(vol1)
    vol1 <- vol1[1:100]
    bq <- backCUSUM::BQ.test(vol1 ~ 1, alternative = "greater")
    value_test <- purrr::pluck(bq, 'detector')
    value_test <- tail(value_test, 1)
    if (value_test > critical_value) {
      alpha <- 0
    } else {
      alpha <- 1
    }
  }
  return(alpha)
}

#* BackCUSUM
#* @param x vector of values
#* @param alternative look at BQ.test docs
#* @post /backcusum
function(x, rejection_value_down = 0.5, rejection_value_up = 2) {

  bc_greater <- backCUSUM::BQ.test(x ~ 1, alternative = "greater")

  if (bc_greater[['statistic']] > rejection_value_down & bc_greater[['statistic']] < rejection_value_up) {
    alpha_sign <- 0
  } else {
    alpha_sign <- 1
  }
  return(alpha_sign)
}
