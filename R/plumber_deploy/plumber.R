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
