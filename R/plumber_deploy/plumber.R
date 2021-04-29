library(plumber)
library(exuber)
library(fracdiff)
library(dpseg)


# load packages
library(data.table)
library(mlr3verse)

# import ML model
model <- readRDS('ml_model_risks.rds')


#* @apiTitle AlphaR API
#* @apiDescription Endpoints for finding alpha for investing on financial markets


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
  radf(x, lag = adf_lag)
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
function(time, price, type_, p) {
  segs <- dpseg(time, price, jumps=FALSE, P=p, type=type_, verb=FALSE)
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
function(x) {
  bc_greater <- backCUSUM::BQ.test(x ~ 1, alternative = "greater")
  bc_greater[['statistic']]
}

#* Var
#* @param x vector of returns
#* @param prob p VaR
#* @param type type of VaR
#* @post /varrisk
function(x, prob = 0.95, type = 'gaussian') {
  returns <- na.omit((x - data.table::shift(x)) / data.table::shift(x))
  PerformanceAnalytics::VaR(returns, p = prob, method = type, clean = 'none', portfolio_method = 'single')[1]
}

#* GAS ES
#* @param x vector of returns
#* @param dist distribution
#* @param scaling_type scaling
#* @param h horizont
#* @param p threshold
#* @post /gas
function(x, dist = 'std', scaling_type = 'Identity', h = 1, p = 0.01) {
  GASSpec <- GAS::UniGASSpec(
    Dist = dist,
    ScalingType = scaling_type,
    GASPar = list(location = TRUE, scale = TRUE, skewness = TRUE, shape = TRUE))
  Fit <- GAS::UniGASFit(GASSpec, x, Compute.SE = FALSE)
  Forecast <- GAS::UniGASFor(Fit, H = h, ReturnDraws = TRUE)
  GAS::quantile(Forecast, p)
}

#* General Pareto Distribution fit
#* @param x vector of returns
#* @param threshold return threshold
#* @param method estimation method
#* @param p apha for quantiles
#* @post /gpd
function(x, threshold = -0.001, method = 'pwm', p = 0.999) {
  out <- evir::gpd(x, threshold = threshold, method = method)
  out <- evir::riskmeasures(out, p)[1, 3]
  return(out)
}

#* ML model
#* @param features vector of feature values
#* @post /ml_model_risks
function(features){
  features <- data.table(t(features))
  colnames(features) <- model$model$learner$state$train_task$feature_names
  predictions <- model$model$learner$predict_newdata(newdata = features)
  probabilities <- as.vector(predictions$prob)
  return(probabilities)
}
