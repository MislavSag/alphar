### STEPS TO CHECK WHEN ADDING NEW FUNCTION
# 1. check ig package we use is installed on droplet. Check for side pacakages (e. g. utils)
# 2. check if we have load package in plumber file with library
# 3. check endpoint in plumber and test
# 4. check argument name in plumber and test
# 5. check the data exists for the mlr3 model
# 6. check the plumber is redeployed

library(plumber)
library(exuber)
library(fracdiff)
library(dpseg)
library(mrisk)
library(httr)
library(quarks)
library(Rcatch22)
library(mlr3automl)
library(mlr3extralearners)
library(backCUSUM)

# load packages
library(data.table)
library(mlr3verse)

# import ML model
model = readRDS('ml_model_risks.rds')
model_hft = readRDS('ml_model_hft.rds')
# model_hft = readRDS('R/plumber_deploy/ml_model_hft.rds')


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


#* Backcusum filter
#* @param returns
#* @post /backcusumfilter
function(returns) {

  # returns <- rnorm(200)

  # calualte backcusum returns
  y <- na.omit(returns)
  y <- SBQ.test(as.formula('y ~ 1'), alternative = 'greater')# [['statistic']]
  results <- c(y[['statistic']], as.integer(y[['rejection']]))
  names(results) <- c("statistics", paste0("backcusum_rejections_", as.numeric(names(y[['rejection']])) * 1000))
  results <- as.data.table(as.list(results))

  return(results)
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


#* mlr3 hft model
#* @param close close prices
#* @post /ml_model_hft
function(close) {

  # FOR TEST
  # close = get_daily_prices("SPY", Sys.Date() - 1000, Sys.Date())
  # close = close$close

  # calcualte features
  exuber_600_4_gsadf <- radf(tail(close, 600), minw = psy_minw(close), lag = 4L)
  exuber_600_4_gsadf <- exuber::tidy(exuber_600_4_gsadf)
  exuber_600_4_gsadf <- exuber_600_4_gsadf$gsadf
  catch22_CO_Embed2_Dist_tau_d_expfit_meandiff_264 <- Rcatch22::CO_Embed2_Dist_tau_d_expfit_meandiff(tail(close, 264))

  # merge features
  features <- data.table(
    exuber_600_4_gsadf = exuber_600_4_gsadf,
    catch22_CO_Embed2_Dist_tau_d_expfit_meandiff_264 = catch22_CO_Embed2_Dist_tau_d_expfit_meandiff_264)

  # make predictions
  predictions <- model_hft$learner$model$learner$predict_newdata(newdata = features)
  probabilities <- as.vector(predictions$prob)
  return(probabilities)
}


#* Radf point
#* @param symbols Vector of symbols
#* @param date Last date
#* @param window Window size
#* @param price_lag Number of lags to use in exuber
#* @param use_log Use/not use log
#* @param time Time frequency to use, eg hour minute
#* @get /radf_point
function(symbols, date, window, price_lag, use_log, time){
  use_log <- as.logical(as.integer(use_log))
  window <- as.numeric(window)
  price_lag <- as.numeric(price_lag)
  date <- as.character(as.Date(substr(date, 1, 8), "%Y%m%d"))
  paste("Arguments: ", symbols, date, window, price_lag, use_log)
  x <- mrisk::radf_point(symbols, date, window, price_lag, use_log, "15cd5d0adf4bc6805a724b4417bbaafc", time)
  x
}
# symbols = "SPY"
# date = "20210628000000"
# window = 100
# price_lag = 1
# use_log = 1
# time = "minute"
# format(Sys.time(), tz="America/New_York", usetz=TRUE)

#* Radf point sp500
#* @param date Last date
#* @param window Window size
#* @param price_lag Number of lags to use in exuber
#* @param use_log Use/not use log
#* @param agg_type Aggregation type
#* @param number_of_assets Number of assets from SP500 to use
#* @get /radf_point_sp
function(date, window, price_lag, use_log, agg_type, number_of_assets){
  api_key <- "15cd5d0adf4bc6805a724b4417bbaafc"
  url <- paste0("https://financialmodelingprep.com/api/v3/sp500_constituent?apikey=", api_key)
  sp500_symbols <- httr::content(httr::GET(url))
  sp500_symbols <- unlist(lapply(sp500_symbols, function(x) x[["symbol"]]))
  use_log <- as.logical(as.integer(use_log))
  window <- as.numeric(window)
  price_lag <- as.numeric(price_lag)
  date <- as.character(as.Date(substr(date, 1, 8), "%Y%m%d"))
  number_of_assets <- as.integer(number_of_assets)
  radfs <- list()
  # i = 1
  for (i in seq_along(sp500_symbols[1:number_of_assets])) {
     x <- mrisk::radf_point(sp500_symbols[i], date, window, price_lag, use_log, "15cd5d0adf4bc6805a724b4417bbaafc", time = "hour")
     radfs[[i]] <- cbind(symbol = sp500_symbols[i], x)
  }
  result <- rbindlist(radfs)
  if (agg_type == "std") {
    measures <- colnames(result)[3:ncol(result)]
    measures_agg <- result[, lapply(.SD, sd), .SDcols=measures]
    result <- cbind.data.frame(datetime=max(result$datetime), measures_agg)
  } else if (agg_type == "null") {
    result <- as.data.frame(result)
  }
  return(result)
}
# date = format.Date(Sys.Date(), "%Y%m%d")
# window = 100
# price_lag = 1
# use_log = 1
# agg_type = "std"
# number_of_assets = 10

#* VaR and ES
#* @param x Vector of returns
#* @param p conf level
#* @param model model for estimating conditional volatility
#* @param method method to be used
#* @param nwin Use/not use log
#* @param nout Aggregation type
#* @post /quark
function(x, p, model, method, nwin, nout) {
  x <- diff(log(x))
  y <- rollcast(x = x,
                p = p,
                model = model,
                method = method,
                nout = nout,
                nwin = nwin)

  # VaR stats
  var_1 <- y$VaR[1]
  var_day <- mean(y$VaR[1:8], na.rm = TRUE)
  var_week <- mean(y$VaR[1:40], na.rm = TRUE)
  var_month <- mean(y$VaR, na.rm = TRUE)
  var_std <- sd(y$VaR)

  # ES stats
  es_1 <- y$ES[1]
  es_day <- mean(y$ES[1:8], na.rm = TRUE)
  es_week <- mean(y$ES[1:40], na.rm = TRUE)
  es_month <- mean(y$ES, na.rm = TRUE)
  es_std <- sd(y$ES)

  # merge all in df
  VaR <- as.data.table(list(var_1 = var_1, var_day = var_day, var_week = var_week, var_month = var_month, var_std = var_std))
  ES <- as.data.table(list(es_1 = es_1, es_day = es_day, es_week = es_week, es_month = es_month, es_std = es_std))
  unlist(cbind.data.frame(VaR, ES), use.names = FALSE)
  return(cbind.data.frame(VaR, ES))
}
# x = y
# p = 0.975
# model = "EWMA"
# method = "plain"
# nwin = 100
# nout = 150
