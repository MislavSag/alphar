library(data.table)
library(mrisk)
library(leanr)
library(httr)
library(rvest)
library(ggplot2)
library(purrr)
library(cpm)
library(runner)
# risk models
library(GAS)
library(evir)
library(quarks)
# performance
library(parallel)
library(future.apply)



# IMPORT DATA -------------------------------------------------------------

# import data
save_path <- "D:/risks/risk-factors"
freq <- "minute"
save_path <- file.path(save_path, freq)
stocks <- import_lean('D:/market_data/equity/usa/hour/trades_adjusted')

# help vars
stocks[, N_ :=.N, by = symbol]
stocks <- setorder(stocks, symbol, datetime)
stocks[, returns := close / shift(close) - 1, by = symbol]
stocks <- na.omit(stocks)

#
# help functions
get_series_statistics <- function(series) {
  var_1 <- series[1]
  var_day <- mean(series[1:8], na.rm = TRUE)
  var_week <- mean(series[1:40], na.rm = TRUE)
  var_month <- mean(series, na.rm = TRUE)
  var_std <- sd(series, na.rm = TRUE)
  return(list(var_1 = var_1, var_day = var_day, var_week = var_week, var_month = var_month, var_std = var_std))
}


# VAR AND ES RISK MEASURES -----------------------------------------------------

# parameters
symbol = unique(stocks$symbol)
symbol = setdiff(symbol, 'SPY')
p = c(0.95, 0.975, 0.99)
win_size = c(100, 200, 400, 800)
model = c("EWMA", "GARCH")
method = c("plain", "age", "vwhs", "fhs")
forecast_length = 150
params <- expand.grid(symbol, p, model, method, win_size, forecast_length, stringsAsFactors = FALSE)
colnames(params) <- c("symbol", "p", "model", "method", "win_size", "forecast_length")
params <- params[!(params$model == 'GARCH' & params$method == "fhs"), ]

# get forecasts
for (i in 1:nrow(params)) {

  # params
  params_ <- params[i, ]
  print(params_)

    # create symbol if it doesnt already exists
  if (!dir.exists(file.path(save_path, tolower(params_$symbol)))) {
    dir.create(file.path(save_path, tolower(params_$symbol)))
  }

  # create file name
  params_$p <- params_$p * 1000
  file_name <- paste0(paste0(paste(params_, sep = "_"), collapse = "_"), ".csv")
  file_name <- file.path(save_path, tolower(params_$symbol), file_name)
  params_$p <- params_$p / 1000

  # cont if file exists
  if (file.exists(file_name)) {
    next()
  }

  # take sample
  sample_ <- stocks[symbol == params_$symbol]

  # stop if number of rows is smaller than params_$win_size + params_$forecast_length
  if (nrow(sample_) < params_$win_size + params_$forecast_length ) {
    next()
  }

  # set up clusters
  cl <- makeCluster(16)
  clusterExport(cl, c("sample_", "params_", "get_series_statistics"), envir = environment())

  # roll estimation
  roll_quarks <- runner(
    x = data.frame(y=sample_$returns),
    f = function(x) {
      library(quarks)
      library(data.table)
      print(x[1])
      if (params_$method == "fhs") {
        y <- rollcast(x,
                      p = params_$p,
                      model = params_$model,
                        method = params_$method,
                      nout = params_$forecast_length,
                      nwin = params_$win_size,
                      nboot = 1000
        )
      } else {
        y <- rollcast(x,
                      p = params_$p,
                      model = params_$model,
                      method = params_$method,
                      nout = params_$forecast_length,
                      nwin = params_$win_size
        )
      }
      VaR <- as.data.table(get_series_statistics(y$VaR))
      ES <- as.data.table(get_series_statistics(y$ES))
      colnames(ES) <- gsub("var_", "es_", colnames(ES))
      return(cbind.data.frame(VaR, ES))
    },
    k = params_$win_size + params_$forecast_length,
    na_pad = TRUE,
    cl = cl
  )
  stopCluster(cl)

  # clean table
  risks <- rbindlist(lapply(roll_quarks, as.data.frame), fill = TRUE)[, -1]
  risks <- cbind.data.frame(datetime = sample_$datetime, risks)

  # save
  fwrite(risks, file_name)
}



# CHANGEPOINTS (FROM HERE OLD CODE) ---------------------------------------

# estimate changepoints using cpm package
arl0 <- c(370, 500, 600, 700, 800, 900, 1000, 2000, 3000, 4000, 5000, 6000,
          7000, 8000, 9000, 10000, 20000, 30000, 40000, 50000)
get_changepoints <- function(returns, method, arl0) {
  # change points roll
  detectiontimes <- numeric()
  changepoints <- numeric()
  cpm <- makeChangePointModel(cpmType=method, ARL0=arl0, startup=200)
  i <- 0
  while (i < length(returns)) {
    i <- i + 1

    # process each observation in turn
    cpm <- processObservation(cpm, returns[i])

    # if a change has been found, log it, and reset the CPM
    if (changeDetected(cpm) == TRUE) {
      detectiontimes <- c(detectiontimes,i)

      # the change point estimate is the maximum D_kt statistic
      Ds <- getStatistics(cpm)
      tau <- which.max(Ds)
      if (length(changepoints) > 0) {
        tau <- tau + changepoints[length(changepoints)]
      }
      changepoints <- c(changepoints,tau)

      # reset the CPM
      cpm <- cpmReset(cpm)

      #resume monitoring from the observation following the change point
      i <- tau
    }
  }
  points <- cbind.data.frame(detectiontimes, changepoints)
  breaks <- rep(FALSE, length(returns))
  breaks[detectiontimes] <- TRUE
  change <- rep(FALSE, length(returns))
  change[changepoints] <- TRUE
  return(cbind.data.frame(breaks, change))
}

# estimate breaks
for (i in c(370, 500, 1000, 5000)) {
  stocks[, paste(c('breaks', 'changes'), i, sep = '_') := get_changepoints(returns, method = 'Mood', i), by = .(symbol)]
}

# save
column_save <- c('.id', 'index', colnames(stocks)[grep('breaks|chang', colnames(stocks))])
fwrite(stocks[, ..column_save], file.path(save_path, 'changepoints.csv'))



# GPD --------------------------------------------------------

# prepare data
DT <- as.data.table(stocks)
DT <- DT[, .(symbol, datetime, close, returns)]
setnames(DT, colnames(DT), c('symbol', 'date', 'close', 'return'))
DT <- DT[DT[, .N, by = .(symbol)][N > 2500], on = "symbol"] # stock have at least 1 year of data
DT <- na.omit(DT)

# GDP ES and VaR
roll_windows = c(100, 250) # 25 and 50 days
thresholds = c(0.01, 0.015)
parameters <- expand.grid(roll_windows, thresholds, stringsAsFactors = FALSE)
colnames(parameters) <- c('w', 'threshold')
p <- c(0.999, 0.9999)

# estimate gpd statistics
estimate_gpd <- function(returns, threshold, window, p, ncores = 8L) {
  cl <- makeCluster(ncores)
  clusterExport(cl, c("returns", "threshold", "window", "p"), envir = environment())
  gpd_roll <- runner::runner(
    x = returns,
    f = function(x) {
      gpd_est_last <- tryCatch({
        library(data.table)
        gpd_est <- evir::gpd(x, threshold = threshold, method = 'ml', information = 'expected')
        es_q <- evir::riskmeasures(gpd_est, p)
        es_q <- as.data.frame(t(as.vector(es_q[, 2:ncol(es_q)])))
        es_q <- cbind.data.frame(es_q, gpd_est$n.exceed, gpd_est$par.ests[1], gpd_est$par.ests[2])
        colnames(es_q) <- c(paste0(apply(expand.grid(c('q_', 'e_'), p * 10000), 1, function(x) paste0(x, collapse = '')),
                                   '_', window, '_', threshold * 1000),
                            paste('n_exceed', window, threshold * 1000, sep = '_'),
                            paste('xi', window, threshold * 1000, sep = '_'),
                            paste('beta', window, threshold * 1000, sep = '_'))
        es_q
      }, error = function(e) data.table(NA))
      return(gpd_est_last)
    },
    k = window,
    na_pad = TRUE,
    cl = cl
  )
  stopCluster(cl)
  gpd_roll <- rbindlist(gpd_roll, fill = TRUE)
  return(gpd_roll)
}

# for gpd risks estimates
gpd_risks_estimate <- function(returns, no_cores = 16) {
  gpd_params <- lapply(1:nrow(parameters), function(i) {
    estimate_gpd(returns, parameters[i, 2], parameters[i, 1], p, no_cores)[, V1 := NULL]
  })
  gpd_params <- do.call(cbind, gpd_params)
  return(gpd_params)
}

# gpd risks
gpd_risks_left_tail <- DT[, gpd_risks_estimate(return * -1, no_cores = 25), by = .(symbol)]
gpd_risks_right_tail <- DT[, gpd_risks_estimate(return, no_cores = 25), by = .(symbol)]

# convert tu numeric becuase some columns are characters for some reaseon
cols <- colnames(gpd_risks_right_tail)[grep('q_|e_', colnames(gpd_risks_right_tail))]
gpd_risks_right_tail[, (cols) := lapply(.SD, as.numeric), .SDcols = cols]
gpd_risks_left_tail[, (cols) := lapply(.SD, as.numeric), .SDcols = cols]

# cbind date column
gpd_risks_right_tail <- cbind(date = DT$date, gpd_risks_right_tail)
gpd_risks_left_tail <- cbind(date = DT$date, gpd_risks_left_tail)

# fread
gpd_risks_left_tail_old <- fread(file.path(save_path, 'gpd_risks_left_tail.csv'), sep = ';')
gpd_risks_right_tail_old <- fread(file.path(save_path, 'gpd_risks_right_tail.csv'), sep = ';')
gpd_risks_left_tail_old <- merge(gpd_risks_left_tail_old, gpd_risks_left_tail,
                                 by = c("symbol", 'date'), all.x = TRUE, all.y = TRUE)
gpd_risks_right_tail_old <- merge(gpd_risks_right_tail_old, gpd_risks_right_tail,
                                  by = c("symbol", 'date'), all.x = TRUE, all.y = TRUE)

# save
fwrite(gpd_risks_left_tail_old, file.path(save_path, 'gpd_risks_left_tail.csv'), sep = ';')
fwrite(gpd_risks_right_tail_old, file.path(save_path, 'gpd_risks_right_tail.csv'), sep = ';')




# PTSUITE -----------------------------------------------------------------

# function to calculate all mesures
estimate_gpd <- function(x, hill_threshold = 0.02, suffix = '_right') {
  columns_names <- c(
    'ptest',
    'hill_shape',
    'scales_geometric_percentiles_method',
    'scales_least_squares',
    'scales_method_of_moments',
    'scales_modified_percentiles_method',
    'scales_weighted_least_squares',
    'shapes_geometric_percentiles_method',
    'shapes_least_squares',
    'shapes_method_of_moments',
    'shapes_modified_percentiles_method',
    'shapes_weighted_least_squares'
  )
  columns_names <- paste0(columns_names, suffix)
  if (length(x) == 0) {
    risks <- data.table(t(rep(0, length(columns_names))))
    setnames(risks, colnames(risks), columns_names)
  } else {
    ptest <- pareto_test(x)$`p-value`
    estimates <- as.data.table(generate_all_estimates(x))
    shapes <- data.table::dcast(estimates[, 1:2], . ~ `Method.of.Estimation`, value.var = 'Shape.Parameter')
    shapes <- shapes[, 2:ncol(shapes)]
    colnames(shapes) <- paste0('shapes_', colnames(clean_names(shapes)))
    scales <- data.table::dcast(estimates[, c(1, 3)], . ~ `Method.of.Estimation`, value.var = 'Scale.Parameter')
    scales <- scales[, 2:ncol(scales)]
    colnames(scales) <- paste0('scales_', colnames(clean_names(scales)))
    hill_estimate <- alpha_hills(x, hill_threshold, FALSE)
    hill_shape <- hill_estimate$shape
    risks <- as.data.table(data.frame(as.list(c(scales, shapes))))
    risks <- cbind(ptest, hill_shape, risks)
    colnames(risks) <- paste0(colnames(risks), suffix)
  }
  return(risks)
}

# roll measures from ptsuite package
roll_gpd <- function(returns, window_ = 500, threshold = 0.02) {
  library(parallel)
  # calculate shapes on rolling windows
  cl <- makeCluster(16)
  environment(window_) <- .GlobalEnv
  environment(threshold) <- .GlobalEnv
  clusterExport(cl, c("estimate_gpd", "window_", "threshold"), envir=environment())
  evt <- runner::runner(
    x = returns,
    f = function(x) {
      library(data.table)
      library(ptsuite)
      library(janitor)

      # pareto left tail
      x_left <- x[x < -threshold] * -1
      risks_left <- estimate_gpd(x_left, hill_threshold = 0.02, suffix = '_left')

      # pareto test right tail
      x_right <- x[x > threshold]
      risks_right <- estimate_gpd(x_right, hill_threshold = 0.02)

      # estimate shape parameters
      pareto_tests <- cbind(risks_left, risks_right)
      return(pareto_tests)
    },
    k = window_,
    na_pad = TRUE,
    type = 'auto',
    cl = cl
  )
  stopCluster(cl)
  risks <- rbindlist(evt, fill = TRUE)
  return(risks)
}

# params

# roll gpd across params
roll_ptsuite_params <- function() {

}

# estimate indicators
DT_sample <- na.omit(stocks[N_ > 1000])
cols <- colnames(roll_gpd(DT_sample[1:50, (returns)], window_ = 20, threshold = 0.02))
DT_sample[, (cols) := roll_gpd(returns), by = symbol]



# GAS PREDICTION ----------------------------------------------------------

# specification of general autoregresive scoring model
estimate_gas <- function(date_return, dist = 'sstd',
                         scale_type = 'Identity', insample_length = 1000,
                         alphas = c(0.001, 0.01, 0.025, 0.05, 0.95, 0.99, 0.999),
                         no_cores = 25) {

  # gas apec
  GASSpec = UniGASSpec(
    Dist = dist,
    ScalingType = scale_type,
    GASPar = list(location = TRUE, scale = TRUE, skewness = TRUE, shape = TRUE))

  # Perform 1-step ahead rolling forecast with refit
  cluster <- makeCluster(no_cores)
  Roll <- UniGASRoll(
    as.xts.data.table(date_return),
    GASSpec,
    Nstart = insample_length,
    RefitEvery = 5,
    RefitWindow = c("moving"),
    cluster = cluster)
  stopCluster(cluster)

  # statistics
  forecasts_gas <- getForecast(Roll)
  var_gas <- quantile(Roll, probs = alphas)  # VaR
  colnames(var_gas) <- paste0('var_gas_', as.character(alphas * 1000))
  es_gas <- GAS::ES(Roll, probs = alphas)
  colnames(es_gas) <- paste0('es_gas_', as.character(alphas * 1000))

  # merge to market_data
  data <- as.data.table(cbind(forecasts_gas, var_gas, es_gas))
  colnames(data) <- paste(dist, scale_type, insample_length, colnames(data), sep = '_')
  return(data)
}

# estimate over parameters
risk_estimate_gas <- function(date_return, params, ncores = 25) {
  output <- lapply(1:nrow(params), function(i) {
    estimate_gas(date_return, params[i, 1], params[i, 2], params[i, 3], ncores)
  })
  output <- do.call(cbind, output)
  return(output)
}

# params
distributions <- c('sstd')
scale_type <- c("Identity", "Inv")
insample_length <- c(500, 1000, 2000) # , 1000, 2000
params <- expand.grid(distributions, scale_type, insample_length, stringsAsFactors = FALSE)

# prepare data
DT <- as.data.table(stocks)
DT <- DT[, .(.id, index, close, returns)]
setnames(DT, colnames(DT), c('symbol', 'date', 'close', 'return'))
DT <- DT[DT[, .N, by = .(symbol)][N > 2500], on = "symbol"] # stock have at least 1 year of data
DT <- na.omit(DT)

# GAS risks
DT <- DT[symbol %in% c("MMM", "SPY")]
date_return <- DT[symbol == "SPY", .(date, return)]
gas_risks <- DT[, risk_estimate_gas(data.table(date, return), params, ncores = 20), by = .(symbol)]



# BACKTEST ----------------------------------------------------------------



# PLAYGROUND --------------------------------------------------------------

# merge all risk factors


# merge
symb <- 'ETSY'
stock <- stocks[.id == symb]
risk_left <- gpd_risks_left_tail[symbol == symb]
colnames(risk_left) <- paste0('left_', colnames(risk_left))
risk_right <- gpd_risks_right_tail[symbol == symb]
colnames(risk_right) <- paste0('right_', colnames(risk_right))
s <- cbind(stock, risk_left, risk_right)
s <- s[left_e_9999_2000_15 < 1 & left_e_9999_2000_15 > -1]

# plot
ggplot(s, aes(x = index)) +
  geom_line(aes(y = close)) +
  geom_line(aes(y = left_e_9999_1000_15 * 10), color = 'red') +
  geom_line(aes(y = right_e_9999_1000_15 * 10), color = 'blue')
ggplot(s[index %between% c('2015-01-01', '2021-01-01')], aes(x = index)) +
  geom_line(aes(y = close)) +
  geom_line(aes(y = left_e_9999_1000_15 * 100), color = 'red') +
  geom_line(aes(y = right_e_9999_1000_15 * 100), color = 'blue')
ggplot(s, aes(x = index)) +
  geom_line(aes(y = close)) +
  geom_line(aes(y = n_exceed_2000_15 * 1), color = 'red')


# portfolio investing




# SAVE DATA FOR ML MODEL --------------------------------------------------

# convert risk to data.table
risks_dt <- lapply(risks, as.data.table)
names(risks_dt) <- names(estimated)
risks_dt <- lapply(risks_dt, setnames, 'index', 'Datetime')
# risks_dt <- rbindlist(risks_dt, idcol = TRUE)
save(risks_dt, file = file.path('mldata', 'risks.rda'))
