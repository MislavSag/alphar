library(ptsuite)
library(runner)
library(parallel)
library(data.table)
library(xts)
library(quantmod)
library(ggplot2)
library(PerformanceAnalytics)
library(runner)
library(janitor)
source('C:/Users/Mislav/Documents/GitHub/alphar/R/parallel_functions.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/outliers.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/import_data.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/features.R')

# performance
plan(multiprocess(workers = availableCores() - 16))  # multiprocess(workers = availableCores() - 8)



# UTILS -------------------------------------------------------------------

# preprocessing function
preprocessing <- function(data, outliers = 'above_daily', add_features = TRUE) {

  # change colnames for spy
  col_change <- grepl('SPY_', colnames(data))
  colnames(data)[col_change] <- gsub('SPY_', '', colnames(data)[col_change])

  # remove outliers
  if (outliers == 'above_daily') {
    data <- remove_outlier_median(data, median_scaler = 25)
  }

  # add features
  if (isTRUE(add_features)) {
    data <- add_features(data)
  }

  data <- na.omit(data)

  return(data)
}



# IMPORT DATA -------------------------------------------------------------

# import intraday market data
market_data <- import_mysql(
  symbols = c('SPY'),
  upsample = 30,
  trading_hours = TRUE,
  use_cache = TRUE,
  combine_data = FALSE,
  save_path = 'D:/market_data/usa/ohlcv',
  RMySQL::MySQL(),
  dbname = 'odvjet12_market_data_usa',
  username = 'odvjet12_mislav',
  password = 'Theanswer0207',
  host = '91.234.46.219'
)

# clean data
market_data <- lapply(market_data, preprocessing, add_features = TRUE)
sample <- as.data.table(market_data[[1]])[, .(index, returns, close)]


# PTSUIT ------------------------------------------------------------------

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

# calculate shapes on rolling windows
cl <- makeCluster(8)
clusterExport(cl, c("pareto_test", "estimate_gpd", "alpha_hills"), envir = environment())
evt <- runner(
  x = sample$returns,
  f = function(x) {
    library(data.table)
    library(ptsuite)
    library(janitor)

    # pareto left tail
    x_left <- x[x < -0.001] * -1
    risks_left <- estimate_gpd(x_left, hill_threshold = 0.001, suffix = '_left')

    # pareto test right tail
    x_right <- x[x > 0.001]
    risks_right <- estimate_gpd(x_right, hill_threshold = 0.001)

        # estimate shape parameters
    pareto_tests <- cbind(risks_left, risks_right)
    return(pareto_tests)
  },
  k = 1000,
  na_pad = TRUE,
  type = 'auto',
  cl = cl
)
stopCluster(cl)

# merge together
risks <- rbindlist(evt, fill = TRUE)

# plot
data_plot <- cbind(sample, risks)
data_plot[, close := close / 100]
data_plot[, `:=`(hill_shape_left = hill_shape_left * 1000,
                 hill_shape_right  = hill_shape_right * 1000)]
data_plot <- melt(data_plot,
                  id.vars = c('index'),
                  measure.vars = colnames(data_plot[, 3:ncol(data_plot)]))
ggplot(data_plot, aes(x = index, y = value, color = variable)) +
  geom_line()
ggplot(data_plot[index %between% c('2019-01-01', '2021-01-01')], aes(x = index, y = value, color = variable)) +
  geom_line()


# test functions
d <- generate_pareto(100000, 1.2, 3)
test <- as.data.table(generate_all_estimates(d))
shapes <- data.table::dcast(test[, 1:2], . ~ `Method.of.Estimation`, value.var = 'Shape.Parameter')
shapes <- shapes[, 2:ncol(shapes)]
colnames(shapes) <- paste0('scales_', colnames(clean_names(shapes)))
scales <- data.table::dcast(test[, c(1, 3)], . ~ `Method.of.Estimation`, value.var = 'Scale.Parameter')
scales <- scales[, 2:ncol(scales)]
colnames(scales) <- paste0('scales_', colnames(clean_names(scales)))

