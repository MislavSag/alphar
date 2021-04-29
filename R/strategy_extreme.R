library(data.table)
library(ggplot2)
library(parallel)
library(ptsuite)
library(PerformanceAnalytics)
source('C:/Users/Mislav/Documents/GitHub/alphar/R/import_data.R')



# import data
save_path <- "D:/risks"
stocks <- import_intraday('D:/market_data/equity/usa/hour/trades_adjusted', 'csv')
stocks[, returns := close / shift(close) - 1, by = symbol]
stocks <- na.omit(stocks)

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

roll_gpd <- function(returns, window = 500, threshold = 0.02) {
  # calculate shapes on rolling windows
  cl <- makeCluster(16)
  clusterExport(cl, c("estimate_gpd", "window", "threshold"), envir = environment())
  evt <- runner(
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
    k = window,
    na_pad = TRUE,
    type = 'auto',
    cl = cl
  )
  stopCluster(cl)
  risks <- rbindlist(evt, fill = TRUE)
  return(risks)
}


# estoimate indicators
DT_sample <- stocks[symbol %in% c("TSLA", "AAL", "SPY")]
cols <- colnames(roll_gpd(DT_sample[1:50, (returns)], window = 20))
ptsuite_risk <- DT_sample[, (cols) := roll_gpd(returns), by = symbol]

# backtest
backtest_data <- ptsuite_risk[symbol == "TSLA", .(datetime, returns, ptest_left, ptest_right)]
# backtest_data <- na.omit(backtest_data)
backtest_data[, c('ptest_left', 'ptest_right') := lapply(.SD, na.locf, na.rm = FALSE), .SDcols = c('ptest_left', 'ptest_right')]
side <- ifelse(shift(backtest_data$ptest_right - backtest_data$ptest_left) >= 0, 1, 0)
table(side)
backtest_data[, returns_strategy := side * returns]
charts.PerformanceSummary(as.xts.data.table(backtest_data[, .(datetime, returns, returns_strategy)]), plot.engine = "ggplot2")


library(httr)

APIKEY = "15cd5d0adf4bc6805a724b4417bbaafc"
base_url = "https://financialmodelingprep.com/api/v4"
symbol = "GOOG"
multiply = 1
time = 'hour'
from = "2014-03-20"
to = "2014-03-30"
q = paste(symbol, multiply, time, from, to, sep = "/")
url <- paste0(base_url, "/historical-price-adjusted/", q)
req <- GET(url, query = list(apikey = APIKEY))
data <- content(req)
data <- rbindlist(data$results)
plot(data$t, data$c, type = 'l')

# # plot
# data_plot <- cbind(DT_sample, risks)
#
# # plots.
# data_plot[, close := close / 1000]
# data_plot[, `:=`(hill_shape_left = hill_shape_left * -100,
#                  hill_shape_right  = hill_shape_right * -100)]
# cols_extract <- colnames(data_plot)[c(5, 17:35)]
# cols_extract <- colnames(data_plot)[c(5, grep("ptest", colnames(data_plot)))]
# data_plot <- melt(data_plot,
#                   id.vars = c('datetime'),
#                   measure.vars = cols_extract)
# ggplot(data_plot, aes(x = datetime, y = value, color = variable)) +
#   geom_line()
# ggplot(data_plot[datetime %between% c('2020-01-01', '2021-03-01')], aes(x = datetime, y = value, color = variable)) +
#   geom_line()
# ggplot(data_plot[datetime %between% c('2015-01-01', '2017-01-01')], aes(x = datetime, y = value, color = variable)) +
#   geom_line()
#
# # backtest
# DT_sample <- DT[symbol %in% c("NVDA", "GOOG")]
