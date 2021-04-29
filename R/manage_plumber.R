library(plumberDeploy)
library(analogsea)
library(plumber)
library(jsonlite)
library(dpseg)
library(xts)
library(data.table)
library(quantmod)
library(PerformanceAnalytics)
source('R/import_data.R')


# Plumber path
plumber_path = file.path(getwd(), 'R', 'plumber_deploy')
account <- analogsea::account()
account$account$status == 'active'

# Test localy
# root <- pr(file.path(plumber_path, "plumber.R"))
# root %>% pr_run(port=8000)

# doplets
drops <- analogsea::droplets()
print(drops)

# install R
# plumberDeploy::do_provision(217507640)

# install required packages
analogsea::install_r_package(217507640, "data.table")
analogsea::install_r_package(217507640, "exuber")
analogsea::install_r_package(217507640, "fracdiff")
analogsea::install_r_package(217507640, "xts")
analogsea::install_r_package(217507640, "dpseg")
analogsea::install_r_package(217507640, "highfrequency")
analogsea::install_r_package(217507640, "anytime")
analogsea::install_r_package(217507640, "purrr")
analogsea::install_r_package(217507640, "roll")
analogsea::install_r_package(217507640, "PerformanceAnalytics")
analogsea::install_r_package(217507640, "GAS")
analogsea::install_r_package(217507640, "evir")
analogsea::install_r_package(217507640, "mlr3")
analogsea::install_r_package(217507640, "mlr3verse")
analogsea::install_r_package(217507640, "ranger")
analogsea::install_r_package(217507640, "ranger")
analogsea::install_github_r_package(217507640, "https://github.com/ottosven/backCUSUM")

# upload files
droplet_ssh(240714478, "pwd")
droplet_upload(
  droplet = 240714478,
  local = 'C:/Users/Mislav/Documents/GitHub/alphar/mlmodels/classif_ranger_tuned_66097a6ccf34ed25.rds',
  remote = '/var/plumber/ml_model_risks.rds',# '/root/var/plumber/alphar/ml_model_risks.rds',
  verbose = TRUE
)

# remove droplet if already exists
redeploy <- function() {
  plumberDeploy::do_remove_api(240714478, 'alphar', delete=TRUE)

  Sys.sleep(1L)

  # Deploy plumber
  plumberDeploy::do_deploy_api(
    droplet = 240714478,
    path = 'alphar',
    localPath = plumber_path,
    port = 8001
  )

}
redeploy()


# prepare data for test
market_data <- import_mysql(
  symbols = c('SPY'),
  upsample = 5,
  trading_hours = TRUE,
  use_cache = TRUE,
  combine_data = FALSE,
  save_path = 'D:/market_data/usa/ohlcv',
  RMySQL::MySQL(),
  dbname = 'odvjet12_market_data_usa',
  username = 'odvjet12_mislav',
  password = 'Theanswer0207',
  host = '91.234.46.219'
)[[1]]
time <- zoo::index(market_data)
y <- as.vector(zoo::coredata(Cl(market_data)))
x <- as.numeric(zoo::index(market_data))

# test exuber
# 207.154.227.4
req_body <- list(x = y[1:600], adf_lag=2)
req_body <- jsonlite::toJSON(req_body, dataframe = 'rows')
response <- httr::POST(paste0("http://", '207.154.227.4', "/alphar/radf"),
                       body=req_body, encode="json")
httr::content(response, simplifyVector=TRUE)

# dpseg
req_body <- list(time = x[1:600], price = y[1:600], type_ = 'var', p = 0.1)
req_body <- jsonlite::toJSON(req_body, dataframe = 'rows')
response <- httr::POST(paste0("http://", '206.81.24.140', "/alphar/dpseg"),
                       body=req_body, encode="json")
httr::content(response, simplifyVector=TRUE)

# Var
req_body <- list(x = y[1:600], ptob = 0.99, type = 'modified')
req_body <- jsonlite::toJSON(req_body, dataframe = 'rows')
response <- httr::POST(url = paste0("http://", '206.81.24.140', "/alphar/varrisk"),
                       body=req_body, encode="json")
httr::content(response, simplifyVector=TRUE)

# test backcusum
req_body <- list(x = y[1:100], width = 2)
req_body <- jsonlite::toJSON(req_body, dataframe = 'rows')
response <- httr::POST(paste0("http://", '206.81.24.140', "/alphar/backcusum"),
                       body=req_body, encode="json")
httr::content(response, simplifyVector=TRUE)

# GAS
req_body <- list(x = diff(log(y[1:600])), dist = 'std', scaling_type = 'Identity', h = 1, p = 0.01)
req_body <- jsonlite::toJSON(req_body, dataframe = 'rows')
response <- httr::POST(paste0("http://", '206.81.24.140', "/alphar/gas"),
                       body=req_body, encode="json")
httr::content(response, simplifyVector=TRUE)

# evir
req_body <- list(x = diff(log(y[1:600])), threshold = -0.001, method = 'pwm', p = 0.999)
req_body <- jsonlite::toJSON(req_body, dataframe = 'rows')
response <- httr::POST(paste0("http://", '206.81.24.140', "/alphar/gpd"),
                       body=req_body, encode="json")
httr::content(response, simplifyVector=TRUE)

# ml model risks
req_body <- list(features = as.vector(abs(t(rnorm(n = length(feature_names))))))
req_body <- jsonlite::toJSON(req_body, dataframe = 'rows')
response <- httr::POST(paste0("http://", '206.81.24.140', "/alphar/ml_model_risks"),
                       body=req_body, encode="json")
httr::content(response, simplifyVector=TRUE)



# TEST ML MODELS ----------------------------------------------------------

# 1) test the prediction fuction is working
models <- list.files(file.path('R/plumber_deploy'))
model <- readRDS(file.path('R/plumber_deploy', models[1]))
model$model$learner
feature_names <-model$model$learner$state$train_task$feature_names
print(feature_names)
test_data_table <- data.table(abs(t(rnorm(n = length(feature_names)))))
colnames(test_data_table) <- model$model$learner$state$train_task$feature_names
predictions <- model$model$learner$predict_newdata(newdata = test_data_table)
predictions$prob

# 2) calulate features from prices
# cl_ath <- cummax(y)
# cl_ath_dev <- (cl_ath - y) / cl_ath
# gpd_es_10000_12_9990
# gpr_feature <- function(y, threshold, p) {
#   req_body <- list(x = diff(log(y)), threshold, method = 'pwm', p)
#   req_body <- jsonlite::toJSON(req_body, dataframe = 'rows')
#   response <- httr::POST(paste0("http://", '206.81.24.140', "/alphar/gpd"),
#                          body=req_body, encode="json")
#   return(httr::content(response, simplifyVector=TRUE))
# }
# gpd_es_10000_12_9990 <- gpr_feature(y[(length(y)-10000):length(y)], 12, 0.999)
# gpd_es_10000_8_9990 <- gpr_feature(y[(length(y)-10000):length(y)], 8, 0.999)
# gpd_es_2500_12_9990 <- gpr_feature(y[(length(y)-2500):length(y)], 12, 0.999)
# gpd_es_5000_8_9990 <- gpr_feature(y[(length(y)-5000):length(y)], 8, 0.999)
# gpd_es_5000_8_9999 <- gpr_feature(y[(length(y)-5000):length(y)], 8, 0.9999)
# skewness_300 <- PerformanceAnalytics::skewness(tail(y, 300))
# std_300 <- StdDev(tail(y, 300))
