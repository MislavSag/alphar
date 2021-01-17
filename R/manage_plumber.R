library(plumberDeploy)
library(analogsea)
library(plumber)
library(jsonlite)
library(dpseg)
library(xts)
library(IBrokers)
library(quantmod)
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
analogsea::install_github_r_package(217507640, "https://github.com/ottosven/backCUSUM")

# remove droplet if already exists
redeploy <- function() {
  plumberDeploy::do_remove_api(217507640, 'alphar', delete=TRUE)

  Sys.sleep(1L)

  # Deploy plumber
  plumberDeploy::do_deploy_api(
    droplet = 217507640,
    path = 'alphar',
    localPath = plumber_path,
    port = 8001
  )

}
redeploy()


# prepare data for test
market_data <- import_mysql(
  contract = 'SPY5',
  save_path = 'D:/market_data/usa/ohlcv',
  trading_days = TRUE,
  RMySQL::MySQL(),
  dbname = 'odvjet12_market_data_usa',
  username = 'odvjet12_mislav',
  password = 'Theanswer0207',
  host = '91.234.46.219'
)
market_data <- market_data[, 2:(ncol(market_data)-1)]
time <- zoo::index(market_data)
y <- as.vector(zoo::coredata(Cl(market_data)))
x <- as.numeric(zoo::index(market_data))

# test exuber
req_body <- list(x = y[1:600], adf_lag=2)
req_body <- jsonlite::toJSON(req_body, dataframe = 'rows')
response <- httr::POST(paste0("http://", '206.81.24.140', "/alphar/radf"),
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
