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
market_data <- xts::xts(market_data[, 2:(ncol(market_data)-1)], order.by = market_data$date)
tzone(market_data) <- 'America/New_York'
time <- zoo::index(market_data)
y <- as.vector(zoo::coredata(Cl(market_data)))
x <- as.numeric(zoo::index(market_data))

# test exuber
req_body <- list(x = y[1:600], adf_lag=2)
req_body <- jsonlite::toJSON(req_body, dataframe = 'rows')
response <- httr::POST(paste0("http://", '206.81.24.140', "/alphar/radf"),
                       body=req_body, encode="json")
httr::content(response, simplifyVector=TRUE)

# test dpseg
req_body <- list(time = x[1:600], price = y[1:600], type_ = 'var')
req_body <- jsonlite::toJSON(req_body, dataframe = 'rows')
response <- httr::POST(paste0("http://", '206.81.24.140', "/alphar/dpseg"),
                       body=req_body, encode="json")
httr::content(response, simplifyVector=TRUE)

# test backcusum
req_body <- list(x = y[1:100], width = 2)
req_body <- jsonlite::toJSON(req_body, dataframe = 'rows')
response <- httr::POST(paste0("http://", '206.81.24.140', "/alphar/backcusum"),
                       body=req_body, encode="json")
httr::content(response, simplifyVector=TRUE)


# test backcusumvol

# market_data <- import_mysql(
#   contract = 'SPY5',
#   save_path = 'D:/market_data/usa/ohlcv',
#   RMySQL::MySQL(),
#   dbname = 'odvjet12_market_data_usa',
#   username = 'odvjet12_mislav',
#   password = 'Theanswer0207',
#   host = '91.234.46.219'
# )
# market_data <- xts::xts(market_data[, 2:(ncol(market_data)-1)], order.by = market_data$date)
# tzone(market_data) <- 'America/New_York'
# market_data <- market_data['2020-01-03 14:25:00/2020-05-12 13:30:00']
# # market_data <- market_data[paste0((min(as.Date(zoo::index(market_data))) + 1), '/', max(as.Date(zoo::index(market_data))))]
# # market_data <- market_data['2020-05-01/2020-05-12']
#
# x <- highfrequency::spotVol(Cl(market_data))
#
#
# time <- zoo::index(market_data)
# y <- as.vector(zoo::coredata(Cl(market_data)))
# x <- as.numeric(zoo::index(market_data))
# req_body <- list(time = as.character(time), price = y)
#
# req_body <- list(time = as.character(time[1:15000]), price = y[1:15000])
# req_body <- jsonlite::toJSON(req_body, dataframe = 'rows')
# response <- httr::POST(paste0("http://", '206.81.24.140', "/alphar/backcusumvol"),
#                        body=req_body, encode="json")
# httr::content(response)
#
#
# min(as.Date(zoo::index(market_data))) + 1



