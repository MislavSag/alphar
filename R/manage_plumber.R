library(plumberDeploy)
library(analogsea)
library(plumber)
library(jsonlite)
library(dpseg)
library(xts)
library(IBrokers)


# test localy
plumber_path = file.path(getwd(), 'R', 'plumber_deploy')
root <- pr(file.path(plumber_path, "plumber.R"))
root %>% pr_run()


# doplets
drops <- analogsea::droplets()

# install required packages
analogsea::install_r_package(208189915, "exuber")
analogsea::install_r_package(208189915, "fracdiff")
analogsea::install_r_package(208189915, "xts")
analogsea::install_r_package(208189915, "dpseg")

# remove droplet if already exists
plumberDeploy::do_remove_api(208189915, 'alphar', delete=TRUE)

# Deploy plumber
plumberDeploy::do_deploy_api(
  droplet = 208189915,
  path = 'alphar',
  localPath = plumber_path,
  port = 8001
)


# prepare data for test
tws <- twsConnect(clientId = 2, host = '127.0.0.1', port = 7496)
df <- reqHistoricalData(tws, twsEquity('SPY', exch = 'SMART', primary = 'ISLAND'), barSize = '1 day', duration = '5 Y')
twsDisconnect(tws)
y <- as.vector(zoo::coredata(df$SPY.Close))[1:400]
x <- as.numeric(zoo::index(df))[1:400]

# test exuber
req_body <- list(x = y, adf_lag=2)
req_body <- jsonlite::toJSON(req_body, dataframe = 'rows')
response <- httr::POST(paste0("http://", '46.101.219.193', "/alphar/radf"),
                       body=req_body, encode="json")
httr::content(response, simplifyVector=TRUE)

# test dpseg
p <- estimateP(x=x, y=y, plot=FALSE)
req_body <- list(time = x, price = y, type_ = 'var')
req_body <- jsonlite::toJSON(req_body, dataframe = 'rows')
response <- httr::POST(paste0("http://", '46.101.219.193', "/alphar/dpseg"),
                       body=req_body, encode="json")
httr::content(response, simplifyVector=TRUE)
