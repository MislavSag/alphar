library(plumberDeploy)
library(analogsea)
library(plumber)
library(jsonlite)


# test localy
plumber_path = file.path(getwd(), 'deploy_folder')
root <- pr(file.path(plumber_path, "plumber.R"))
root
root %>% pr_run()


# doplets
analogsea::droplets()
drops <- analogsea::droplets()


analogsea::install_r_package(208189915, "exuber")
analogsea::install_r_package(208189915, "fracdiff")
analogsea::install_r_package(208189915, "xts")
analogsea::install_r_package(208189915, "dpseg")


# plumberDeploy::do_provision(208189915)
plumberDeploy::do_deploy_api(
  droplet = 208189915,
  path = 'plumber_test',
  localPath = plumber_path,
  port = 8001
)


plumberDeploy::do_remove_api(208189915, 'plumber_test', delete=TRUE)


analogsea::keys()


x <-  sample(1:100, 500, replace = TRUE)
x <- list(x = x, adf_lag=2)
x <- jsonlite::toJSON(x, dataframe = 'rows')

if (is.character(x)) {
  if (jsonlite::validate(x)) {
    print('json')
  }
}

response <- httr::POST(paste0("http://", '46.101.219.193', "/plumber_test/radf"),
                       body=x, encode="json")
httr::content(response, simplifyVector=TRUE)

# 46.101.219.193/plumber_test/plot
