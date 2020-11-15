library(RMySQL)
library(DBI)
library(fst)


# IMPORT DATA FROM MYSQL --------------------------------------------------

import_mysql <- function(contract, save_path=getwd(), ...) {
  # read from saved file if exist
  file_path <- file.path(save_path, paste0(contract, '.fts'))
  if (file.exists(file_path)) {
    market_data <- read.fst(file_path)
    old <- difftime(Sys.time(), tail(market_data$date, 1))
    print(paste0('Read from local file. The data is ', round(old, 2), ' days old.'))
    return(market_data)
  }

  # make connection
  con <- DBI::dbConnect(...)

  # query table
  market_data <- DBI::dbGetQuery(con, paste0('SELECT * FROM ', contract, ';'))

  # close connection
  dbDisconnect(con)

  # clean table
  market_data$date <- as.POSIXct(market_data$date)
  market_data <- market_data[base::order(market_data$date, decreasing  = FALSE), ]
  market_data$id <- NULL

  # save data to path
  write.fst(market_data, file_path)

  return(market_data)
}




