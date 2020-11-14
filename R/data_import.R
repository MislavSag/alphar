
#
#
#
# url <- '/portal/sso/validate'
#
# reset_config()
# httr::set_config(config(ssl_verifypeer = 0L, ssl_verifyhost = 0L))
#
# get_request <- function(url, ...) {
#   base_url <- 'https://localhost:5000/v1'
#   url <- paste0(base_url, url)
#   res <- httr::GET(url, ...)
#   return(res)
# }
#
# post_request <- function(url, ...) {
#   base_url <- 'https://localhost:5000/v1'
#   url <- paste0(base_url, url)
#   res <- httr::POST(url, ...)
#   return(res)
# }
#
# res <- get_request('/portal/sso/validate')
# httr::content(res)
#
# res <- get_request('/portal/iserver/auth/status')
# httr::content(res)
#
#
# step_ <- 1
# while (step_ < 1000) {
#   Sys.sleep(30)
#   res <- get_request('/portal/sso/validate')
#   httr::content(res)
# }
#
#
# # Market Data History
# res <- get_request('/portal/iserver/marketdata/history', query = list(conid = "756733", period = "2d", bar = '1h', outsideRth = FALSE))
# res <- httr::content(res, as = 'text')
# res <- jsonlite::fromJSON(res)
# data <- res$data
# data$time <- as.POSIXct(res$startTime, format = '%Y%m%d-%H:%M:%OS')
# data$t[1] / 86400000
# head(data, 10)
#
#
#
# # Stocks
# res <- get_request('/portal/trsrv/stocks', query = list(symbols = "SPY"))
# httr::content(res, as = 'text')
#
#
# # Get a list of subscriptions
# res <- get_request('/portal/fyi/settings')
# httr::content(res)
#
#
#
# # Contract Details
# res <- get_request('/portal/iserver/contract/273982664/info')
# httr::content(res, as = 'text')

