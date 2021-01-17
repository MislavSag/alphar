# Spot volatility
# spot_vol <- spotVol(market_data$close, method = 'RM', lookBackPeriod = 3, on = 'minutes', k = 5, marketOpen = "09:30:00", marketClose = "16:00:00")
# market_data <- merge.xts(market_data_low, spot_vol$spot, all = c(TRUE, FALSE))[zoo::index(market_data_low)]


# spot_vol <- slider_parallel(
#   .x = as.data.table(market_data),
#   .f =   ~ {
#     library(data.table)
#     library(highfrequency)
#     cols <- c('index', 'close')
#     data_ <- .x[, ..cols]
#     colnames(data_) <- c('DT', 'PRICE')
#     vol <- tryCatch({
#         vol <- spotVol(data_, method = 'detPer', dailyvol = 'bipower',
#                        on = 'minutes', k = 60, marketOpen = '09:30:00',
#                        marketClose = '16:00:00', tz = 'America/New_York')
#         vol <- vol$spot
#         vol <- tail(vol, 1)
#       },
#       error = function(e) NA
#     )
#     return(vol)
#   },
#   .before = 5000,
#   .after = 0L,
#   .complete = TRUE,
#   n_cores = -1
# )
# vol_clean <- purrr::flatten(vol)
# vol_clean <- unlist(vol_clean)
# errors <- lapply(vol_clean, is.na)
# sum(unlist(errors))
# vol_clean <- purrr::compact(vol_clean)
#
# length(vol_clean)
# nrow(market_data)
#
# market_data_vol <- cbind.data.frame(market_data[(5000+1):nrow(market_data),], vol_clean)
# market_data_vol <- na.omit(market_data_vol)
# market_data_vol <- xts::xts(market_data_vol,
#                             order.by = as.POSIXct(rownames(market_data_vol), tz = 'America/New_York'))
# head(market_data_vol, 50)
# tail(market_data_vol, 20)
