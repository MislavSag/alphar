
add_features <- function(data) {
  # add ohlc transformations
  data$high_low <- data$high - data$low
  data$close_open <- data$close - data$open
  data$close_ath <- cummax(data$close)

  return(data)
}
