library(PerformanceAnalytics)


add_features <- function(data) {

  # add ohlc transformations
  data$high_low <- data$high - data$low
  data$close_open <- data$close - data$open
  data$close_ath <- cummax(data$close)

  # returns
  data$returns <- PerformanceAnalytics::Return.calculate(data$close)
  data$returns_squared <- (data$returns)^2

  # rolling volatility
  data$std_5 <- roll::roll_sd(data$close, width = 5)
  data$std_10 <- roll::roll_sd(data$close, width = 10)
  data$std_20 <- roll::roll_sd(data$close, width = 20)
  data$std_40 <- roll::roll_sd(data$close, width = 40)
  data$std_80 <- roll::roll_sd(data$close, width = 80)
  data$std_100 <- roll::roll_sd(data$close, width = 100)
  data$std_100 <- roll::roll_sd(data$close, width = 200)

  return(data)
}
