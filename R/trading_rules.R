# backtet functoin
trade_threshold <- function(indicator, returns, threshold, relation = `>`) {

  # check arguments
  checkmate::assert_number(threshold)
  checkmate::assert_function(relation)
  if (xts::is.xts(indicator)) {
    datetime <- zoo::index(indicator)
    indicator <- as.vector(zoo::coredata(indicator))
  }
  if (xts::is.xts(returns)) {
    returns <- as.vector(zoo::coredata(returns))
  }
  checkmate::assert_choice(length(indicator) == length(returns), TRUE)

  # trading rule
  side <- vector(mode = 'integer', length = length(indicator))
  for (i in seq_along(indicator)) {
    if (i == 1 || is.na(indicator[i-1])) {
      side[i] <- NA
    } else if (relation(indicator[i-1], threshold)) {
      side[i] <- 0
    } else {
      side[i] <- 1
    }
  }
  # merge
  returns_strategy <- returns * side
  returns_strategy <- xts::xts(cbind(returns, returns_strategy), order.by = datetime)
  return(returns_strategy)
}


