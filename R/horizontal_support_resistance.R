library(data.table)
library(arrow)
library(lubridate)
library(TTR)
library(pracma)


# PARAMETERS -------------------------------------------------------------
# Parameters we use in analysis below
symbols = c("aapl", "abbv", "amzn", "cost", "spy", "dow", "duk", "acn")
zz_pct_change = 5 # 2, 3 sd
e = 0.01 # mean return

# import Databento minute data
prices = read_parquet("F:/databento/minute.parquet")

# change timezone
attr(prices$ts_event, "tz")
prices[, ts_event := with_tz(ts_event, tz = "America/New_York")]
attr(prices$ts_event, "tz")

#
sample_ = prices[symbol == "AAPL"]


# Calculate daily price range
price <- sample_[, close]

# Function to find support and resistance levels
find_levels <- function(price) {
  levels <- c()
  for(i in 2:(length(price)-1)) {
    if(price[i] > price[i-1] && price[i] > price[i+1]) {
      levels <- c(levels, price[i])
    } else if(price[i] < price[i-1] && price[i] < price[i+1]) {
      levels <- c(levels, price[i])
    }
  }
  return(levels)
}

levels <- find_levels(price)

# Simplify levels by rounding (you can adjust the precision)
rounded_levels <- round(levels, -1) # Rounds to nearest 10

# Find unique levels for clarity
unique_levels <- unique(rounded_levels)

# Plotting
plot(price, main = "AAPL with Horizontal Support and Resistance Levels")
abline(h = unique_levels, col = "red", lty = 2)
