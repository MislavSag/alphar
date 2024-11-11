# Load necessary libraries
library(POT)
library(quantmod)
library(dplyr)
library(ggplot2)
library(PerformanceAnalytics)

# 1. Data Collection and Preparation
# -----------------------------------

# Define the stock symbol and time period
stock_symbol <- "AAPL"
start_date <- as.Date("2000-01-01")  # Starting from January 1, 2000
end_date <- Sys.Date()               # Up to the current date

# Fetch historical stock data using quantmod
getSymbols(stock_symbol, src = "yahoo", from = start_date, to = end_date, auto.assign = TRUE)

# Extract the adjusted closing prices
stock_data <- Cl(get(stock_symbol))

# Plot the adjusted closing prices
chartSeries(stock_data, name = "AAPL Adjusted Closing Prices", theme = chartTheme("white"))

# 2. Calculating Daily Log Returns
# ---------------------------------

# Calculate daily log returns
log_returns <- dailyReturn(stock_data, type = 'log')

# Convert to a data frame for easier manipulation
returns_df <- data.frame(Date = index(log_returns), Return = coredata(log_returns))

# View the first few rows
head(returns_df)

# ########## MY CHANGE
colnames(returns_df)[2] <- "Return"

# 3. Splitting Data into In-Sample and Out-of-Sample Sets
# --------------------------------------------------------

# Define the proportion of data to be used for training (in-sample)
train_proportion <- 0.8  # 80% for training, 20% for testing

# Determine the cutoff index and date
cutoff_index <- floor(nrow(returns_df) * train_proportion)
cutoff_date <- returns_df$Date[cutoff_index]

print(paste("Cutoff Date for Out-of-Sample Analysis:", cutoff_date))

# Split the data
in_sample_df <- returns_df[1:cutoff_index, ]
out_sample_df <- returns_df[(cutoff_index + 1):nrow(returns_df), ]

# Plot to visualize the split
ggplot() +
  geom_line(data = returns_df, aes(x = Date, y = Return), color = "grey") +
  geom_vline(xintercept = as.numeric(cutoff_date), color = "blue", linetype = "dashed") +
  labs(title = "In-Sample and Out-of-Sample Data Split",
       x = "Date",
       y = "Daily Log Return") +
  theme_minimal()

# 4. In-Sample Analysis
# ----------------------

# 4.1. Threshold Selection
# -------------------------

# Define the high threshold (e.g., 95th percentile of positive returns)
threshold_quantile <- 0.95
threshold <- quantile(in_sample_df$Return[in_sample_df$Return > 0], threshold_quantile)

# Print the threshold value
print(paste("In-Sample Threshold (95th percentile of positive returns):", round(threshold, 4)))

# Visualize the threshold on a histogram
ggplot(in_sample_df, aes(x = Return)) +
  geom_histogram(binwidth = 0.005, fill = "lightblue", color = "black") +
  geom_vline(xintercept = threshold, color = "red", linetype = "dashed", size = 1) +
  labs(title = "In-Sample Histogram of Daily Log Returns with Threshold",
       x = "Daily Log Return",
       y = "Frequency") +
  theme_minimal()

# 4.2. Identifying Exceedances
# ----------------------------

# Extract exceedances above the threshold
exceedances_in <- in_sample_df$Return[in_sample_df$Return > threshold]

# Check the number of exceedances
print(paste("Number of Exceedances in In-Sample Data:", length(exceedances_in)))

# Plot exceedances
ggplot(data.frame(Exceedance = exceedances_in), aes(x = Exceedance)) +
  geom_histogram(binwidth = 0.005, fill = "orange", color = "black") +
  labs(title = "In-Sample Exceedances Over Threshold",
       x = "Exceedance",
       y = "Frequency") +
  theme_minimal()

# 4.3. Fitting the Markov Chain GPD Model
# ----------------------------------------

# Fit the Markov Chain GPD model to the exceedances
# The fitmcgpd function requires the exceedances and the threshold
# We assume a first-order Markov chain (order = 1)
fgpd_in <- fitmcgpd(data = exceedances_in, threshold = threshold)

# View the fitted model summary
summary(fgpd_in)

# 4.4. Estimating the Extremal Index
# -----------------------------------

# Estimate the Extremal Index using the dexi function
# dexi expects an object of class 'mcpot', which is what fitmcgpd returns
extremal_index_in <- dexi(fgpd_in, n.sim = 1000)

# View the estimated Extremal Index
print(paste("In-Sample Estimated Extremal Index:", round(extremal_index_in$exi, 3)))

# 4.5. Generating Buy Signals Based on Exceedances
# --------------------------------------------------

# Initialize a Signal column in in-sample data
in_sample_df$Signal <- 0

# Assign buy signals where exceedances occur
in_sample_df$Signal[in_sample_df$Return > threshold] <- 1

# View the first few signals
head(in_sample_df)

# Visualize buy signals on the return plot
ggplot(in_sample_df, aes(x = Date, y = Return)) +
  geom_line(color = "grey") +
  geom_point(data = subset(in_sample_df, Signal == 1), aes(x = Date, y = Return), color = "red", size = 2) +
  geom_vline(xintercept = as.numeric(cutoff_date), color = "blue", linetype = "dashed") +
  geom_hline(yintercept = threshold, color = "green", linetype = "dashed") +
  labs(title = "In-Sample Daily Log Returns with Buy Signals",
       x = "Date",
       y = "Daily Log Return") +
  theme_minimal()

# 5. Out-of-Sample Analysis
# --------------------------

# 5.1. Generating Buy Signals for Out-of-Sample Data
# ---------------------------------------------------

# Initialize a Signal column in out-of-sample data
out_sample_df$Signal <- 0

# Assign buy signals based on in-sample threshold
out_sample_df$Signal[out_sample_df$Return > threshold] <- 1

# View the first few signals
head(out_sample_df)

# Visualize buy signals on the out-of-sample return plot
ggplot(out_sample_df, aes(x = Date, y = Return)) +
  geom_line(color = "grey") +
  geom_point(data = subset(out_sample_df, Signal == 1), aes(x = Date, y = Return), color = "red", size = 2) +
  geom_hline(yintercept = threshold, color = "green", linetype = "dashed") +
  labs(title = "Out-of-Sample Daily Log Returns with Buy Signals",
       x = "Date",
       y = "Daily Log Return") +
  theme_minimal()

# 5.2. Backtesting the Strategy on Out-of-Sample Data
# -----------------------------------------------------

# Initialize portfolio parameters for out-of-sample
initial_capital_out <- 100000  # Starting with $100,000
portfolio_out <- data.frame(Date = out_sample_df$Date, PortfolioValue = initial_capital_out)
portfolio_out$PortfolioValue[1] <- initial_capital_out

# Define investment parameters (same as in-sample)
position_size_out <- 0.1        # Invest 10% of portfolio per trade
commission_out <- 5              # Fixed commission per trade
slippage_out <- 0.0005           # 0.05% slippage per trade
stop_loss_threshold_out <- 0.95  # 5% stop-loss

# Initialize variables
position_out <- 0
purchase_price_out <- 0

# Loop through the out-of-sample returns and simulate trades
for (i in 2:nrow(out_sample_df)) {
  # Carry forward the previous portfolio value
  portfolio_out$PortfolioValue[i] <- portfolio_out$PortfolioValue[i-1]

  # If there was a buy signal the previous day and no current position
  if (out_sample_df$Signal[i-1] == 1 && position_out == 0) {
    # Determine the amount to invest
    investment <- position_size_out * portfolio_out$PortfolioValue[i]

    # Deduct the investment and transaction costs
    total_cost <- investment + commission_out + (investment * slippage_out)
    portfolio_out$PortfolioValue[i] <- portfolio_out$PortfolioValue[i] - total_cost

    # Buy at the current day's closing price
    # Ensure that the index does not exceed the bounds
    if ((i + cutoff_index) <= length(stock_data)) {
      purchase_price_out <- as.numeric(stock_data[i + cutoff_index])
    } else {
      purchase_price_out <- as.numeric(tail(stock_data, 1))
    }

    # Record the position
    position_out <- investment / purchase_price_out
  }

  # If holding a position, check for stop-loss or sell signal
  if (position_out > 0) {
    current_price_out <- as.numeric(stock_data[i + cutoff_index])

    # Check for stop-loss condition
    if (current_price_out <= (stop_loss_threshold_out * purchase_price_out)) {
      # Sell at current price with slippage and commission
      proceeds <- position_out * current_price_out * (1 - slippage_out) - commission_out
      portfolio_out$PortfolioValue[i] <- portfolio_out$PortfolioValue[i] + proceeds

      # Reset position
      position_out <- 0
    }
    # Else, sell the position at the next day's closing price (carry-over)
    else if (i < nrow(out_sample_df)) {
      # Sell at the next day's closing price
      sell_price_out <- as.numeric(stock_data[i + cutoff_index + 1])
      proceeds <- position_out * sell_price_out * (1 - slippage_out) - commission_out
      portfolio_out$PortfolioValue[i + 1] <- portfolio_out$PortfolioValue[i + 1] + proceeds

      # Reset position
      position_out <- 0
    }
  }
}

# Calculate daily portfolio returns
portfolio_out$StrategyReturn <- c(NA, diff(portfolio_out$PortfolioValue) / portfolio_out$PortfolioValue[-nrow(portfolio_out)])

# Calculate cumulative returns
portfolio_out$CumulativeReturn <- cumprod(1 + portfolio_out$StrategyReturn, na.rm = TRUE)
portfolio_out$CumulativeReturn[is.na(portfolio_out$CumulativeReturn)] <- 1

# Plot the portfolio growth for out-of-sample
ggplot(portfolio_out, aes(x = Date, y = CumulativeReturn)) +
  geom_line(color = "purple") +
  labs(title = "Out-of-Sample Portfolio Growth from Trading Strategy",
       x = "Date",
       y = "Cumulative Return") +
  theme_minimal()

# 6. Performance Evaluation
# ----------------------------

# Combine in-sample and out-of-sample portfolio data for comparison
combined_portfolio <- rbind(
  data.frame(Date = in_sample_df$Date, CumulativeReturn = portfolio_in$CumulativeReturn),
  data.frame(Date = out_sample_df$Date, CumulativeReturn = portfolio_out$CumulativeReturn)
)

# Calculate Buy and Hold cumulative returns for the entire period
returns_df$BuyHoldCumulative <- cumprod(1 + returns_df$Return)

# Split BuyHold into in-sample and out-of-sample
buyhold_in <- returns_df$BuyHoldCumulative[1:cutoff_index]
buyhold_out <- returns_df$BuyHoldCumulative[(cutoff_index + 1):nrow(returns_df)]

# Combine BuyHold for comparison
combined_buyhold <- rbind(
  data.frame(Date = in_sample_df$Date, BuyHoldCumulative = buyhold_in),
  data.frame(Date = out_sample_df$Date, BuyHoldCumulative = buyhold_out)
)

# Merge strategy and BuyHold for visualization
comparison <- merge(
  data.frame(Date = combined_portfolio$Date, Strategy = combined_portfolio$CumulativeReturn),
  data.frame(Date = combined_buyhold$Date, BuyHold = combined_buyhold$BuyHoldCumulative),
  by = "Date"
)

# Plot both strategies (In-Sample and Out-of-Sample)
ggplot(comparison, aes(x = Date)) +
  geom_line(aes(y = Strategy, color = "Extreme Cluster Strategy")) +
  geom_line(aes(y = BuyHold, color = "Buy and Hold")) +
  labs(title = "Trading Strategy vs. Buy and Hold Performance (2000-Present)",
       x = "Date",
       y = "Cumulative Return",
       color = "") +
  theme_minimal()

# Performance Metrics Calculation

# Convert Strategy and BuyHold to xts objects
strategy_xts <- xts(comparison$Strategy, order.by = comparison$Date)
buyhold_xts <- xts(comparison$BuyHold, order.by = comparison$Date)

# Calculate daily returns for PerformanceAnalytics
strategy_returns <- dailyReturn(strategy_xts)
buyhold_returns <- dailyReturn(buyhold_xts)

# Combine the returns
combined_returns <- merge(strategy_returns, buyhold_returns, join = "inner")
colnames(combined_returns) <- c("Strategy", "BuyHold")

# Remove any NA values
combined_returns <- na.omit(combined_returns)

# Plot Performance Summary
charts.PerformanceSummary(combined_returns, main = "Strategy vs. Buy and Hold Performance (2000-Present)")

# Calculate performance statistics
performance_stats <- rbind(
  SharpeRatio.annualized(combined_returns, Rf = 0),
  Return.annualized(combined_returns),
  maxDrawdown(combined_returns)
)
rownames(performance_stats) <- c("Sharpe Ratio", "Annualized Return", "Max Drawdown")
print("Performance Statistics:")
print(performance_stats)
