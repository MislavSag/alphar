library(quantmod)
library(PerformanceAnalytics)
library(TTR)
library(moments)
library(roll)

# Get Data
getSymbols(c("SPY", "^VIX", "^VVIX"), from = "2010-01-01")
spy_returns <- dailyReturn(Cl(SPY))

# Indicators Calculation
VaR_21 <- rollapply(spy_returns, 21, function(x) VaR(x, p=0.95, method="historical"), fill=NA, align="right")
vix <- Cl(VIX[index(spy_returns)])
realized_vol <- runSD(spy_returns, 21)
drawdowns <- Drawdowns(spy_returns)
skewness_21 <- rollapply(spy_returns, 21, skewness, fill=NA, align="right")
vvix <- Cl(VVIX[index(spy_returns)])
kurtosis_21 <- rollapply(spy_returns, 21, kurtosis, fill=NA, align="right")
es_21 <- rollapply(spy_returns, 21, function(x) ES(x, p=0.95, method="historical"), fill=NA, align="right")
max_drawdown_252 <- rollapply(spy_returns, 252, maxDrawdown, fill=NA, align="right")
vol_of_vol <- runSD(vix, 21)

# Merge indicators
indicators <- na.omit(merge(spy_returns, VaR_21, vix, realized_vol, drawdowns, skewness_21,
                            vvix, kurtosis_21, es_21, max_drawdown_252, vol_of_vol))
colnames(indicators) <- c("SPY_Returns", "VaR", "VIX", "RealizedVol", "Drawdown", "Skewness",
                          "VVIX", "Kurtosis", "ES", "MaxDrawdown252", "VolOfVol")

# Rolling PCA calculation (first principal component)
window <- 252
pca_scores <- rep(NA, nrow(indicators))

for (i in (window + 1):nrow(indicators)) {
  past_data <- indicators[(i - window):(i - 1), -1]
  pca <- prcomp(past_data, scale. = TRUE)
  pca_scores[i] <- predict(pca, indicators[i, -1])[1]
}

# Define discrete exposure based on PCA scores quantiles
pca_exposure <- rep(1, length(pca_scores))

for (i in (window + 1):length(pca_scores)) {
  past_scores <- pca_scores[(i - window):(i - 1)]
  q_high <- quantile(past_scores, 0.90, na.rm = TRUE)
  q_low <- quantile(past_scores, 0.10, na.rm = TRUE)
  if (pca_scores[i] > q_high) {
    pca_exposure[i] <- 0.5
  } else if (pca_scores[i] < q_low) {
    pca_exposure[i] <- 1.5
  } else {
    pca_exposure[i] <- 1
  }
}

# Lag exposure to avoid look-ahead bias
indicators$exposure <- lag(pca_exposure, 1)

# Strategy Returns
indicators$strategy_returns <- indicators$SPY_Returns * indicators$exposure

# Final cleanup
final_returns <- na.omit(indicators[, c("strategy_returns", "SPY_Returns")])
colnames(final_returns) <- c("Strategy", "SPY_BuyHold")

# Performance Comparison
charts.PerformanceSummary(final_returns,
                          main = "Rolling PCA-based (PC1) Quantile Allocation vs SPY Buy&Hold")
