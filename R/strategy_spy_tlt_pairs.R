library(fastverse)
library(finutils)
library(roll)
library(PerformanceAnalytics)
library(TTR)
library(ggplot2)
library(copula)


# DATA --------------------------------------------------------------------
# Import data
dtl = qc_daily(
  "F:/lean/data/stocks_daily.csv",
  symbols = c("spy", "tlt"),
  add_dv_rank = FALSE
)
dt = dtl[, .(symbol, date, close)]

# Ratio prep
ratio = dcast(dt, date ~ symbol, value.var = "close")
ratio = na.omit(ratio)
# ratio[, spy_ret := log(spy) - log(shift(spy))]
# ratio[, tlt_ret := log(tlt) - log(shift(tlt))]
# ratio = na.omit(ratio)
# epsilon = 1e-6
# ratio[, spy_u := rank(spy_ret) / (.N + 1)]
# ratio[, tlt_u := rank(tlt_ret) / (.N + 1)]
# ratio[, spy_u := pmin(pmax(spy_u, epsilon), 1 - epsilon)]
# ratio[, tlt_u := pmin(pmax(tlt_u, epsilon), 1 - epsilon)]
# tryCatch({
#   copula_model = normalCopula(dim = 2)
#   fit = fitCopula(copula_model, cbind(ratio$spy_u, ratio$tlt_u), method = "ml")
#   rho = coef(fit)
# }, error = function(e) {
#   message("Error in fitting copula: ", e)
#   rho <<- 0
# })
# x = cbind(ratio$spy_u, ratio$tlt_u)
# x = na.omit(x)
# x = x[apply(x, 1, function(row) all(row > epsilon & row < (1 - epsilon))), ]
# u_joint = pCopula(x, copula = copula_model)


# 1) Spread
# Calculate spread between prices
# ratio[, ratio := spy / tlt]
# ratio[, ratio_ema_fast := TTR::EMA(ratio, 20)]
# ratio[, ratio_ema_slow := TTR::EMA(ratio, 60)]
# ratio[, ratio_scaled := (ratio_ema_slow - ratio_ema_fast) / ratio_ema_slow]
# ratio[, ratio_scaled := roll_scale(ratio_scaled, width = 66)]

# 2) Spread
ratio[, ratio := log(spy / tlt)]
ratio[, ratio_scaled := roll_scale(ratio, width = 22)]
ratio = na.omit(ratio)

# 3) Spread with PCA (GPT)
# ratio[, spy_log := log(spy)]
# ratio[, tlt_log := log(tlt)]
# ratio[, spy_norm := (spy_log - mean(spy_log)) / sd(spy_log)]
# ratio[, tlt_norm := (tlt_log - mean(tlt_log)) / sd(tlt_log)]
# pca_data = as.matrix(ratio[, .(spy_norm, tlt_norm)])
# pca_result = prcomp(pca_data, center = FALSE, scale. = FALSE)
# ratio[, PC1 := pca_result$x[, 1]]
# ratio[, PC2 := pca_result$x[, 2]]
# ratio[, spread := PC2]
# ratio[, ratio_scaled := roll_scale(spread, width = 22)]

# Plots
ggplot(ratio, aes(x = date, y = ratio_scaled)) +
  geom_line() +
  labs(title = "SPY/TLT Spread Z Score", x = "Date", y = "Z Score")
# ggplot(ratio[date %between% c(as.Date("2020-01-01"), as.Date("2021-01-01"))], aes(x = date)) +
#   geom_line(aes(y = ratio), color = "black") +
#   geom_line(aes(y = ratio_ema_fast, color = "red")) +
#   geom_line(aes(y = ratio_ema_slow, color = "blue"))

# Z score of SPY and bond ratios
dt[, returns := close / shift(close) - 1, by = symbol]
dt = na.omit(dt)
dt = dcast(dt, date ~ symbol, value.var = "returns")
dt = na.omit(dt)
dt = ratio[, .(date, ratio_scaled)][dt, on = "date"]
dt = na.omit(dt)

# Strategy
signals = vector("numeric", length = nrow(dt))
ratios = dt[, ratio_scaled]
threshold = 1.5
exit_threshold = 1
for (i in seq_along(signals)) {
  if (i == 1) {
    signals[i] = 0
  } else if (ratios[i-1] >= threshold & signals[i - 1] == 0) {
    signals[i] = 1
  } else if (signals[i - 1] == 1 & ratios[i-1] >= exit_threshold) {
    signals[i] = 1
  } else if (ratios[i-1] <= -threshold & signals[i - 1] == 0) {
    signals[i] = -1
  } else if (signals[i - 1] == -1 & ratios[i-1] <= -exit_threshold) {
    signals[i] = -1
  } else {
    signals[i] = 0
  }
}

# Strategy
dt[, strategy := fifelse(signals == 1, tlt, 0)]
dt[, strategy := fifelse(signals == -1, spy, strategy)]
dt_xts = as.xts.data.table(dt[, .(date, strategy)])
spy_xts = as.xts.data.table(dt[, .(date, spy)])
tlt_xts = as.xts.data.table(dt[, .(date, tlt)])
charts.PerformanceSummary(merge(dt_xts, spy_xts, tlt_xts))
finutils::portfolio_stats(merge(dt_xts, spy_xts, tlt_xts), market = spy_xts)
