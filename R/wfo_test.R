library(data.table)
library(Rcpp)
library(runner)


# Import data
df            = fread("C:/Users/Mislav/Documents/GitHub/alphar/tmp/df.csv")
backtest_data = fread("C:/Users/Mislav/Documents/GitHub/alphar/tmp/backtest_data.csv")
params_wf     = fread("C:/Users/Mislav/Documents/GitHub/alphar/tmp/params_wf.csv")

#
# variables_wf  = colnames(df)[3:length(df)]
# quantiles_wf  = df[, ..variables_wf][, lapply(.SD, quantile, probs = seq(0, 1, 0.05))]
# params_wf = melt(quantiles_wf)
# params_wf[, variable := as.character(variable)]
# setnames(params_wf, c("variable", "thresholds"))

# Source Rcpp
Rcpp::sourceCpp("C:/Users/Mislav/Documents/GitHub/alphar/backtest.cpp")


# Test backtest function
BacktestVectorized = function(returns, indicator, threshold, return_cumulative = TRUE) {
  sides <- ifelse(c(NA, head(indicator, -1)) > threshold, 0, 1)
  sides[is.na(sides)] <- 1

  returns_strategy <- returns * sides

  if (return_cumulative) {
    return(PerformanceAnalytics::Return.cumulative(returns_strategy))
  } else {
    return(returns_strategy)
  }
}
system.time({b1 = BacktestVectorized(df$returns, backtest_data[, sd_pr_below_dummy_net_03975940], 0.44)})
system.time({b2 = backtest_cpp_gpt(df$returns, backtest_data[, sd_pr_below_dummy_net_03975940], 0.44)})
round(b1, 2) == round(b2, 2)

# Test params backtest
system.time({
  paramb =
    vapply(1:nrow(params_wf), function(i)
      BacktestVectorized(df$returns,
                         backtest_data[, get(params_wf$variable[i])],
                         # TTR::SMA(backtest_data[, get(vars[i])], ns[i]),
                         params_wf$thresholds[i]),
      numeric(1))
})
system.time({paramb_cpp = opt(as.data.frame(df), as.data.frame(params_wf))})
all(round(paramb, 2) == round(paramb_cpp, 2))

# Test rolling params backtest
system.time({
  test1 = runner(
    x = as.data.frame(df),
    f = function(x) {
      vapply(1:nrow(params_wf), function(i)
        BacktestVectorized(x$returns,
                           x[, params_wf$variable[i]],
                           params_wf$thresholds[i]),
        numeric(1))

    },
    k = 10,
    at = 100:200,
    simplify = FALSE
  )
})

system.time({
  test2 = runner(
    x = as.data.frame(df),
    f = function(x) {
      opt(x, params_wf)
    },
    k = 10,
    at = 100:200,
    simplify = FALSE
  )
})

# # Test dataframe slicing
# df_ = sliceDataFrameColumnWise(df, 0, 2)
# df_
# df[1:3, ]
#
# # Test binding scalar and df
# cbind_scalar_and_df(210, df)
#
# # Test binding scalar and vector
# bind_scalar_to_vector(c(210, 310), df[, "returns"])

# Test wfo function
df_test = as.data.frame(df[1:120])
x = wfo(df_test, params_wf, 20, "rolling")
y = wfo(df_test, params_wf, 20, "expanding")
