# TODO: I don't need all those libriaries
library(data.table)
library(lubridate)
library(ggplot2)
library(moments)


# DATA --------------------------------------------------------------------
# Import prices and MinMax data
list.files("F:/predictors/minmax")
dt = fread("F:/predictors/minmax/20240228.csv")

# check timezone
dt[, attr(date, "tz")]
dt[, date := with_tz(date, tzone = "America/New_York")]
dt[, attr(date, "tz")]

# Spy data
spy = dt[symbol == "spy", .(date, close, returns)]

# Extreme returns
cols = colnames(dt)[grep("^p_9", colnames(dt))]
cols_new_up = paste0("above_", cols)
dt[, (cols_new_up) := lapply(.SD, function(x) ifelse(returns > x, returns - shift(x), 0)),
   by = .(symbol), .SDcols = cols] # Shifted to remove look-ahead bias
cols = colnames(dt)[grep("^p_0", colnames(dt))]
cols_new_down = paste0("below_", cols)
dt[, (cols_new_down) := lapply(.SD, function(x) ifelse(returns < x, abs(returns - shift(x)), 0)),
   by = .(symbol), .SDcols = cols]


# SYSTEMIC RISK -----------------------------------------------------------
# help function to calcualte tail risk measures from panel
tail_risk = function(dt, FUN = mean, cols_prefix = "mean_") {
  cols = colnames(dt)[grep("below_p|above_p", colnames(dt))]
  indicators_ = dt[, lapply(.SD, function(x) f(x, na.rm = TRUE)),
                   by = .(date), .SDcols = cols,
                   env = list(f = FUN)]
  colnames(indicators_) = c("date", paste0(cols_prefix, cols))
  setorder(indicators_, date)
  above_sum_cols = colnames(indicators_)[grep("above", colnames(indicators_))]
  below_sum_cols = colnames(indicators_)[grep("below", colnames(indicators_))]
  excess_sum_cols = gsub("above", "excess", above_sum_cols)
  indicators_[, (excess_sum_cols) := indicators_[, ..above_sum_cols] - indicators_[, ..below_sum_cols]]
}

# Get tail risk mesures
indicators_mean      = tail_risk(dt, FUN = "mean", cols_prefix = "mean_")
indicators_sd        = tail_risk(dt, FUN = "sd", cols_prefix = "sd_")
indicators_sum       = tail_risk(dt, FUN = "sum", cols_prefix = "sum_")
indicators_skewness  = tail_risk(dt, FUN = "skewness", cols_prefix = "skewness_")
indicators_kurtosis  = tail_risk(dt, FUN = "kurtosis", cols_prefix = "kurtosis_")

# Merge indicators and spy
indicators = Reduce(function(x, y) merge(x, y, by = "date", all.x = TRUE, all.y = FALSE),
                    list(indicators_mean, indicators_sd, indicators_sum,
                         indicators_skewness, indicators_kurtosis))

# Save indicators
fwrite(indicators, "F:/predictors/minmax/indicators.csv")
