library(findata)
library(data.table)
library(ggplot2)
library(roll)
library(PerformanceAnalytics)


# Import CME data
cme = CME$new()
cme$token
fwdt = cme$fedwatch_history()

# q: How to get expected rate from lowerRt, upperRt and probability
fwdt[, expected_rate := sum(probability * ((lowerRt + upperRt) / 2)), by = reportingDt]
setorder(fwdt, reportingDt) # TODO: Add to package

# Import history of meeting dates to get reall lower an upper
fwdt_real = cme$get_meetings_history(limit = 100)
fwdt_real = fwdt_real[meetingDt > fwdt[, min(reportingDt)]]

# Plot expected rate vs real rate
data_plot = fwdt[, expected_rate := sum(probability * ((lowerRt + upperRt) / 2)), by = reportingDt]

# PLot lowerRT and upperRt varaibles to see how they change over time. Emphesize curve based on probability variable
dt_plot = melt(fwdt[, .(reportingDt, lowerRt, upperRt, probability)],
               id.vars = c("reportingDt", "probability"))
ggplot(dt_plot, aes(x = reportingDt, y = value, color = variable)) +
  geom_line() +
  geom_line(data = fwdt_real, aes(x = meetingDt, y = upperRt), color = "black") +
  # geom_point(data = fwdt, aes(x = meetingDt, y = probability), color = "blue") +
  labs(title = "Rate Range Changes Over Time with Probability",
       x = "Meeting Date",
       y = "Rate Range",
       color = "Probability") +
  theme_minimal()

# Crate predictors
X = unique(fwdt[, .(date = reportingDt, e = expected_rate)])

# TLT prices
tlt = getSymbols("TLT", from = "2020-01-01", auto.assign = FALSE)
tlt = as.data.table(tlt)
tlt = tlt[, .(date = index, close = `TLT.Adjusted`)]
tlt[, returns := close / shift(close) - 1]
tlt = na.omit(tlt)
tlt = tlt[, .(date, returns)]

# Merge tlt and fwdt
X = merge(X, tlt, by = "date", all.x = TRUE, all.y = FALSE)
X = X[, e := shift(e, 1)]
# X[, e := e - shift(e)]
X = na.omit(X)

# Rolling regession
X[, (c("intercept", "coeff_1")) := as.data.table(roll_lm(e, returns, width = nrow(X), min_obs = 22 * 5)$coefficients)]
X[, prediction := shift(intercept) + shift(coeff_1) * e]

# Backtest
back = X[, .(date,
             benchmark = returns,
             strategy = fifelse(prediction > 0, returns, 0))]
charts.PerformanceSummary(as.xts.data.table(back))

