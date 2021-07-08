library(data.table)
library(leanr)
library(esback)


# parameters
ticker = 'AAL'
backtest_path <- paste0("D:/risks/risk-factors/hour/", ticker)

# import spy radf data
spy <- import_lean('D:/market_data/equity/usa/hour/trades_adjusted', ticker)
spy[, returns := close / shift(close) - 1]
bactest_files <- list.files(backtest_path, full.names = TRUE)
backtest_data <- lapply(bactest_files, fread)
names(backtest_data) <- gsub(".*/|\\.csv", "", bactest_files)
backtest_data <- rbindlist(backtest_data, idcol = TRUE)
backtest_data[, datetime := as.POSIXct(datetime, tz = "EST")]
backtest_data <- merge(backtest_data, spy[, .(datetime, returns)], by = "datetime", all.x = TRUE, all.y = FALSE)
setorderv(backtest_data, c(".id", "datetime"))

# backtest
sample_ <- na.omit(backtest_data[1:10000])
x <- esr_backtest(r = sample_$returns, q = sample_$var_week, e = sample_$es_week, alpha = 0.025, version = 2)
x

backtest_data[, (cols_new) := lapply(.SD, function(x) roll_quantile(x, 6 * 22 * 1, p = 0.95)),
              .SDcols = cols, by = .id]

# backtest

# Load the esback package
library(esback)

# Load the data
data(risk_forecasts)
head(risk_forecasts)

# Plot the returns and expected shortfall forecasts
plot(risk_forecasts$r, xlab = "Observation Number", ylab = "Return and ES forecasts")
lines(risk_forecasts$e, col = "red", lwd = 2)

# Backtest the forecast using the ESR test
esr_backtest(r = risk_forecasts$r, e = risk_forecasts$e, alpha = 0.025, version = 1)
