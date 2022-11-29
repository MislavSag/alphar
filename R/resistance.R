# library(ts2net)
library(finfeatures)
library(ggplot2)
library(data.table)
library(stringr)
library(dataPreparation)
library(runner)


# importa data
data("spy_hour")

# calcualte ohlc returns
setDT(spy_hour)
cols <- c("open", "high", "low", "close")
spy_hour[, (paste0(cols, "_returns")) := lapply(.SD, function(x) (x / shift(x)) - 1), .SDcols = cols]
spy_hour <- na.omit(spy_hour)

# TODO add to mlfinance package
# remove ooutliers
remove_sd_outlier_recurse <- function(df, ...) {

  # remove outliers
  x_ <- remove_sd_outlier(data_set = df, ...)
  while (nrow(x_) < nrow(df)) {
    df <- x_
    x_ <- remove_sd_outlier(data_set = df, ...)
  }
  return(df)
}

# remove outliers
spy <- remove_sd_outlier_recurse(df = spy_hour, cols = c("high_returns", "high"), n_sigmas = 5)
# high = spy$high[1:2000]
# close = spy$close[1:2000]

#  resistance function
get_resistance <- function(high, close, breaks = 20) {

  # get bins
  ts_intervals = cut(high, breaks = breaks)
  upper_bound <- sapply(str_extract_all(ts_intervals, "-?[0-9.]+"), function(x) max(as.numeric(x)))
  lower_bound <- sapply(str_extract_all(ts_intervals, "-?[0-9.]+"), function(x) min(as.numeric(x)))

  # merge data
  dt <- cbind.data.frame(n = seq_along(high), high = high, upper_bound,
                         lower_bound, state = as.integer(ts_intervals),
                         bin_mean = (upper_bound + lower_bound) / 2,
                         close = close)
  setDT(dt)
  dt[, grp := rleid(state)]

  # calculate resistance
  grp_high <- dt[, .(max_price_bin = max(high)), by = grp]
  grp_high <- grp_high[max_price_bin > shift(max_price_bin) &
                         shift(max_price_bin, type = "lead") < max_price_bin, grp_opt := TRUE]
  grp_high <- grp_high[grp_opt == TRUE][
    max_price_bin < shift(max_price_bin) | max_price_bin < shift(max_price_bin, type = "lead"), grp_opt := FALSE][
      grp_opt == TRUE][, .(grp, grp_opt)]
  DT <- grp_high[dt, on = c("grp" = "grp")]
  DT[, resistance := grp_opt == TRUE & high == max(high), by = grp]
  DT[resistance == TRUE, resistance_value := max(high), by = state]

  # add number of resistance points
  DT <- DT[, .(resistance_n = sum(resistance, na.rm = TRUE)), by = state][DT, on = c("state" = "state")]

  return(DT)
}

# plot resistance function
plot_resistance <- function(DT) {
  annotation <- data.frame(
    x = rep(0, length(unique(DT$upper_bound))),
    y = unique(DT[order(state), bin_mean]),
    label = DT[, sum(resistance, na.rm = TRUE), by = state][order(state), V1]
  )

  ggplot(DT, aes(x = n, y = high)) +
    geom_line() +
    geom_hline(yintercept = unique(DT$upper_bound), alpha = 0.2) +
    geom_point(data = DT[resistance == TRUE], color = "red", size = 2) +
    geom_text(data=annotation, aes( x=x, y=y, label=label),
              color="blue",
              size=4, fontface="bold" ) +
    geom_hline(yintercept = DT$resistance_value, color = "red")

}


# calcualte resistane on rolling window
resistances <- runner(
  x = as.data.frame(spy),
  f = function(x) {
    get_resistance(x[, "high"], x[, "close"])
  },
  k = 1000,
  lag = 1L,
  na_pad = TRUE,
  simplify = FALSE
)

# merge and clean results
length(resistances[[1]])
resistance_enter <- lapply(resistances, function(x) {
  if (length(x) > 1) {
    resistances_values <- na.omit(x[resistance_n  >= 5, unique(resistance_value)])
    enter <- any((tail(x$high, 1) / (resistances_values) - 1) > 0.01) &
      any((tail(x$high, 1) / (resistances_values) - 1) < 0.02)
  } else {
    enter <- NA
  }
  return(enter)
})
signals <- unlist(resistance_enter)
table(signals, useNA = "ifany")

# signals index
singals_index <- which(c(0, diff(signals)) == 1)

# get highs
prices_high <- lapply(resistances, function(x) {
  if (length(x) > 1) {
    return(tail(x$high, 1))
  } else {
    return(NA)
  }
})
prices_high <- unlist(prices_high)

# plot signals
dt_plot <- as.data.table(prices_high)
dt_plot[singals_index, buy := TRUE]
dt_plot[, n := seq_along(buy)]
ggplot(dt_plot, aes(x = n, y = prices_high)) +
  geom_line() +
  geom_point(data = dt_plot[buy == TRUE], aes(x = n, y = prices_high), color = "red")

# zoom in
dt_plot_sample <- dt_plot[28000:30000]
ggplot(dt_plot_sample, aes(x = n, y = prices_high)) +
  geom_line() +
  geom_point(data = dt_plot_sample[buy == TRUE], aes(x = n, y = prices_high), color = "red")
dt_plot_sample <- dt_plot[10000:12000]
ggplot(dt_plot_sample, aes(x = n, y = prices_high)) +
  geom_line() +
  geom_point(data = dt_plot_sample[buy == TRUE], aes(x = n, y = prices_high), color = "red")


dt_plot_sample <- dt_plot[8000:10000]
ggplot(dt_plot_sample, aes(x = n, y = prices_high)) +
  geom_line() +
  geom_point(data = dt_plot_sample[buy == TRUE], aes(x = n, y = prices_high), color = "red")

dt_plot_sample <- dt_plot[27000:28700]
ggplot(dt_plot_sample, aes(x = n, y = prices_high)) +
  geom_line() +
  geom_point(data = dt_plot_sample[buy == TRUE], aes(x = n, y = prices_high), color = "red")


# send signals to azure blob
qc_resistance <- copy(spy)
qc_resistance[singals_index, buy := 1]
qc_resistance <- qc_resistance[, .(datetime, buy)]
qc_resistance[, datetime := format.POSIXct(datetime, format = "%Y%m%d %H:%M")]
qc_resistance[, buy := nafill(buy, fill = 0)]
BLOBENDPOINT <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT"),
                                 key=Sys.getenv("BLOB-KEY"))
cont <- storage_container(BLOBENDPOINT, "qc-backtest")
storage_write_csv(qc_resistance, cont, file = "resistance.csv", col_names = FALSE)

