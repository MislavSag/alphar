library(data.table)
library(arrow)
library(dplyr)
library(qlcal)
library(ggplot2)


# SET UP ------------------------------------------------------------------
# Global vars
PATH         = "F:/data/equity/us"
PATH_DATASET = "F:/data/strategies/pead"

# Set NYSE calendar
setCalendar("UnitedStates/NYSE")

# TODO: NOt sure if I need this
# # Constants
# update = TRUE

# EARING ANNOUNCEMENT DATA ------------------------------------------------
# get events data
events = read_parquet(fs::path(PATH, "fundamentals", "earning_announcements", ext = "parquet"))

# Plot number of rows by date
if (interactive()) {
  ggplot(events[, .N, by = date][order(date)], aes(x = date, y = N)) +
    geom_line() +
    theme_minimal()
}

# Remove observations before today
events = events[date < Sys.Date()]

# Remove duplicates
events[, .N, by = c("symbol", "date")][N > 1]
events = unique(events, by = c("symbol", "date"))  # remove duplicated symbol / date pair

# Get investing.com data
investingcom_ea = read_parquet(
  fs::path(
    PATH,
    "fundamentals",
    "earning_announcements_investingcom",
    ext = "parquet"
  )
)

# Keep columns we need and convert date and add col names prefix
investingcom_ea = investingcom_ea[, .(symbol, time, eps, eps_forecast, revenue, revenue_forecast, right_time)]
investingcom_ea[, date_investingcom := as.Date(time)]
investingcom_ea = investingcom_ea[date_investingcom <= qlcal::advanceDate(Sys.Date(), 2)]
setnames(investingcom_ea,
         colnames(investingcom_ea)[2:6],
         paste0(colnames(investingcom_ea)[2:6], "_investingcom"))
setorder(investingcom_ea, date_investingcom)
investingcom_ea[date_investingcom %between% c("2024-11-01", "2024-11-08")][!is.na(eps_investingcom)]

# Plot number of rows by date
if (interactive()) {
  ggplot(investingcom_ea[, .N, by = date_investingcom][order(date_investingcom)],
         aes(x = date_investingcom, y = N)) +
    geom_line() +
    theme_minimal()
}

# Get earnings surprises data from FMP
es = read_parquet(
  fs::path(
    PATH,
    "fundamentals",
    "earning_surprises",
    ext = "parquet"
  )
)
if (interactive()) {
  ggplot(es[, .N, by = date][order(date)], aes(x = date, y = N)) +
    geom_line() +
    theme_minimal()
}

# merge DT and investing com earnings surprises
events = merge(
  events,
  investingcom_ea,
  by.x = c("symbol", "date"),
  by.y = c("symbol", "date_investingcom"),
  all.x = TRUE,
  all.y = FALSE
)
events = merge(events, es, by = c("symbol", "date"), all = TRUE)

# Keep only observations available in both datasets by checking dates
events = events[!is.na(date) & !is.na(as.Date(time_investingcom))]

# Check if time are the same
events[!is.na(right_time) & right_time == "marketClosed ", right_time := "amc"]
events[!is.na(right_time) & right_time == "marketOpen ", right_time := "bmo"]
events[, same_announce_time := time == right_time]

# if both fmp cloud and investing.com data exists keep similar
print(paste0("Number of removed observations because time of announcements are not same :",
             sum(!((events$same_announce_time) == TRUE), na.rm = TRUE), " or ",
             round(sum(!((events$same_announce_time) == TRUE), na.rm = TRUE) / nrow(events), 4) * 100, "% percent."))
events = events[events$same_announce_time == TRUE]

# Remove duplicated events
events = unique(events, by = c("symbol", "date"))

# Keep only rows with similar eps at least in one of datasets (investing com of sueprises in FMP)
eps_threshold = 0.1
events_ic_similar  = events[(eps >= eps_investingcom * 1-eps_threshold) & eps <= (1 + eps_threshold)]
events_fmp_similar = events[(eps >= actualEarningResult * 1-eps_threshold) & actualEarningResult <= (1 + eps_threshold)]
events = unique(rbind(events_ic_similar,
                      events_fmp_similar,
                      events[date > (Sys.Date() - 1)]))

# Plot number of rows by date
if (interactive()) {
  ggplot(events[, .N, by = date][order(date)], aes(x = date, y = N)) +
    geom_line() +
    theme_minimal()
}

# Remove column we don't need
cols_remove = c(
  "eps_investingcom", "eps_forecast_investingcom", "revenue_investingcom",
  "revenue_forecast_investingcom", "right_time", "actualEarningResult",
  "estimatedEarning", "same_announce_time", "time_investingcom", "updatedFromDate")
events[, (cols_remove) := NULL]

# Check for business days
events[, all(isBusinessDay(date))]
events[, sum(!isBusinessDay(date))]
round((events[, sum(!isBusinessDay(date))] / nrow(events)) * 100, 2)
events = events[symbol %notin% events[!isBusinessDay(date), symbol]]
events[, all(isBusinessDay(date))]

# Checks
events[, max(date)] # last date
events[date == max(date), symbol] # Symbols for last date


# MARKET DATA AND FUNDAMENTALS ---------------------------------------------
# Get factors
path_to_parquet = fs::path(PATH, "predictors_daily", "factors", "prices_factors.parquet")
events_symbols = c(events[, unique(symbol)], "SPY")
prices_dt = open_dataset(path_to_parquet, format = "parquet") |>
  dplyr::filter(date > "2008-01-01", symbol %in% events_symbols) |>
  dplyr::arrange(symbol, date) |>
  collect() |>
  setDT(prices_dt)

# Checks and summarizes
prices_dt[, max(date, na.rm = TRUE)]

# Filter dates and symbols
prices_dt = unique(prices_dt, by = c("symbol", "date"))
prices_n = prices_dt[, .N, by = symbol]
prices_n = prices_n[which(prices_n$N > 700)]  # remove prices with only 700 or less observations
prices_dt = prices_dt[symbol %chin% prices_n[, symbol]]

# Remove symbols that have (almost) constant close prices
prices_dt[, sd_roll := roll::roll_sd(close, 22 * 6), by = symbol]
symbols_remove = prices_dt[sd_roll == 0, unique(symbol)]
prices_dt = prices_dt[symbol %notin% symbols_remove]

# Set key
setkey(prices_dt, "symbol")
setorder(prices_dt, symbol, date)
key(prices_dt)

# Calculate most liquid symbols
prices_dt[, dollar_vol_rank := frankv(close_raw * volume, order = -1L), by = date]

# SPY data
spy = open_dataset("F:/data/equity/daily_fmp_all.csv", format = "csv") |>
  dplyr::filter(symbol == "SPY") |>
  dplyr::select(date, adjClose) |>
  dplyr::rename(close = adjClose) |>
  collect()
setDT(spy)
spy[, returns := close / shift(close) - 1]
spy = na.omit(spy)

# Free memory
gc()

# Check if dates form events are aligned with dates from prices
# This checks should be done after market closes, but my data is updated after
# 00:00, so take this into account
events[, max(date)]
last_trading_day = events[, data.table::last(sort(unique(date)), 3)[1]]
last_trading_day_corected = events[, data.table::last(sort(unique(date)), 4)[1]]
prices_dt[, max(date)]
prices_dt[date == last_trading_day]
prices_dt[date == last_trading_day_corected]


# REGRESSION LABELING ----------------------------------------------------------
# calculate returns
prices_dt[, ret_5 := shift(close, -6L, "shift") / shift(close, -1L, "shift") - 1, by = "symbol"]
prices_dt[, ret_22 := shift(close, -22L, "shift") / shift(close, -1L, "shift") - 1, by = "symbol"]
prices_dt[, ret_44 := shift(close, -44L, "shift") / shift(close, -1L, "shift") - 1, by = "symbol"]
prices_dt[, ret_66 := shift(close, -66L, "shift") / shift(close, -1L, "shift") - 1, by = "symbol"]

# calculate rolling sd
prices_dt[, sd_5 := roll::roll_sd(returns, 5), by = "symbol"]
prices_dt[, sd_22 := roll::roll_sd(close / shift(close, 1L) - 1, 22), by = "symbol"]
prices_dt[, sd_44 := roll::roll_sd(close / shift(close, 1L) - 1, 44), by = "symbol"]
prices_dt[, sd_66 := roll::roll_sd(close / shift(close, 1L) - 1, 66), by = "symbol"]

# calculate spy returns
spy[, ret_5_spy := shift(close, -6L, "shift") / shift(close, -1L, "shift") - 1]
spy[, ret_22_spy := shift(close, -22L, "shift") / shift(close, -1L, "shift") - 1]
spy[, ret_44_spy := shift(close, -44L, "shift") / shift(close, -1L, "shift") - 1]
spy[, ret_66_spy := shift(close, -66L, "shift") / shift(close, -1L, "shift") - 1]

# calculate excess returns
prices_dt <- merge(prices_dt,
                   spy[, .(date, ret_5_spy, ret_22_spy, ret_44_spy, ret_66_spy)],
                   by = "date", all.x = TRUE, all.y = FALSE)
prices_dt[, ret_5_excess := ret_5 - ret_5_spy]
prices_dt[, ret_22_excess := ret_22 - ret_22_spy]
prices_dt[, ret_44_excess := ret_44 - ret_44_spy]
prices_dt[, ret_66_excess := ret_66 - ret_66_spy]
prices_dt[, `:=`(ret_5_spy = NULL, ret_22_spy = NULL, ret_44_spy = NULL, ret_66_spy = NULL)]
setkey(prices_dt, symbol)
setorder(prices_dt, symbol, date)

# Calculate standardized excess returns
prices_dt[, ret_excess_stand_5 := ret_5_excess / shift(sd_5, -4L), by = "symbol"]
prices_dt[, ret_excess_stand_22 := ret_22_excess / shift(sd_22, -21L), by = "symbol"]
prices_dt[, ret_excess_stand_44 := ret_44_excess / shift(sd_44, -43L), by = "symbol"]
prices_dt[, ret_excess_stand_66 := ret_66_excess / shift(sd_66, -65L), by = "symbol"]

# Calculate standardized returns
prices_dt[, ret_stand_5 := ret_5 / shift(sd_5, -4L), by = "symbol"]
prices_dt[, ret_stand_22 := ret_22 / shift(sd_22, -21L), by = "symbol"]
prices_dt[, ret_stand_44 := ret_44 / shift(sd_44, -43L), by = "symbol"]
prices_dt[, ret_stand_66 := ret_66 / shift(sd_66, -65L), by = "symbol"]

# remove unnecesary columns
prices_dt[, `:=`(sd_5 = NULL, sd_22 = NULL, sd_44 = NULL, sd_66 = NULL)]
# prices_dt[, `:=`(ret_5 = NULL, ret_22 = NULL, ret_44 = NULL, ret_66 = NULL)]
prices_dt[, `:=`(ret_excess_stand_5 = NULL, ret_excess_stand_22 = NULL,
                 ret_excess_stand_44 = NULL, ret_excess_stand_66 = NULL)]

# Calculate one day return. We will use that in results, to calculate portfolio
# returns easier.
prices_dt[, ret_1_lead := shift(close, type = "lead") / close - 1, by = "symbol"]


# MERGE MARKET DATA, EVENTS AND CLASSIF LABELS ---------------------------------
# Merge events and prices
events[, date_event := date]
prices_dt[, date_prices := date]
dataset = prices_dt[events, on = c("symbol", "date"), roll = Inf]

# Defin possibly target columns
possible_target_vars = colnames(dataset)[grepl("^ret_", colnames(dataset))]

# Extreme labeling (BIAS? quantile on all set)
bin_extreme_col_names <- paste0("bin_extreme_", possible_target_vars)
dataset[, (bin_extreme_col_names) := lapply(.SD, function(x) {
  y <- cut(x,
           quantile(x, probs = c(0, 0.2, 0.8, 1), na.rm = TRUE),
           labels = c(-1, NA, 1),
           include.lowest = TRUE)
  as.factor(droplevels(y))
}), .SDcols = possible_target_vars]

# around zero labeling
labeling_around_zero <- function(x) {
  x_abs <- abs(x)
  bin <- cut(x_abs, quantile(x_abs, probs = c(0, 0.3333), na.rm = TRUE), labels = 0L, include.lowest = TRUE)
  max_0 <- max(x[bin == 0], na.rm = TRUE)
  min_0 <- min(x[bin == 0], na.rm = TRUE)
  levels(bin) <- c(levels(bin), 1L, -1L)
  bin[x > max_0] <- as.character(1L)
  bin[x < min_0] <- as.factor(-1)
  return(bin)
}
bin_aroundzero_col_names <- paste0("bin_aroundzero_", possible_target_vars)
dataset[, (bin_aroundzero_col_names) := lapply(.SD, labeling_around_zero), .SDcols = possible_target_vars]

# simple labeling (ret > 0 -> 1, vice versa)
bin_extreme_col_names <- paste0("bin_simple_", possible_target_vars)
dataset[, (bin_extreme_col_names) := lapply(.SD, function(x) {
  as.factor(ifelse(x > 0, 1, 0))
}), .SDcols = possible_target_vars]

# decile labeling
bin_decile_col_names <- paste0("bin_decile_", possible_target_vars)
dataset[, (bin_decile_col_names) := lapply(.SD, function(x) {
  y <- cut(x,
           quantile(x, probs = c(0, seq(0.1, 0.9, 0.1), 1), na.rm = TRUE),
           labels = 1:10,
           include.lowest = TRUE)
  as.factor(droplevels(y))
}), .SDcols = possible_target_vars]

# Sort dataset and set key
setkey(dataset, symbol)
setorderv(dataset, c("symbol", "date"))

# Checks
dataset[, .(date, date_event, date_prices)]
dataset[ date_event != date_prices, .(date, date_event, date_prices)]
events[, max(date)]
last_trading_day = events[, data.table::last(sort(unique(date)), 3)[1]]
last_trading_day_corected = events[, data.table::last(sort(unique(date)), 4)[1]]
prices_dt[, max(date)]
prices_dt[date == last_trading_day]
prices_dt[date == last_trading_day_corected]
symbols_ = dataset[date == max(date), symbol]
prices_dt[date == max(date) & symbol %in% symbols_, .SD, .SDcols = c("date", "symbol", "maxret")]
dataset[date == max(date), .SD, .SDcols = c("date", "symbol", "maxret")]
dataset[, max(date_prices, na.rm = TRUE)]
dataset[date_prices == max(date_prices, na.rm = TRUE), .SD, .SDcols = c("date", "symbol", "maxret")]
cols_ = c("date", "symbol", "maxret", "indmom")
dataset[date == last_trading_day_corected, .SD, , .SDcols = cols_]


# PEAD EFFECT -------------------------------------------------------------
# Calculate PEAD effect
back = dataset[, .(symbol, date, date_event, date_prices, time, marketCap, open,
                   close, volume, close_raw, returns, dollar_vol_rank,
                   eps, epsEstimated, revenue, revenueEstimated,
                   ret_5, ret_22, ret_44, ret_66)]
back[, eps_surprse := (eps - epsEstimated / abs(epsEstimated))]

# summaries
back[, .(
  mret5 = mean(ret_5, na.rm = TRUE),
  mret22 = mean(ret_22, na.rm = TRUE),
  mret44 = mean(ret_44, na.rm = TRUE),
  mret66 = mean(ret_66, na.rm = TRUE)
  )]
back[eps_surprse > 0, .(
  mret5 = mean(ret_5, na.rm = TRUE),
  mret22 = mean(ret_22, na.rm = TRUE),
  mret44 = mean(ret_44, na.rm = TRUE),
  mret66 = mean(ret_66, na.rm = TRUE)
)]
back[eps_surprse > 0.1, .(
  mret5 = mean(ret_5, na.rm = TRUE),
  mret22 = mean(ret_22, na.rm = TRUE),
  mret44 = mean(ret_44, na.rm = TRUE),
  mret66 = mean(ret_66, na.rm = TRUE)
)]

# Regrssion
ggplot(back, aes(x = eps_surprse, y = ret_5)) +
  geom_point() +
  geom_smooth(method = "lm") +
  theme_minimal()
ggplot(back[eps_surprse %between% c(-10, 100)], aes(x = eps_surprse, y = ret_5)) +
  geom_point() +
  geom_smooth(method = "lm") +
  theme_minimal()
ggplot(back[eps_surprse > 0], aes(x = eps_surprse, y = ret_5)) +
  geom_point() +
  geom_smooth(method = "lm") +
  theme_minimal()
ggplot(back[eps_surprse > 0], aes(x = eps_surprse, y = ret_5)) +
  geom_point() +
  geom_smooth(method = "glm") +
  theme_minimal()
ggplot(back[eps_surprse > 0], aes(x = eps_surprse, y = ret_5)) +
  geom_point() +
  geom_smooth(method = "gam") +
  theme_minimal()

# Bars
back[, eps_surperise_bins_equal := frank(eps_surprse)]
back[, eps_surperise_bins_equal := cut(
  eps_surperise_bins_equal,
  quantile(eps_surperise_bins_equal, probs = seq(0, 1, by = 0.1), na.rm = TRUE),
  include.lowest = TRUE)]
back[, .N, by = eps_surperise_bins_equal]
back[, mean(ret_5, na.rm = TRUE),
     by = eps_surperise_bins_equal] |>
  ggplot(aes(x = eps_surperise_bins_equal, y = V1)) +
  geom_bar(stat = "identity") +
  theme_minimal()
back[, mean(ret_22, na.rm = TRUE),
     by = eps_surperise_bins_equal] |>
  ggplot(aes(x = eps_surperise_bins_equal, y = V1)) +
  geom_bar(stat = "identity") +
  theme_minimal()
back[, mean(ret_44, na.rm = TRUE),
     by = eps_surperise_bins_equal] |>
  ggplot(aes(x = eps_surperise_bins_equal, y = V1)) +
  geom_bar(stat = "identity") +
  theme_minimal()
back[, mean(ret_66, na.rm = TRUE),
     by = eps_surperise_bins_equal] |>
  ggplot(aes(x = eps_surperise_bins_equal, y = V1)) +
  geom_bar(stat = "identity") +
  theme_minimal()

back[, median(ret_5, na.rm = TRUE),
     by = eps_surperise_bins_equal] |>
  ggplot(aes(x = eps_surperise_bins_equal, y = V1)) +
  geom_bar(stat = "identity") +
  theme_minimal()
back[, median(ret_22, na.rm = TRUE),
     by = eps_surperise_bins_equal] |>
  ggplot(aes(x = eps_surperise_bins_equal, y = V1)) +
  geom_bar(stat = "identity") +
  theme_minimal()
back[, median(ret_44, na.rm = TRUE),
     by = eps_surperise_bins_equal] |>
  ggplot(aes(x = eps_surperise_bins_equal, y = V1)) +
  geom_bar(stat = "identity") +
  theme_minimal()
back[, median(ret_66, na.rm = TRUE),
     by = eps_surperise_bins_equal] |>
  ggplot(aes(x = eps_surperise_bins_equal, y = V1)) +
  geom_bar(stat = "identity") +
  theme_minimal()

# Impact of dollar vol



# QC VS LOCAL -------------------------------------------------------------
# Comapre straddle over earnings between WC and local
symbol_ = "BAC"
date_1 = as.Date("2021-12-20")
date_2 = as.Date("2022-01-20")
dataset[symbol == symbol_ & date %between% c(date_1, date_2), .(symbol, date, date_event, date_prices, time)]

