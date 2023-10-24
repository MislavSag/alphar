library(data.table)
library(lubridate)
library(PerformanceAnalytics)
library(ggplot2)
library(StockDistFit)
library(patchwork)



# import adjusted daily market data
dt = fread("F:/lean_root/data/all_stocks_daily.csv") # try hourly

# this want be necessary after update
setnames(dt, c("date", "open", "high", "low", "close", "volume", "close_adj", "symbol"))

# remove duplicates
dt = unique(dt, by = c("symbol", "date"))

# remove missing values
dt = na.omit(dt)

# add variables
dt[, dollar_volume := volume * close]

# remove negative prices
dt = dt[close > 0 & close_adj > 0]

# order data
setorder(dt, "symbol", "date")

# adjust all prices, not just close
adjust_cols <- c("open", "high", "low")
adjust_cols_new <- c("open_adj", "high_adj", "low_adj")
dt[, (adjust_cols_new) := lapply(.SD, function(x) x * (close_adj / close)), .SDcols = adjust_cols] # adjust open, high and low prices

# calculate returns
dt[, returns := close_adj / shift(close_adj) - 1, by = symbol] # calculate returns
dt <- dt[returns < 1] # TODO:: better outlier detection mechanism. For now, remove daily returns above 100%

# remove symobls with < 252 observations
dt_n <- dt[, .N, by = symbol]
dt_n <- dt_n[N > 252]  # remove prices with only 700 or less observations
dt <- dt[symbol %in% dt_n[, symbol]]

# save SPY for later and keep only events symbols
spy <- dt[symbol == "spy"]
setorder(spy, date)

# prepare data
X = dt[, .(symbol, date, close_adj, returns)]
X = na.omit(X)

# fit distribution cross sectionaly (for every date)
dist <- c("norm_fit", "t_fit", "cauchy_fit", "ghd_fit", "hd_fit",
          "sym.ghd_fit", "sym.hd_fit", "vg_fit", "sym.vg_fit",
          "nig_fit", "ged_fit", "skew.t_fit", "skew.normal_fit",
          "skew.ged_fit")
distributions_names = stringr::str_to_title(gsub("_.*", "", dist))
X_sample = X[date > as.Date("2005-01-01") & date < as.Date("2010-01-01")]
empty_df = data.table(
  norm_fit = NA_real_,
  t_fit = NA_real_,
  cauchy_fit = NA_real_,
  ghd_fit = NA_real_,
  hd_fit = NA_real_,
  sym.ghd_fit = NA_real_,
  sym.hd_fit = NA_real_,
  vg_fit = NA_real_,
  sym.vg_fit = NA_real_,
  nig_fit = NA_real_,
  ged_fit = NA_real_,
  skew.t_fit = NA_real_,
  skew.normal_fit = NA_real_,
  skew.ged_fit = NA_real_,
  best_aic = NA_character_
)
dist_fit_cross_section = X_sample[, tryCatch({
  print(.BY$date)
  best_dist(fit_multiple_dist(dist, as.data.frame(returns)),
            distributions_names)
}, error = function(e) empty_df),
by = date]

# save
time_ = strftime(Sys.time(), format = "%Y%m%d%H%M%S")
fwrite(dist_fit_cross_section, paste0("D:/features/dist_fit_cross_section_", time_, ".csv"))

# loada data
list.files("D:/features", pattern = "dist_fit")
dist_fit_cross_section_1 = fread("D:/features/dist_fit_cross_section_20230610145248.csv")
dist_fit_cross_section_2 = fread("D:/features/dist_fit_cross_section_20230612141535.csv")
dist_fit_cross_section_3 = fread("D:/features/dist_fit_cross_section_20230613155138.csv")
dist_fit_cross_section_all = rbind(
  dist_fit_cross_section_3,
  dist_fit_cross_section_2,
  dist_fit_cross_section_1)
setorder(dist_fit_cross_section_all, date)

# plot dist across time
table(dist_fit_cross_section_all[, best_aic])
bdata = dist_fit_cross_section_all[spy, on = "date"]
bdata = bdata[, .(date, close, best_aic)]
bdata = na.omit(bdata)
ggplot(bdata, aes(x = date, y = close, color = factor(best_aic))) +
  geom_line(aes(group = 1))
g1 = ggplot(bdata[date %between% c("2021-01-01", "2022-01-01")], aes(x = date, y = close, color = factor(best_aic))) +
  geom_line(aes(group = 1))
g2 = ggplot(bdata[date %between% c("2022-01-01", "2023-01-01")], aes(x = date, y = close, color = factor(best_aic))) +
  geom_line(aes(group = 1))
g1/g2

# lead lag relationship
bdata = dist_fit_cross_section_all[spy, on = "date"]
bdata = bdata[, .(date, close, best_aic, returns)]
bdata = na.omit(bdata)
bdata[, best_aic_lag := shift(best_aic)]
bdata = na.omit(bdata)
bdata[, mean(returns), by = best_aic_lag]
bdata[, median(returns), by = best_aic_lag]
bdata[, prod(1 + returns) - 1, by = best_aic_lag]

# backtest
bdata[, signal := ifelse(shift(best_aic) %in% c("Sym.vg", "Vg"), 1, 0)]
bdata[, strategy := signal * returns]
bdata[, strategy_x2 := strategy * 2]
bdata[, strategy_x3 := strategy * 3]
bdata[, strategy_short := -strategy]
x = as.xts.data.table(bdata[, .(date, strategy, strategy_x2, strategy_x3, strategy_short, benchmark = returns)])
PerformanceAnalytics::charts.PerformanceSummary(x)
bdata_ = copy(bdata)
bdata_[, best_aic := ifelse(best_aic != "Vg", NA, "Vg")]
ggplot(bdata_, aes(x = date, y = close, color = factor(best_aic))) +
  geom_line(aes(group = 1))

