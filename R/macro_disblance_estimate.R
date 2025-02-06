library(fastverse)
library(finutils)
library(roll)
library(forecast)
library(RollingWindow)
library(ggplot2)
library(PerformanceAnalytics)
library(patchwork)
library(runner)
library(vars)
library(parallel)
library(doParallel)
library(AzureStor)


# SETUP -------------------------------------------------------------------
# Parameters
VINTAGE = TRUE # If VINTAGE is TRUE, then we will use vintage data



# DATA --------------------------------------------------------------------
# Import SPY data
spy = qc_daily(file_path = "F:/lean/data/stocks_daily.csv", symbols = "spy")

# Import FRED metadata
meta = fread("D:/fred/meta.csv")
metad = meta[frequency_short == "D"]
metad = metad[observation_end > (max(observation_end) - 3)]

# Function to import FRED data
import_fred = function(path = "D:/fredbiased", vintage = FALSE, diff_n = FALSE) {
  # Debug
  # path = file.path("D:/fred", "fred")

  # Import data from files
  dt = rbindlist(lapply(list.files(path, full.names = TRUE), fread))

  # Merge popularity
  dt = merge(dt, unique(metad[, .(id, popularity)]), by.x = "series_id", by.y = "id")
  setorder(dt, series_id, date)

  # Create date column
  dt[, date_real := date]
  if (vintage == TRUE) {
    dt[vintage == 1, date_real := realtime_start]
    dt[date < date_real & date_real == min(date_real), date_real := date]
  }
  dt[, realtime_start := NULL]

  # Keep only SPY dates
  dt = dt[date_real %in% spy[, date]]

  # Select columns we need
  dt = dt[, .(series_id, date_real, value, popularity)]

  # locf data
  setorder(dt, series_id, date_real)
  dt[, value := nafill(value, type = "locf"), by = series_id]

  # Keep unique dates by keeping first observation
  dt = unique(dt, by = c("series_id", "date_real"))

  # locf data
  setorder(dt, series_id, date_real)
  dt[, value := nafill(value, type = "locf"), by = series_id]

  # Diff if necessary
  if (diff_n == TRUE) {
    dt = dt[, ndif := ndiffs(value), by = series_id]
    dt[, unique(ndif), by = series_id][, .N, by = V1]
    dt[ndif > 0, value_diff := c(rep(NA, unique(ndif)), diff(value, unique(ndif))), by = series_id]
  }

  # Remove missing values
  dt = na.omit(dt)

  # Volatility
  windows_vol = c(22, 66, 125, 252, 504)
  cols_vol = paste0("vol_", windows_vol)
  dt[, (cols_vol) := lapply(windows_vol, function(w) roll_sd(value, w)), by = series_id]

  # Calculate z-score of value and volatilties for all ids
  remove_ids = dt[, nrow(na.omit(.SD)), by = series_id][V1 < 252]
  dt = dt[series_id %notin% remove_ids$series_id]
  dt[, z := RollingZscore(value, window = 252, expanding = TRUE), by = series_id]
  cols_vol_z = paste0("vol_z_", windows_vol)
  dt[, (cols_vol_z) := lapply(.SD, function(x) RollingZscore(x, window = 252, expanding = TRUE, na_method = "ignore")),
     by = series_id, .SDcols = cols_vol]

  return(dt)
}

# Import data using function
macro_dt = import_fred("D:/fredbiased")
macro_dtv = import_fred(file.path("D:/fred", "fred"), vintage = TRUE)

# Plot number of observations by date
g1 = ggplot(macro_dt[, .N, by = date_real][order(date_real)], aes(x = date_real, y = N)) +
  geom_line() + scale_x_date(date_breaks = "1 year", date_labels = "%Y")
g2 = ggplot(macro_dtv[, .N, by = date_real][order(date_real)], aes(x = date_real, y = N)) +
  geom_line() + scale_x_date(date_breaks = "1 year", date_labels = "%Y")
g1 / g2

# Compare VIX for vintage and non vintage
ggplot(macro_dt[series_id == "VIXCLS"], aes(x = date_real)) +
  geom_line(aes(y = value)) +
  geom_line(data = macro_dtv[series_id == "VIXCLS"], aes(y = value), color = "red")

# Indicators
indicators = na.omit(
  macro_dt[, lapply(.SD, function(x) weighted.mean(x, 1/popularity, na.rm = TRUE)),
           by = .(date = date_real),
            .SDcols = data.table::patterns("z", cols = colnames(macro_dt))][order(date)]
)
indicatorsv = na.omit(
  macro_dtv[, lapply(.SD, function(x) weighted.mean(x, 1/popularity, na.rm = TRUE)),
            by = .(date = date_real),
            .SDcols = data.table::patterns("z", cols = colnames(macro_dtv))][order(date)]
)
ggplot(indicators, aes(x = date)) +
  geom_line(aes(y = z)) +
  geom_line(data = indicatorsv, aes(x = date, y = z), color = "red")

# Plot indicator
ggplot(melt(indicators[ , .SD, .SDcols = patterns("vol_z_\\d+$|date")], id.vars = "date"),
       aes(x = date, y = value, color = variable)) +
  geom_line() +
  scale_x_date(date_breaks = "1 year", date_labels = "%Y")

# Add vix to the indicators
indicators = merge(
  indicators,
  macro_dt[series_id == "VIXCLS", .(date = date_real, vix = value)],
  by = "date",
  all.x = TRUE,
  all.y = FALSE
  )
indicators[, ratio := vol_z_22 / vix]
ggplot(indicators, aes(x = date)) +
  geom_line(aes(y = vix / 10)) +
  geom_line(aes(y = vol_z_22)) +
  scale_x_date(date_breaks = "1 year", date_labels = "%Y")
ggplot(indicators, aes(x = date, y = ratio)) +
  geom_line() +
  scale_x_date(breaks = "1 year", date_labels = "%Y")
ggplot(indicators[date > as.Date("2020-01-01")], aes(x = date, y = ratio)) +
  geom_line() +
  scale_x_date(breaks = "1 year", date_labels = "%Y")
indicators[, mean(ratio)]
indicators[, median(ratio)]
# TODO: Idea - sell VIX when diff is very high

# Save to QC
qc_data = indicators[, .SD, .SDcols = data.table::patterns("vol_z_\\d+$|^z$|date|ratio")]
setorder(qc_data, date)
qc_data = na.omit(qc_data)
qc_data[, date := as.character(date)]
qc_data[, date := paste0(date, " 16:00:00")]
setcolorder(qc_data, c("date", "vol_z_22"))
blob_key = Sys.getenv("BLOB-KEY-SNP")
endpoint = Sys.getenv("BLOB-ENDPOINT-SNP")
BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)
cont = storage_container(BLOBENDPOINT, "qc-backtest")
storage_write_csv(qc_data, cont, "macro_disbalance.csv")

# Scatter
back = indicators[spy, on = "date"]
ggplot(back, aes(x = shift(vol_z_66), y = returns)) +
  geom_point() +
  geom_smooth(method = "lm")
summary(lm(returns ~ shift(vol_z_66), data = back))
ggplot(back, aes(x = shift(vol_z_22), y = returns)) +
  geom_point() +
  geom_smooth(method = "lm")
summary(lm(returns ~ shift(vol_z_22), data = back))
ggplot(back, aes(x = shift(ratio), y = returns)) +
  geom_point() +
  geom_smooth(method = "lm")
summary(lm(returns ~ shift(ratio), data = back))
ggplot(back, aes(x = shift(vix), y = returns)) +
  geom_point() +
  geom_smooth(method = "lm")
summary(lm(returns ~ shift(vix), data = back))
ggplot(back, aes(x = shift(vix), y = roll_sd(returns, 22))) +
  geom_point() +
  geom_smooth(method = "lm")
summary(lm(roll_sd(returns, 22) ~ shift(vix), data = back))
ggplot(back, aes(x = shift(vol_z_22), y = roll_sd(returns, 22))) +
  geom_point() +
  geom_smooth(method = "lm")
summary(lm(roll_sd(returns, 22) ~ shift(vol_z_22), data = back))

# Bars
back[, mean(returns), by = cut(vol_z_22, breaks = quantile(vol_z_22, probs = seq(0.1, 1, 0.1), na.rm = TRUE))] |>
  ggplot(aes(x = cut, y = V1)) +
  geom_bar(stat = "identity")
back[, mean(returns), by = cut(vol_z_66, breaks = quantile(vol_z_66, probs = seq(0.1, 1, 0.1), na.rm = TRUE))] |>
  ggplot(aes(x = cut, y = V1)) +
  geom_bar(stat = "identity")
back[, mean(returns), by = cut(vol_z_125, breaks = quantile(vol_z_66, probs = seq(0.1, 1, 0.1), na.rm = TRUE))] |>
  ggplot(aes(x = cut, y = V1)) +
  geom_bar(stat = "identity")
back[, mean(returns), by = cut(vol_z_252, breaks = quantile(vol_z_66, probs = seq(0.1, 1, 0.1), na.rm = TRUE))] |>
  ggplot(aes(x = cut, y = V1)) +
  geom_bar(stat = "identity")
back[, mean(returns), by = cut(ratio, breaks = quantile(ratio, probs = seq(0.1, 1, 0.1), na.rm = TRUE))] |>
  ggplot(aes(x = cut, y = V1)) +
  geom_bar(stat = "identity")

# Insample backtest with thresholds
back = indicators[spy, on = "date"]
thresholds = seq(0, 2, by = 0.1)
results_i = list()
cols_vol_z = back[, colnames(.SD), .SDcols = patterns("vol_z_\\d+$")]
for (i in seq_along(cols_vol_z)) {
  var_ = cols_vol_z[i]
  print(var_)
  results = list()
  for (j in seq_along(thresholds)) {
    th = thresholds[j]
    back[, signal := ifelse(shift(get(var_)) > th, 0, 1)]
    back_ = back[, .(date, strategy = shift(signal) * returns, benchmark = returns)]
    setnames(back_, "strategy", paste0("strategy_", th))
    back_ = na.omit(back_)
    results[[j]] = back_
  }
  results = Reduce(function(x, y) merge(x, y[, 1:2], by = "date"), results)
  results = as.xts.data.table(results)
  sr = SharpeRatio.annualized(results)
  sr = as.data.table(melt(sr))
  results_i[[i]] = sr
}
names(results_i) = cols_vol_z
results_insample = rbindlist(results_i, idcol = "variable")
b = SharpeRatio.annualized(as.xts.data.table(na.omit(back)[, .(date, returns)]))
ggplot(results_insample, aes(x = value)) +
  geom_histogram() +
  geom_vline(xintercept = b, color = "red")
results_insample[order(value)]

# Insample backtest with quantiles
back = indicators[spy, on = "date"]
cols_vol_q = paste0(cols_vol_z, "_q")
back[, (cols_vol_q) := lapply(.SD, function(x) roll_quantile(x, width = length(x), min_obs = 252, p = c(0.9))),
     .SDcols = cols_vol_z]
results = list()
for (i in seq_along(cols_vol_q)) {
  var_ = back[, colnames(.SD), .SDcols = 2+i]
  varq_ = back[, colnames(.SD), .SDcols = 15+i]
  back[, signal := ifelse(shift(x) > shift(y), 0, 1), env = list(x = var_, y = varq_)]
  back_ = back[, .(date, strategy = shift(signal) * returns, benchmark = returns)]
  setnames(back_, "strategy", paste0("strategy_", stringr::str_extract(var_, "\\d+")))
  back_ = na.omit(back_)
  results[[i]] = back_
}
results = Reduce(function(x, y) merge(x, y[, 1:2], by = "date"), results)
results_xts = as.xts.data.table(results)
charts.PerformanceSummary(results_xts)
sr = SharpeRatio.annualized(results_xts)
as.data.table(melt(sr))[order(value)]

# Backtest with variable position sizing
cols_vol_z_min = paste0(cols_vol_z, "_min")
indicators[, (cols_vol_z_min) := lapply(.SD, function(x) roll_min(x, length(x), min_obs = 100)),
           .SDcols = cols_vol_z]
cols_vol_z_max = paste0(cols_vol_z, "_max")
indicators[, (cols_vol_z_max) := lapply(.SD, function(x) roll_max(x, length(x), min_obs = 100)),
           .SDcols = cols_vol_z]
back = indicators[spy, on = "date"]
back[, dist_ratio_22 := pmax(0, pmin(1, (vol_z_22 - vol_z_22_min)/(vol_z_22_max - vol_z_22_min)))]
back[, dist_ratio_66 := pmax(0, pmin(1, (vol_z_66 - vol_z_66_min)/(vol_z_66_max - vol_z_66_min)))]
back[, dist_ratio_125 := pmax(0, pmin(1, (vol_z_125 - vol_z_125_min)/(vol_z_125_max - vol_z_125_min)))]
back[, dist_ratio_252 := pmax(0, pmin(1, (vol_z_252 - vol_z_252_min)/(vol_z_252_max - vol_z_252_min)))]
back[, dist_ratio_504 := pmax(0, pmin(1, (vol_z_504 - vol_z_504_min)/(vol_z_504_max - vol_z_504_min)))]
results = list()
for (i in seq_along(cols_vol_z_max)) {
  var_ = back[, colnames(.SD), .SDcols = 2+i]
  dist_ratio_ = paste0("dist_ratio_", stringr::str_extract(cols_vol_z_max[i], "\\d+"))
  back[, leverage := 1 * (1 - x), env = list(x = dist_ratio_)]
  back[, leverage := round(leverage, 1)]
  setnafill(back, type = "locf", cols = "leverage")
  # back[shift(x, 1) > 0.5, leverage := 0, env = list(x = var_)]
  back_ = back[, .(date, strategy = shift(leverage) * returns, benchmark = returns)]
  setnames(back_, "strategy", paste0("strategy_", stringr::str_extract(var_, "\\d+")))
  results[[i]] = back_
}
results = Reduce(function(x, y) merge(x, y[, 1:2], by = "date"), results)
results_xts = as.xts.data.table(results)
charts.PerformanceSummary(results_xts)
sr = SharpeRatio.annualized(results_xts)
as.data.table(melt(sr))[order(value)]

# Insample golder rule
back = indicators[spy, on = "date"]
# back[, signal := ifelse(TTR::EMA(vol_z_22, 50) < TTR::EMA(vol_z_22, 200), 1, 0)] # 1)
# back[, signal := ifelse(shift(vol_z_22) < shift(vol_z_22, 66), 1, 0)]            # 2)
back[, signal := ifelse((TTR::EMA(vol_z_22, 50) < TTR::EMA(vol_z_22, 200)) | vol_z_22 < 0,
                        1, 0)] # 1)
back = back[, .(date, strategy = shift(signal, 1) * returns * 1.5, benchmark = returns)]
back_xts = na.omit(as.xts.data.table(back))
charts.PerformanceSummary(back_xts["2002/"])
sr = SharpeRatio.annualized(back_xts["2002/"])
as.data.table(melt(sr))[order(value)]
lapply(Drawdowns(back_xts["2002/"]), min)
Return.cumulative(back_xts["2002/"])
# charts.PerformanceSummary(back_xts["2024"])
# charts.PerformanceSummary(back_xts["2025"])

# Insample golder rule with VIX
back = macro_dt[series_id == "VIXCLS", .(date = date_real, vix = value)][spy, on = "date"]
back[, signal := ifelse(TTR::EMA(vix, 50) < TTR::EMA(vix, 200), 1, 0)]
back = back[, .(date, strategy = shift(signal, 1) * returns * 1, benchmark = returns)]
back_xts = na.omit(as.xts.data.table(back))
charts.PerformanceSummary(back_xts)
sr = SharpeRatio.annualized(back_xts)
as.data.table(melt(sr))[order(value)]
lapply(Drawdowns(back_xts), min)


# VAR ---------------------------------------------------------------------
# prepare dataset
back = indicators[spy, on = "date"]
keep_cols_var = colnames(back)[grep("vol_z_\\d+$|^z$", colnames(back))]
# keep_cols_var = c("pr_below_dummy_net_03974224", "pr_below_dummy_net_059540")
keep_cols_var = c("date", "returns", keep_cols_var)
X = back[, ..keep_cols_var]
X = na.omit(X)
head(X, 5)

# predictions for every period
roll_var = runner(
  x = X,
  f = function(x) {
    # x <- X[1:300, 1:ncol(X)]
    y = as.data.frame(x[, 2:ncol(x)])
    y = y[, which(apply(y, 2 , sd) != 0), drop = FALSE]
    if(length(y) == 1) print("STOP!")
    res = VAR(y, lag.max = 2, type = "both")
    # coef(res)
    # summary(res)
    p = predict(res)
    p_last = p$fcst[[1]][1, 1]
    data.frame(prediction = p_last)
  },
  # k = 252,
  lag = 0L,
  at = 100,
  na_pad = TRUE
)
predictions_var = lapply(roll_var, as.data.table)
predictions_var = rbindlist(predictions_var, fill = TRUE)
predictions_var = cbind(date = X[100:nrow(X), date], predictions_var)
predictions_var = merge(X, predictions_var, by = "date", all.x = TRUE, all.y = FALSE)
predictions_var = na.omit(predictions_var)
tail(predictions_var, 10)

# backtest apply
predictions_var[, signal := ifelse(shift(V1) > 0, 1, 0)]
predictions_var = predictions_var[, .(date, strategy = signal * returns, benchmark = returns)]
predictions_var = na.omit(predictions_var)
predictions_var_xts = as.xts.data.table(predictions_var)
charts.PerformanceSummary(predictions_var_xts)
SharpeRatio.annualized(predictions_var_xts)
lapply(Drawdowns(predictions_var_xts), min)


# TVAR --------------------------------------------------------------------
# prepare data2
back = indicators[spy, on = "date"]
colnames(back)[grep("vol_z_\\d+$|^z$", colnames(back))]
varvar = "vol_z_66"
cols = c("returns", varvar)
X_vars = as.data.frame(back[, ..cols])
head(X_vars)
tail(X_vars)
dim(X_vars)

# descriptive
acf(X_vars$returns)     # short memory
acf(na.omit(X_vars$vol_z_66)) # long memory

# window_lengths
window_lengths = c(252, 504, 1008)
cl = makeCluster(8)
clusterExport(cl, "X_vars", envir = environment())
clusterEvalQ(cl, {library(tsDyn)})
roll_preds = lapply(window_lengths, function(x) {
  runner(
    x = X_vars,
    f = function(x) {
      # debug
      # x = X_vars[1:500, ]

      # # TVAR (1)
      tv1 <- tryCatch(TVAR(data = x,
                           lag = 3,       # Number of lags to include in each regime
                           model = "TAR", # Whether the transition variable is taken in levels (TAR) or difference (MTAR)
                           nthresh = 2,   # Number of thresholds
                           thDelay = 1,   # 'time delay' for the threshold variable
                           trim = 0.05,   # trimming parameter indicating the minimal percentage of observations in each regime
                           mTh = 2,       # ombination of variables with same lag order for the transition variable. Either a single value (indicating which variable to take) or a combination
                           plot=FALSE),
                      error = function(e) NULL)
      if (is.null(tv1)) {
        tv1_pred_onestep <- NA
      } else {
        # tv1$coeffmat
        tv1_pred <- predict(tv1)[, 1]
        names(tv1_pred) <- paste0("predictions_", 1:5)
        thresholds <- tv1$model.specific$Thresh
        names(thresholds) <- paste0("threshold_", seq_along(thresholds))
        coef_1 <- tv1$coefficients$Bdown[1, ]
        names(coef_1) <- paste0(names(coef_1), "_bdown")
        coef_2 <- tv1$coefficients$Bmiddle[1, ]
        names(coef_2) <- paste0(names(coef_2), "_bmiddle")
        coef_3 <- tv1$coefficients$Bup[1, ]
        names(coef_3) <- paste0(names(coef_3), "_bup")
        cbind.data.frame(as.data.frame(as.list(thresholds)),
                         as.data.frame(as.list(tv1_pred)),
                         data.frame(aic = AIC(tv1)),
                         data.frame(bic = BIC(tv1)),
                         data.frame(loglik = logLik(tv1)),
                         as.data.frame(as.list(coef_1)),
                         as.data.frame(as.list(coef_2)),
                         as.data.frame(as.list(coef_3)))
      }
    },
    k = x,
    lag = 0L,
    cl = cl,
    na_pad = TRUE
  )
})
stopCluster(cl)
gc()

# save results
file_name = paste0("md_threshold2_", varvar, "_", format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S"), ".rds")
file_name = file.path("D:/features", file_name)
saveRDS(roll_preds, file_name)

# read results
# list.files("D:/features", pattern = "exuberv3_threshold2_kurtosis_bsadf_log_", full.names = TRUE)
# list.files("D:/features", pattern = "exuberv3_threshold2_sd", full.names = TRUE)
# # roll_preds <- readRDS("D:/features/exuberv3_threshold2_kurtosis_bsadf_log_20230711074304.rds")
# # roll_preds <- readRDS("D:/features/exuberv3_threshold2_sd_radf_sum_20230804080720.rds")
# length(roll_preds)
# roll_preds[[1]][[2000]]
# varvar = "sd_radf_sum"

# extract info from object
roll_results = lapply(roll_preds, function(x) lapply(x, as.data.table))
roll_results = lapply(roll_results, rbindlist, fill = TRUE)
roll_results[[1]]
cols = c("date", "returns", varvar)
tvar_res = lapply(roll_results, function(x){
  cbind(back[, ..cols], x)
})

# visualize
ggplot(tvar_res[[1]], aes(date)) +
  geom_line(aes(y = threshold_1), color = "green") +
  geom_line(aes(y = threshold_2), color = "red") +
  geom_line(aes(y = vol_z_66))
ggplot(tvar_res[[1]], aes(date)) +
  geom_line(aes(y = bic), color = "green")
ggplot(tvar_res[[1]][date %between% c("2020-01-01", "2022-10-01")], aes(date)) +
  geom_line(aes(y = threshold_1), color = "green") +
  geom_line(aes(y = threshold_2), color = "red") +
  geom_line(aes(y = vol_z_66))
ggplot(tvar_res[[1]][date %between% c("2022-01-01", "2025-01-01")], aes(date)) +
  geom_line(aes(y = threshold_1), color = "green") +
  geom_line(aes(y = threshold_2), color = "red") +
  geom_line(aes(y = vol_z_66))

# threshold based backtest
tvar_backtest = function(tvar_res_i) {
  # tvar_res_i = tvar_res[[2]]
  returns <- tvar_res_i$returns
  threshold_1 = tvar_res_i$threshold_1
  threshold_2 = tvar_res_i$threshold_2
  coef_1_up = tvar_res_i$sd_radf_sum..1_bup
  coef_1_down = tvar_res_i$sd_radf_sum..1_bdown
  coef_ret_1_up = tvar_res_i$returns..1_bup
  coef_ret_1_down = tvar_res_i$returns..1_bdown
  coef_ret_1_middle = tvar_res_i$returns..1_bmiddle
  coef_ret_2_up = tvar_res_i$returns..2_bup
  coef_ret_2_down = tvar_res_i$returns..2_bdown
  coef_ret_2_middle = tvar_res_i$returns..2_bmiddle
  coef_ret_3_up = tvar_res_i$returns..3_bup
  coef_ret_3_down = tvar_res_i$returns..3_bdown
  coef_ret_3_middle = tvar_res_i$returns..3_bmiddle
  predictions_1 = tvar_res_i$predictions_1
  aic_ = tvar_res_i$aic
  indicator = tvar_res_i$vol_z_66
  sides = vector("integer", length(predictions))
  sides = vector("integer", length(returns))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(threshold_2[i-1]) || is.na(threshold_2[i-2]) || is.na(indicator[i-1]) || is.na(coef_ret_1_up[i-1])) {
      print(i)
      sides[i] = NA
    } else if (indicator[i-1] > threshold_2[i-1] & coef_ret_1_up[i-1] < 0) {
      sides[i] = 0
    # } else if (indicator[i-1] > threshold_1[i-1] & coef_ret_1_middle[i-1] < 0) {
    #   sides[i] = 0
      # } else if (indicator[i-1] < threshold_1[i-1] & coef_ret_1_down[i-1] < 0 & coef_ret_2_down[i-1] < 0 & coef_ret_3_down[i-1] < 0) {
      #   sides[i] <- 0
    } else {
      sides[i] = 1
    }
  }
  sides <- ifelse(is.na(sides), 1, sides)
  returns_strategy = returns * sides
  returns_strategy
  # Return.cumulative(returns_strategy)
}
lapply(tvar_res, function(x) Return.cumulative(tvar_backtest(x)))
res_ = tvar_res[[2]]
returns_strategy = tvar_backtest(res_)
PerformanceAnalytics::Return.cumulative(returns_strategy)
portfolio = as.xts.data.table(res_[, .(date, returns, returns_strategy = returns_strategy)])
charts.PerformanceSummary(portfolio["2000/"])

