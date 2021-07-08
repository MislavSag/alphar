library(fmpcloudr)
library(data.table)
library(Rcatch22)
library(httr)
library(mlr3verse)
library(mlr3extralearners)
library(future.apply)
library(PerformanceAnalytics)


# set fmpcloudr api token
API_KEY = "15cd5d0adf4bc6805a724b4417bbaafc"
fmpc_set_token(API_KEY)

# import mlr3 benchmark results
list.files('C:/Users/Mislav/Documents/GitHub/alphar/mlmodels')
model_file <- 'C:/Users/Mislav/Documents/GitHub/alphar/mlmodels/bmr_results-20210702-084320.rds'
bmr_results <- readRDS(model_file)
autoplot(bmr_results) + theme(axis.text.x = element_text(angle = 45, hjust = 1))
measures = list(
  msr("classif.acc", id = "acc_train", predict_sets = 'train'),
  msr("classif.acc", id = "acc_test", predict_sets = 'test')
)
best_models <- bmr_results$score(measures)[,best := acc_test == max(acc_test),  by = learner_id]
best_models <- best_models[best == 1]
best_models <- best_models[learner_id != 'classif.featureless']
feature_names <- best_models$task[[1]]$feature_names

# import new announcements
get_earnings_announcement <- function(from, to) {
  url <- "https://financialmodelingprep.com/api/v3/earning_calendar"
  p <- GET(url, query = list(from = from, to = to, apikey = API_KEY))
  result <- rbindlist(httr::content(p))
  return(result)
}
ea <- get_earnings_announcement(Sys.Date() - 30, Sys.Date())
ea <- ea[!is.na(ea$epsEstimated)]
ea[, date := as.Date(date)]
ea[, event_date := as.Date(date)]

# get transcripts
get_earning_call_transcript <- function(symbol, year = 2020, api_key) {
  url = paste0("https://financialmodelingprep.com/api/v4/batch_earning_call_transcript/", symbol)
  req <- content(GET(url, query = list(year = year, apikey = api_key)))
  result <- rbindlist(req)
}

# scrap data
transcripts <- lapply(unique(ea$symbol), function(tick) {
  print(tick)
  transcript_symbol <- lapply(2021, function(y) {
    get_earning_call_transcript(tick, y, API_KEY)
  })
  transcript_symbol <- rbindlist(transcript_symbol)
  transcript_symbol
})
transcripts <- rbindlist(transcripts)
transcripts[, datetime := as.POSIXct(date)]
transcripts[, date := as.Date(datetime)]
transcripts[, date_transcript := date] # we will need date which will be discard in merge
transcripts <- transcripts[!duplicated(transcripts[, .(symbol, date)])] # remove one duplicate
table(format.POSIXct(transcripts$datetime, "%H:%M:%S")) # frequenciese of transcript times; most are > 17:00

# price data
prices <- lapply(unique(ea$symbol), fmpc_price_history, startDate = Sys.Date() - 60)
prices <- rbindlist(prices)
prices[, returns := adjClose / data.table::shift(adjClose) - 1, by = symbol]

# create catch22 features
price_sybmols <- unique(prices$symbol)
catch22_vars<- list()
for (i in 1:length(price_sybmols)) {
  s <- price_sybmols[i]
  print(s)

  # data sample
  sample_ <- copy(prices)
  sample_ <- sample_[symbol == s]

  # create catch 22 features
  n <- 22
  sample_[, `:=`(
    CO_Embed2_Dist_tau_d_expfit_meandiff = frollapply(adjClose, n, Rcatch22::CO_Embed2_Dist_tau_d_expfit_meandiff),
    CO_f1ecac = frollapply(adjClose, n, Rcatch22::CO_f1ecac),
    CO_FirstMin_ac = frollapply(adjClose, n, Rcatch22::CO_FirstMin_ac),
    CO_HistogramAMI_even_2_5 = frollapply(adjClose, n, Rcatch22::CO_HistogramAMI_even_2_5),
    CO_trev_1_num = frollapply(adjClose, n, Rcatch22::CO_trev_1_num),
    DN_HistogramMode_10 = frollapply(adjClose, n, Rcatch22::DN_HistogramMode_10),
    DN_HistogramMode_5 = frollapply(adjClose, n, Rcatch22::DN_HistogramMode_5),
    DN_OutlierInclude_n_001_mdrmd = frollapply(adjClose, n, Rcatch22::DN_OutlierInclude_n_001_mdrmd),
    DN_OutlierInclude_p_001_mdrmd = frollapply(adjClose, n, Rcatch22::DN_OutlierInclude_p_001_mdrmd),
    FC_LocalSimple_mean1_tauresrat = frollapply(adjClose, n, Rcatch22::FC_LocalSimple_mean1_tauresrat),
    FC_LocalSimple_mean3_stderr = frollapply(adjClose, n, Rcatch22::FC_LocalSimple_mean3_stderr),
    IN_AutoMutualInfoStats_40_gaussian_fmmi = frollapply(adjClose, n, Rcatch22::IN_AutoMutualInfoStats_40_gaussian_fmmi),
    MD_hrv_classic_pnn40 = frollapply(adjClose, n, Rcatch22::MD_hrv_classic_pnn40),
    PD_PeriodicityWang_th0_01 = frollapply(adjClose, n, Rcatch22::PD_PeriodicityWang_th0_01),
    SB_BinaryStats_diff_longstretch0 = frollapply(adjClose, n, Rcatch22::SB_BinaryStats_diff_longstretch0),
    SB_BinaryStats_mean_longstretch1 = frollapply(adjClose, n, Rcatch22::SB_BinaryStats_mean_longstretch1),
    SB_MotifThree_quantile_hh = frollapply(adjClose, n, Rcatch22::SB_MotifThree_quantile_hh),
    SB_TransitionMatrix_3ac_sumdiagcov = frollapply(adjClose, n, Rcatch22::SB_TransitionMatrix_3ac_sumdiagcov),
    SC_FluctAnal_2_dfa_50_1_2_logi_prop_r1 = frollapply(adjClose, n, Rcatch22::SC_FluctAnal_2_dfa_50_1_2_logi_prop_r1),
    SC_FluctAnal_2_rsrangefit_50_1_logi_prop_r1 = frollapply(adjClose, n, Rcatch22::SC_FluctAnal_2_rsrangefit_50_1_logi_prop_r1),
    SP_Summaries_welch_rect_area_5_1 = frollapply(adjClose, n, Rcatch22::SP_Summaries_welch_rect_area_5_1),
    SP_Summaries_welch_rect_centroid = frollapply(adjClose, n, Rcatch22::SP_Summaries_welch_rect_centroid)
  )]

  # save
  cols <- c("symbol", "date",
            colnames(sample_)[which(colnames(sample_) == "CO_f1ecac"):ncol(sample_)])
  catch22_vars[[i]] <- sample_[, ..cols]
}
catch_22_features <- rbindlist(catch22_vars)

# get financial growth ratios
get_fundamental_data <- function(symbol, api_tag) {
  url <- paste0("https://financialmodelingprep.com/api/v3/", api_tag, "/", symbol)
  req <- GET(url, query = list(period = "quarter", limit = 1, apikey = API_KEY))
  fdata <- rbindlist(content(req))
  return(fdata)
}

# get income statement reports
get_pl <- function(symbol) {
  url <- paste0("https://financialmodelingprep.com/api/v3/income-statement/", symbol)
  req <- GET(url, query = list(period = "quarter", limit = 1, apikey = API_KEY))
  fdata <- rbindlist(content(req))
  return(fdata)
}

# get fundamental data
pl <- lapply(unique(ea$symbol), get_pl)
pl_dt <- rbindlist(pl)

# key metrics
metrics <- lapply(unique(ea$symbol), function(x) get_fundamental_data(x, "key-metrics"))
metrics_dt <- rbindlist(metrics)

# financial growth
fg <- lapply(unique(ea$symbol), function(x) get_fundamental_data(x, "financial-growth"))
fg_dt <- rbindlist(fg)



# CREATE DATA SET ---------------------------------------------------------

# merge events and transcripts
DT <- transcripts[ea, on = c(symbol = "symbol", date = "date"), roll = "nearest"]
DT[, .(symbol, date, date_transcript)]  # view merge. Transcript date >= date
DT[date_transcript < date, `:=`(content = NA, date_transcript = NA)] # transcript date can't be before earnings announcement date (?)
DT[, date_diff := date_transcript - date] # diff between earning report date and transcript call date
DT[date_diff > 10, `:=`(content = NA, date_transcript = NA)] # remove if transcript is delayed by more than 10 days
DT[!is.na(content) & duplicated(content), `:=`(content = NA, date_transcript = NA)] # duplicated transcripts to NA
DTT <- DT[!is.na(content)]
str(DT)

# features from prices
indicators <- copy(prices)
setorderv(indicators, c("symbol", "date"))

# catch22 features
indicators <- catch_22_features[indicators, on = c(symbol = "symbol", date = "date")]

# indicators
close_ath_percente <- function(close) {
  cl_ath <- cummax(close)
  cl_ath_dev <- (cl_ath - close) / cl_ath
  return(cl_ath_dev)
}
indicators[, `:=`(
  returns_week = frollsum(returns, 5, na.rm = TRUE),
  returns_month = frollsum(returns, 22, na.rm = TRUE),
  volume_week = frollmean(volume, 5, na.rm = TRUE),
  volume_month = frollmean(volume, 22, na.rm = TRUE),
  close_ath = close_ath_percente(adjClose),
  # rsi_month = RSI(adjClose, 22),
  # macd = MACD(adjClose, 22)[, "macd"],
  # macdsignal = MACD(adjClose, 22)[, "signal"],
  std_week = roll::roll_sd(returns, 5),
  std_month = roll::roll_sd(returns, 22),
  # skew_month = as.vector(RollingWindow::RollingSkew(returns, window = 22, na_method = 'ignore')),
  kurt_month = as.vector(RollingWindow::RollingKurt(returns, window = 22, na_method = 'ignore'))
), by = symbol]
A <- indicators[DT, on = c(symbol = "symbol", date = "date"), roll = -Inf]
A[, .(symbol, date, datetime, date_transcript)]

# actual vs estimated
A[, `:=`(
  eps_diff = (eps - epsEstimated) / adjClose,
  rev_diff = (revenue - revenueEstimated) / adjClose
)]
setorderv(A, c("symbol", "date"))

# fundamental data
reports <- copy(pl_dt)
reports[, `:=`(date = as.Date(date),
               fillingDate = as.Date(fillingDate),
               acceptedDateTime = as.POSIXct(acceptedDate, format = "%Y-%m-%d %H:%M:%S"),
               acceptedDate = as.Date(acceptedDate, format = "%Y-%m-%d %H:%M:%S"))]
fin_growth <- copy(fg_dt)
fin_growth[, date := as.Date(date)]
fin_ratios <- copy(metrics_dt)
fin_ratios[, date := as.Date(date)]
fundamentals <- merge(reports, fin_growth, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE)
fundamentals <- merge(fundamentals, fin_ratios, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE)
fundamentals <- fundamentals[date > as.Date("1998-01-01")]
fundamentals[, acceptedDateFundamentals := acceptedDate]
data.table::setnames(fundamentals, "date", "fundamental_date")
str(fundamentals)
fundamentals[, .(ebitgrowth, acceptedDateFundamentals, fillingDate)]

# A <- fundamentals[, .(symbol, acceptedDate, ebitgrowth)][A[, .(symbol, date, volume_month)], on = c(symbol = "symbol", "acceptedDate" = "date")]
A <- fundamentals[A, on = c(symbol = "symbol", "acceptedDate" = "date")]
A <- A[!is.na(ebitgrowth)]
A[, .(symbol, event_date, acceptedDate, fillingDate)]



# PREPARE DATA FOR ML AND PREDICT -----------------------------------------

# test data for prediciton
feature_names[!(feature_names %in% colnames(A))]
predict_data <- na.omit(A, cols = feature_names)
predict_data[, .(symbol, event_date, fundamental_date, fillingDate, acceptedDate)]

# add quasi true labels
trues <- vector("integer", nrow(predict_data))
for (i in seq_along(trues)) {
  print(i)
  sample_ <- prices[symbol == predict_data[i, symbol] & date %between% c(predict_data[i, event_date] + 1, Sys.Date())]
  sample_ <- na.omit(sample_[, returns := adjClose / data.table::shift(adjClose) - 1, by = symbol])
  cum_returns <- Return.cumulative(sample_$returns)
  trues[i] <- ifelse(cum_returns > 0, 1, -1)
}
predict_data[, best_models$task[[1]]$target_names := as.factor(trues)]
predict_data <- cbind(predict_data[, .(symbol, event_date, bin)], predict_data[, ..feature_names])
str(predict_data[, .(symbol, event_date, bin)])


# make predictions
task_pred = TaskClassif$new(predict_data[, .SD, .SDcols = !c("event_date")], id = "mlfin_holdout", target = "bin")
task_pred$set_col_roles("symbol", roles = "name")
predictions <- lapply(best_models$learner, function(x) {
  y <- as.data.table(x$learner$predict_newdata(task_pred$data()))
  colnames(y) <- paste(colnames(y), x$id, sep = "_")
  y
})
predictions <- do.call(cbind, predictions)
colnames(predictions)[2] <- "truth"
predictions[, grep("row_ids|truth_", colnames(predictions)) := NULL]
predictions <- cbind(predict_data[, .(symbol, event_date)], predictions)
setorder(predictions, "event_date")


# compare with true
holdout_set <- data.frame(truth = as.factor(predictions$truth),
                          res = predictions$response_classif.ranger.tuned)
holdout_set <- holdout_set[holdout_set$res != 0, ]
holdout_set$truth <- droplevels(holdout_set$truth)
holdout_set$res <- droplevels(holdout_set$res)
mlr3measures::confusion_matrix(holdout_set$truth, holdout_set$res, "1")
cols <- c(1:3, 8:11)
predictions[truth == 1, ..cols]



