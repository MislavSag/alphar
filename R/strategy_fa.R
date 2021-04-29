library(data.table)
library(dplyr)
library(fmpcloudr)
library(simfinapi)
library(lubridate)
library(roll)
library(xts)
library(ggplot2)
library(naniar)     # handling missing values
library(DescTools)
library(skimr)
library(TSPred)
source('C:/Users/Mislav/Documents/GitHub/alphar/R/import_data.R')



# set api token
APIKEY = "15cd5d0adf4bc6805a724b4417bbaafc"
fmpc_set_token(APIKEY)

# sp500 symbols
sp500_symbols <- import_sp500()

# utils
future_return <- function(x, n) {
  (data.table::shift(x, n, type = 'lead') - x) / x
}
trading_days_year <- 256
trading_days_halfyear <- trading_days_year / 2
trading_days_q <- trading_days_year / 4

# market capitalization
market_cap <- import_daily(path = 'D:/fundamental_data/market_cap', extension = 'csv')

# import daily data
prices <- import_daily(path = 'D:/market_data/equity/usa/day/trades', extension = 'csv')
prices <- market_cap[prices, on = c('symbol', 'date'), roll = -Inf]
prices[, `:=`(year = year(date), month = month(date))]

# Average volumes
prices[, Advt_12M_Usd := frollmean(volume, trading_days_year, na.rm = TRUE), by = .(symbol)]
prices[, Advt_6M_Usd := frollmean(volume, trading_days_halfyear, na.rm = TRUE), by = .(symbol)]
prices[, Advt_3M_Usd := frollmean(volume, trading_days_q, na.rm = TRUE), by = .(symbol)]

# Rolling volatility
prices[, Vol1Y_Usd := roll_sd(adjClose, trading_days_year), by = list(symbol)]
prices[, Vol3Y_Usd := roll_sd(adjClose, trading_days_year * 3), by = list(symbol)]
tail(prices[, .(date, close, Vol1Y_Usd, Vol3Y_Usd)])

# Average market capitalization
prices[, Mkt_Cap_12M_Usd := frollmean(as.numeric(marketCap), trading_days_year, na.rm = TRUE), by = list(symbol)]
prices[, Mkt_Cap_6M_Usd := frollmean(as.numeric(marketCap), trading_days_halfyear, na.rm = TRUE), by = list(symbol)]
prices[, Mkt_Cap_3M_Usd := frollmean(as.numeric(marketCap), trading_days_q, na.rm = TRUE), by = list(symbol)]
tail(prices[, .(date, marketCap, Mkt_Cap_12M_Usd, Mkt_Cap_6M_Usd, Mkt_Cap_3M_Usd)], 10)

# filter last observation in month and convert it to end of mont date
prices <- prices[, .SD[which.max(date)] , by = .(symbol, year, month)]
prices[, date := ceiling_date(date, "month") - days(1)]

# Momentum
prices[, Mom_11M_Usd := data.table::shift(close, 12)/data.table::shift(close, 1)-1, by = list(symbol)]
prices[, Mom_5M_Usd := data.table::shift(close, 5)/data.table::shift(close, 1)-1, by = list(symbol)]
head(prices[, c('date', 'close', 'Mom_11M_Usd', 'Mom_5M_Usd')], 15)

# Labels
prices <- prices[,`:=`(R1M_Usd = future_return(adjClose, 1),
                       R3M_Usd = future_return(adjClose, 3),
                       R6M_Usd = future_return(adjClose, 6),
                       R12M_Usd = future_return(adjClose, 12)), by = .(symbol)]
head(prices[, .(date, adjClose, R1M_Usd, R3M_Usd, R6M_Usd, R12M_Usd)])

# labels for classification
prices[, `:=`(R1M_Usd_C = as.factor(R1M_Usd > median(R1M_Usd, na.rm = TRUE)),
              R12M_Usd_C = as.factor(R12M_Usd > median(R12M_Usd, na.rm = TRUE))), by = date]

# import balance sheet data
balance <- import_daily(path = "D:/fundamental_data/balance_sheet", extension = 'csv')
balance <- as.data.table(balance)

# import ratios
ratios <- import_daily(path = "D:/fundamental_data/ratios", extension = 'csv')
ratios <- as.data.table(ratios)
ratios <- ratios[order(symbol, date)]

# ratios
ratios[, price := pbRatio * bookValuePerShare]
ratios[, eps_ttm := frollsum(netIncomePerShare, 4), by = .(symbol)]
ratios[, pe_ttm := price / eps_ttm]

# merge ratios and financial statements
ratios <- balance[ratios, on = .(symbol, date)]

# merge ratios and prices
ratios[, date_end_month := ceiling_date(fillingDate, "month") - days(1)]
data <- merge(prices, ratios, by.x = c('symbol', 'date'), by.y = c('symbol', 'date_end_month'), all.x = TRUE, all.y = FALSE)

# define features and labels
labels_reg <- c("R1M_Usd", "R3M_Usd", "R6M_Usd", "R12M_Usd")
labels_clf <- c("R1M_Usd_C", "R12M_Usd_C")
features <- c(
  "Advt_3M_Usd", "Advt_6M_Usd", "Advt_12M_Usd",
  "bookValuePerShare",
  "cashPerShare", "dividendYield", "eps_ttm",
  "Mkt_Cap_3M_Usd", "Mkt_Cap_6M_Usd", "Mkt_Cap_12M_Usd",
  "Vol1Y_Usd", "Vol3Y_Usd",
  "enterpriseValue",
  "freeCashFlowPerShare",
  "debtToEquity",
  "Mom_5M_Usd", "Mom_11M_Usd",
  # 'netDebtToEBITDA',
  "netIncomePerShare",
  "operatingCashFlowPerShare",
  "pbRatio", "pe_ttm",
  "revenuePerShare",
  # non ml factor features
  "tangibleBookValuePerShare"
)



# PREPARE DATA ------------------------------------------------------------

# final table
cols <- c("symbol", "date", features, labels_reg, labels_clf)
DT <- data[, ..cols]
variables <- colnames(DT)[3:ncol(DT)]
DT <- DT[, (variables) := lapply(.SD, function(x) na.locf(x, na.rm = FALSE)), by = .(symbol), .SDcols = variables]
tail(DT, 20)

# Keep the sample with sufficient data points
DT <- DT[date %between% c('2000-01-01', '2020-01-01')]

# missing values
gg_miss_var(DT, show_pct = TRUE)
# vis_miss(DT, warn_large_data = FALSE)
DT <- DT[rowSums(is.na(DT[, 4:ncol(DT)])) != (ncol(DT) - 3)] # remove rows where all NA
gg_miss_var(DT, show_pct = TRUE)
fundamental_cols <- c('dividendYield', 'operatingCashFlowPerShare', 'pbRatio', 'eps_ttm')
DT <- DT[rowSums(is.na(DT[, ..fundamental_cols])) != length(fundamental_cols)] # remove if all fundamental missing
gg_miss_var(DT, show_pct = TRUE)
market_data_cols <- c('Mkt_Cap_12M_Usd', 'Mom_11M_Usd', 'Vol1Y_Usd')
DT <- DT[rowSums(is.na(DT[, ..market_data_cols])) != length(market_data_cols)] # remove if all fundamental missing
gg_miss_var(DT, show_pct = TRUE)
DT[, dividendYield := ifelse(is.na(dividendYield), 0, dividendYield)]
gg_miss_var(DT, show_pct = TRUE)
sum(is.na(DT$Vol3Y_Usd))
sum(is.na(DT$Mkt_Cap_12M_Usd))
DT[is.na(Vol3Y_Usd)]
DT <- DT[!is.na(Vol3Y_Usd)]
gg_miss_var(DT, show_pct = TRUE)
DT <- DT[!is.na(eps_ttm)]
gg_miss_var(DT, show_pct = TRUE)

# outliers and incorrect values
# skim(DT)
hist(DT$operatingCashFlowPerShare)
DT[, enterpriseValue := as.numeric(enterpriseValue)]
DT <- DT[, (features) := lapply(.SD, Winsorize, probs = c(0.02, 0.98)), by = date, .SDcols = features] # across dates
# skim(DT)

# uniformization
DT <- DT[, (features) := lapply(.SD, function(x) percent_rank(x)), by = date, .SDcols = features] # across dates



# EXPLORE DATA ------------------------------------------------------------


# number of firms
DT %>%
  dplyr::group_by(date) %>%                                   # Group by date
  dplyr::summarize(nb_assets = symbol %>%                   # Count nb assets
                     as.factor() %>% nlevels()) %>%
  ggplot(aes(x = date, y = nb_assets)) + geom_col()

# uniformized featuers
DT %>%
  filter(date == "2019-01-31") %>%
  ggplot(aes(x = pbRatio)) + geom_histogram(bins = 100)




# ML MODELS ---------------------------------------------------------------

# # training / test set
# separation_date <- as.Date("2016-01-15")
# training_sample <- DT[date < separation_date]
# testing_sample <- DT[date >= separation_date]
#
# # random forest
# library(randomForest)
# formula <- paste("R1M_Usd_C ~", paste(features, collapse = " + ")) # Defines the model
# formula <- as.formula(formula)                                   # Forcing formula object
# fit_RF_C <- randomForest(formula,         # New formula!
#                          data = training_sample,    # Data source: training sample
#                          sampsize = 20000,          # Size of (random) sample for each tree
#                          replace = FALSE,           # Is the sampling done with replacement?
#                          nodesize = 250,            # Minimum size of terminal cluster
#                          ntree = 40,                # Number of random trees
#                          mtry = 10                  # Number of predictive variables for each tree
# )
# mean(predict(fit_RF_C, testing_sample) == testing_sample$R1M_Usd_C) # Hit ratio




# MLR3 APPROACH -----------------------------------------------------------

library(mlr3verse)
library(mlr3forecasting)
library(mlr3viz)
library(mlr3fselect)
library(mlr3extralearners)
library(mlr3misc)
library(future.apply)

# mlr3 task
DT_sample <- DT[, c(1:25, 30)]
setorder(DT_sample, date, -Mom_5M_Usd)
biggest <- DT_sample[, head(.SD, 300), by = date]
lowest <- DT_sample[, tail(.SD, 300), by = date]
DT_sample <- rbind(biggest, lowest)
DT_sample <- DT_sample[order(symbol, date)]
DT_sample[, `:=`(symbol = NULL, date = NULL)]
DT_sample$R1M_Usd_C <- as.factor(ifelse(DT_sample$R1M_Usd_C == TRUE, 1, 0))

# task
task = TaskClassif$new(id = 'mlfin', backend = DT_sample, target = 'R1M_Usd_C', positive = '1')

# remove missing values
task$filter(which(complete.cases(task$data())))

# mlr outer resmaling, main measure, terminators, type of search
# same for all models!
resampling = rsmp('forecastHoldout', ratio = 0.7)
measure = msr("classif.acc")
terminator = trm('evals', n_evals = 30)
tuner = tnr("grid_search", resolution = 1)

# filters
filter_features <- function(filters) {
  filters = flts(filters)
  filtered_features <- rbindlist(lapply(filters, function(x) {
    flt <- x$calculate(task)
    flt_table <- as.data.table(flt)[, `:=`(id = flt$id, package = flt$packages)]
  })
  )
  filtered_features <- filtered_features[, sum(score), by = c('feature')]
  filtered_features <- filtered_features[order(V1, decreasing = TRUE)]
  filtered_features[, rank := 1:.N][]
}
filtered_features_praznik <- filter_features(c("disr", "jmim", "jmi", "mim", "mrmr", "njmim", "cmim"))
# filtered_features_relief <- filter_features(c("relief", "information_gain"))

# selection
learners <- list(lrn("classif.randomForest"), lrn("classif.naive_bayes"),
                 lrn("classif.ranger"), lrn('classif.xgboost'), lrn('classif.rpart'))
fselector = fs("random_search")
autofs <- lapply(learners, function(x) {
  AutoFSelector$new(x, resampling, measure, terminator, fselector)
})

# set benchmark
design = benchmark_grid(
  task = task,
  learner = autofs,
  resampling = rsmp("cv", folds = 5)
)
bmr_fs = benchmark(design, store_models = TRUE)

# extract 10 most important features
bmr_fs$aggregate(c(msrs("classif.acc"), msrs("classif.ce")))
important_features <- lapply(bmr_fs$data$learners()$learner, function(x) unlist(x$fselect_result$features))
important_features <- as.data.table(table(unlist(important_features)))
important_features <- important_features[order(N, decreasing = TRUE), ]
important_features <- important_features[N > (length(autofs) * 5 / 3), ] # 5 is number of folds
setnames(important_features, 'V1', 'feature')
most_important_features <- fintersect(filtered_features_praznik[1:10, 1], important_features[, .(feature)])

# choose only most important features in the task
task$select(most_important_features[[1]])




# TRAIN ML MODELS WITH MOST IMPORTANT FEATURES ----------------------------

# functoin for constracting autotuners
make_auto_tuner <- function(learner, params) {
  AutoTuner$new(
    learner = learner,
    resampling = resampling,
    measure = measure,
    search_space = params,
    terminator = terminator,
    tuner = tuner,
    store_models = TRUE
  )
}

# random forest
learner = lrn("classif.ranger", predict_type = "prob", predict_sets = c("train", "test"))
params = ParamSet$new(
  params = list(
    ParamInt$new("max.depth", lower = 2, upper = 6),
    ParamInt$new("min.node.size", lower = 3, upper = 6)

  ))
rf = make_auto_tuner(learner, params)

# xgboost
learner = lrn("classif.xgboost", predict_type = "prob", predict_sets = c("train", "test"))
params = ParamSet$new(
  params = list(
    ParamInt$new("max_depth", lower = 2, upper = 6),
    ParamDbl$new("colsample_bytree", lower = 0.1, upper = 0.7)

  ))
xgboost = make_auto_tuner(learner, params)

# neural net
learner = lrn("classif.nnet", predict_type = "prob", predict_sets = c("train", "test"))
params = ParamSet$new(
  params = list(
    ParamInt$new("size", lower = 2, upper = 6),
    ParamDbl$new("decay", lower = 0, upper = 0.001),
    ParamInt$new("maxit", lower = 200, upper = 400)
  ))
nnet = make_auto_tuner(learner, params)

# glmnet
learner = lrn("classif.glmnet", predict_type = "prob", predict_sets = c("train", "test"))
params = ParamSet$new(
  params = list(
    ParamInt$new("alpha", lower = 0, upper = 1)
  ))
glmnet = make_auto_tuner(learner, params)


# adaboostm1
# learner = lrn("classif.AdaBoostM1", predict_type = "prob", predict_sets = c("train", "test"))
# rf_params = ParamSet$new(
#   params = list(
#     ParamInt$new("I", lower = 10, upper = 20)
#
#   ))
# adaboostm1 = AutoTuner$new(
#   learner = learner,
#   resampling = resampling,
#   measure = measure,
#   search_space = rf_params,
#   terminator = terminator,
#   tuner = tuner,
#   store_models = TRUE
# )
# learner$param_set

# learners used for benchmarking
featureless = lrn("classif.featureless", predict_type = "prob", predict_sets = c("train", "test"))

# set benchmark
design = benchmark_grid(
  task = task,
  learner = list(rf, xgboost, nnet, glmnet, featureless),
  resampling = rsmp("cv", folds = 5)
)
bmr = mlr3::benchmark(design = design, store_models = TRUE)



# INSPECT RESULTS AND SAVE ------------------------------------------------

# results by mean
bmr_results <- bmr$clone(deep = TRUE)
measures = list(
  msr("classif.acc", id = "acc_train", predict_sets = 'train'),
  msr("classif.acc", id = "acc_test", predict_sets = 'test'),
  msr("classif.auc", id = "auc_train", predict_sets = 'train'),
  msr("classif.auc", id = "auc_test", predict_sets = 'test')

)
bmr_results$aggregate(measures)
tab = bmr_results$aggregate(measures)
ranks = tab[, .(learner_id, rank_train = rank(-auc_train), rank_test = rank(-auc_test)), by = task_id]
ranks = ranks[, .(mrank_train = mean(rank_train), mrank_test = mean(rank_test)), by = learner_id]
ranks[order(mrank_test)]
autoplot(bmr_results) + theme(axis.text.x = element_text(angle = 45, hjust = 1))
autoplot(bmr_results$clone(deep = TRUE)$filter(task_id = "mlfin"), type = "roc") # doesnt work

# result for specific model
choose_model <- 'classif.ranger.tuned'
measures = list(
  msr("classif.acc", id = "acc_train", predict_sets = 'train'),
  msr("classif.acc", id = "acc_test", predict_sets = 'test'),
  msr("classif.auc", id = "auc_train", predict_sets = 'train'),
  msr("classif.auc", id = "auc_test", predict_sets = 'test')
)
best_submodel <- bmr$score(measures)[learner_id == 'classif.ranger.tuned', ][acc_test == max(acc_test)]$learner[[1]]

# save
file_name <- paste0('mlmodels/', gsub('\\.', '_', choose_model), '_', best_submodel$hash, '.rds')
saveRDS(best_submodel, file = file_name)


# TEST --------------------------------------------------------------------

# x <- fmpc_financial_bs_is_cf('FMC', limit = 20000)
# x <-  fmpc_financial_metrics('FMC', metric = 'ratios', quarterly = TRUE, limit = 20000)
# x <- as.data.table(x)
# x <- x[, .(symbol, date, dividendYield)]
# head(x, 50)
#
# # simfin+
# statements <- sfa_get_statement(Ticker = 'YUM',
#                                 statement = 'derived',
#                                 period = 'quarters',
#                                 ttm = TRUE,
#                                 api_key = "8qk9Xc9scFc0Rbpfrx6PLdaiomvi7Dxc")
# prices <- sfa_get_prices(Ticker = 'BA',
#                          ratios = TRUE,
#                          api_key = "8qk9Xc9scFc0Rbpfrx6PLdaiomvi7Dxc")
# prices[Date %between% c('2019-02-01', '2019-04-05'), c('Ticker', 'Date', "Price to Book Value (ttm)")]
