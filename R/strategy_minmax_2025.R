library(fastverse)
library(mlr3)
library(mlr3pipelines)
library(mlr3learners)
library(mlr3extralearners)
library(mlr3tuning)
library(paradox)
library(mlr3finance)
library(lubridate)
library(ggplot2)


# DATA --------------------------------------------------------------------
# Import backtest data
back = fread("D:/strategies/minmax/minmax_back.csv")
attr(back$date, "tz")
back[, date := with_tz(date, tzone = "America/New_York")]
setorder(back, date)

# Remove skew and kurtosis because of many NA values
cols_remove = colnames(back)[grepl("skew|kurto", colnames(back))]
back = back[, .SD, .SDcols = -cols_remove]

# Plots
dim(back)
colnames(back)[grepl("mean_excess_p_9", colnames(back))]
ggplot(back, aes(x = date, y = mean_excess_p_999_1764)) + geom_line() +
  geom_hline(yintercept = 0, linetype = "dashed", color = "red") +
  theme_minimal() + theme(legend.position = "bottom") +
  labs(title = "Mean excess returns above 99.9% percentile",
       x = "Date", y = "Mean excess returns")
dates_down = back[mean_excess_p_999_1764 < -0.001, date]
dates_up   = back[mean_excess_p_999_1764 > 0.001, date]
na.omit(back[, .(date, close, mean_excess_p_999_1764)]) |>
  ggplot(aes(x = date, y = close)) + geom_line() +
  geom_vline(xintercept = dates_down, linetype = "dashed", color = "red") +
  geom_vline(xintercept = dates_up, linetype = "dashed", color = "green") +
  theme_minimal() + theme(legend.position = "bottom") +
  labs(title = "SPY close price",
       x = "Date", y = "Close price")


# ML ----------------------------------------------------------------------
# Prepare task
taskml = na.omit(back)
dim(taskml)
taskml[, .(
  mindate = min(date),
  maxdate = max(date),
  nrows = .N
)]
taskml[, month := yearmon(as.Date(date))]
taskml[, target := shift(close, type = "lead") / close - 1]
hist(taskml$target, breaks = 100)
ids = c("date", "month")
predictors = colnames(taskml)[grepl("mean|sum|sd", colnames(taskml))]
cols_ = c(ids, "target", predictors)
task_minmax = as_task_regr(taskml[, ..cols_], id = "minmax", target = "target")
task_minmax$set_col_roles("month", "group")
task_minmax$set_col_roles("date", "order")

# Main graph
graph =
  po("removeconstants", id = "removeconstants_1", ratio = 0)  %>>%
  po("branch", options = c("noscale", "uniformization", "scale"), id = "scale_branch") %>>%
  gunion(list(
    po("nop", id = "nop_scale"),
    po("uniform"),
    po("scale")
  )) %>>%
  po("unbranch", id = "scale_unbranch") %>>%
  gunion(list(
    po("nop", id = "nop_union_pca"),
    po("pca", center = FALSE, rank. = 10),
    po("ica", n.comp = 10)
  )) %>>% po("featureunion", id = "feature_union_pca") %>>%
  po("removeconstants", id = "removeconstants_3", ratio = 0) %>>%
  gunion(
    list(
      ranger  = lrn("regr.ranger", id = "ranger"),
      xgboost = lrn("regr.xgboost", id = "xgboost"),
      nnet    = lrn("regr.nnet", id = "nnet", MaxNWts = 50000)
      # mlp     = mlp_graph
    )
  )%>>%
  po("regravg")
plot(graph)
graph_learner = as_learner(graph)
graph_learner$impute_selected_features = TRUE

#
# Search space
gp = as.data.table(graph$param_set)
search_space = ps(
  scale_branch.selection = p_fct(c("noscale", "uniformization", "scale")),
  ranger.ranger.max.depth  = p_int(1, 15),
  ranger.ranger.replace    = p_lgl(),
  ranger.ranger.mtry.ratio = p_dbl(0.3, 1),
  ranger.ranger.num.trees  = p_int(10, 2000),
  ranger.ranger.splitrule  = p_fct(levels = c("variance", "extratrees")),
  xgboost.xgboost.alpha     = p_dbl(0.001, 100, logscale = TRUE),
  xgboost.xgboost.max_depth = p_int(1, 20),
  xgboost.xgboost.eta       = p_dbl(0.0001, 1, logscale = TRUE),
  xgboost.xgboost.nrounds   = p_int(1, 5000),
  xgboost.xgboost.subsample = p_dbl(0.1, 1),
  nnet.nnet.size  = p_int(lower = 2, upper = 15),
  nnet.nnet.decay = p_dbl(lower = 0.0001, upper = 0.1),
  nnet.nnet.maxit = p_int(lower = 50, upper = 500)
  # mlp.torch_model_regr.batch_size = p_int(lower = 8, upper = 128),
  # mlp.torch_optimizer.lr = p_dbl(lower = 1e-4, upper = 1e-1, logscale = TRUE),
  # mlp.torch_model_regr.epochs = p_int(lower = 20, upper = 200)
)

# auto tuner rf
mlr_resamplings
at_graph = auto_tuner(
  tuner        = tnr("random_search"), # tnr("hyperband", eta = 3); terminator = trm("none")
  learner      = graph_learner,
  resampling   = rsmp("holdout_gap_ratio", gap = 1, ratio = 0.8),
  measure      = msr("regr.mse"),
  search_space = search_space,
  term_evals   = 2
)
at_graph$id = "finautoml"

# Create design
mlr3::mlr_resamplings
design = benchmark_grid(
  tasks = task_minmax,
  learners = list(at_graph),
  resamplings = rsmp("gap_cv", initial_window = 12, horizon = 1, gap = 0, step = 1, rolling = FALSE),
)

# Populate registry with problems and algorithms to form the jobs
benchmark_res = benchmark(design)

