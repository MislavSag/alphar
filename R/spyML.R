library(batchtools)
library(mlr3batchmark)
library(mlr3verse)




# IMPORT DATA -------------------------------------------------------------



# TASKS -------------------------------------------------------------------
# ID columns we always keep
id_cols = c("date", "target")

# Create task
cols_ = c(id_cols, cols_features)
task = as_task_regr(DT[, ..cols_], id = "pre", target = "target")

# Set roles for id columns
task$col_roles$feature = setdiff(task$col_roles$feature, id_cols)


# LEARNERS ----------------------------------------------------------------
# Import pipeops
mlr_pipeops$add("filter_target", finautoml::PipeOpFilterRegrTarget)
mlr_filters$add("gausscov_f1st", finautoml::FilterGausscovF1st)
mlr_pipeops$add("dropna", finautoml::PipeOpDropNA)
mlr_pipeops$add("dropnacol", finautoml::PipeOpDropNACol)
mlr_pipeops$add("dropcorr", finautoml::PipeOpDropCorr)
mlr_pipeops$add("uniformization", finautoml::PipeOpUniform)
mlr_pipeops$add("winsorizesimple", finautoml::PipeOpWinsorizeSimple)

# graph templates
gr = gunion(list(
  po("nop", id = "nop_union_pca"),
  po("pca", center = FALSE, rank. = 50),
  po("ica", n.comp = 10)
)) %>>% po("featureunion", id = "feature_union_pca")
filter_target_gr = po("branch",
                      options = c("nop_filter_target", "filter_target_select"),
                      id = "filter_target_branch") %>>%
  gunion(list(
    po("nop", id = "nop_filter_target"),
    po("filter_target", id = "filter_target_id")
  )) %>>%
  po("unbranch", id = "filter_target_unbranch")
# create mlr3 graph that takes 3 filters and union predictors from them
filters_ = list(
  po("filter", flt("disr"), filter.nfeat = 5),
  po("filter", flt("jmim"), filter.nfeat = 5),
  po("filter", flt("jmi"), filter.nfeat = 5),
  po("filter", flt("mim"), filter.nfeat = 5),
  po("filter", flt("mrmr"), filter.nfeat = 5),
  po("filter", flt("njmim"), filter.nfeat = 5),
  po("filter", flt("cmim"), filter.nfeat = 5),
  po("filter", flt("carscore"), filter.nfeat = 5), # UNCOMMENT LATER< SLOWER SO COMMENTED FOR DEVELOPING
  po("filter", flt("information_gain"), filter.nfeat = 5),
  po("filter", filter = flt("relief"), filter.nfeat = 5),
  po("filter", filter = flt("gausscov_f1st"), p0 = 0.25, filter.cutoff = 0)
  # po("filter", mlr3filters::flt("importance", learner = mlr3::lrn("classif.rpart")), filter.nfeat = 10, id = "importance_1"),
  # po("filter", mlr3filters::flt("importance", learner = lrn), filter.nfeat = 10, id = "importance_2")
)
graph_filters = gunion(filters_) %>>%
  po("featureunion", length(filters_), id = "feature_union_filters")
graph_template =
  filter_target_gr %>>%
  po("subsample") %>>% # uncomment this for hyperparameter tuning
  po("dropnacol", id = "dropnacol", cutoff = 0.05) %>>%
  po("dropna", id = "dropna") %>>%
  po("removeconstants", id = "removeconstants_1", ratio = 0)  %>>%
  po("fixfactors", id = "fixfactors") %>>%
  po("winsorizesimple", id = "winsorizesimple", probs_low = 0.01, probs_high = 0.99, na.rm = TRUE) %>>%
  po("removeconstants", id = "removeconstants_2", ratio = 0)  %>>%
  po("dropcorr", id = "dropcorr", cutoff = 0.99) %>>%
  po("branch", options = c("uniformization", "scale"), id = "scale_branch") %>>%
  gunion(list(po("uniformization"),
              po("scale")
  )) %>>%
  po("unbranch", id = "scale_unbranch") %>>%
  po("dropna", id = "dropna_v2") %>>%
  # add pca columns
  gr %>>%
  graph_filters %>>%
  po("removeconstants", id = "removeconstants_3", ratio = 0)

# hyperparameters template
as.data.table(graph_template$param_set)[1:100]
plot(graph_template)
winsorize_sp =  c(0.999, 0.99, 0.98, 0.97, 0.90, 0.8)
search_space_template = ps(
  # filter target
  filter_target_branch.selection = p_fct(levels = c("nop_filter_target", "filter_target_select")),
  filter_target_id.q = p_fct(levels = c("0.4", "0.3", "0.2"),
                             trafo = function(x, param_set) return(as.double(x)),
                             depends = filter_target_branch.selection == "filter_target_select"),
  # subsample for hyperband
  subsample.frac = p_dbl(0.7, 1, tags = "budget"), # commencement this if we want to use hyperband optimization
  # preprocessing
  dropcorr.cutoff = p_fct(c("0.80", "0.90", "0.95", "0.99"),
                          trafo = function(x, param_set) return(as.double(x))),
  winsorizesimple.probs_high = p_fct(as.character(winsorize_sp),
                                     trafo = function(x, param_set) return(as.double(x))),
  winsorizesimple.probs_low = p_fct(as.character(1-winsorize_sp),
                                    trafo = function(x, param_set) return(as.double(x))),
  # scaling
  scale_branch.selection = p_fct(levels = c("uniformization", "scale"))
)

# random forest graph
graph_rf = graph_template %>>%
  po("learner", learner = lrn("regr.ranger"))
graph_rf = as_learner(graph_rf)
as.data.table(graph_rf$param_set)[, .(id, class, lower, upper, levels)]
search_space_rf = search_space_template$clone()
search_space_rf$add(
  ps(regr.ranger.max.depth  = p_int(1, 15),
     regr.ranger.replace    = p_lgl(),
     regr.ranger.mtry.ratio = p_dbl(0.3, 1),
     regr.ranger.num.trees  = p_int(10, 2000),
     regr.ranger.splitrule  = p_fct(levels = c("variance", "extratrees")))
)

# xgboost graph
graph_xgboost = graph_template %>>%
  po("learner", learner = lrn("regr.xgboost"))
plot(graph_xgboost)
graph_xgboost = as_learner(graph_xgboost)
as.data.table(graph_xgboost$param_set)[grep("depth", id), .(id, class, lower, upper, levels)]
search_space_xgboost = ps(
  # filter target
  filter_target_branch.selection = p_fct(levels = c("nop_filter_target", "filter_target_select")),
  filter_target_id.q = p_fct(levels = c("0.4", "0.3", "0.2"),
                             trafo = function(x, param_set) return(as.double(x)),
                             depends = filter_target_branch.selection == "filter_target_select"),
  # subsample for hyperband
  subsample.frac = p_dbl(0.7, 1, tags = "budget"), # commencement this if we want to use hyperband optimization
  # preprocessing
  dropcorr.cutoff = p_fct(c("0.80", "0.90", "0.95", "0.99"),
                          trafo = function(x, param_set) return(as.double(x))),
  winsorizesimple.probs_high = p_fct(as.character(winsorize_sp),
                                     trafo = function(x, param_set) return(as.double(x))),
  winsorizesimple.probs_low = p_fct(as.character(1-winsorize_sp),
                                    trafo = function(x, param_set) return(as.double(x))),
  # scaling
  scale_branch.selection = p_fct(levels = c("uniformization", "scale")),
  # learner
  regr.xgboost.alpha     = p_dbl(0.001, 100, logscale = TRUE),
  regr.xgboost.max_depth = p_int(1, 20),
  regr.xgboost.eta       = p_dbl(0.0001, 1, logscale = TRUE),
  regr.xgboost.nrounds   = p_int(1, 5000),
  regr.xgboost.subsample = p_dbl(0.1, 1)
)

# Threads
threads = 4
set_threads(graph_rf, n = threads)
set_threads(graph_xgboost, n = threads)


# DESIGNS -----------------------------------------------------------------
designs_l = lapply(custom_cvs, function(cv_) {
  # debug
  # cv_ = custom_cvs[[1]]

  # get cv inner object
  cv_inner = cv_$inner
  cv_outer = cv_$outer
  cat("Number of iterations fo cv inner is ", cv_inner$iters, "\n")

  # debug
  if (LIVE) {
    to_ = cv_inner$iters
  } else if (interactive()) {
    to_ = c(1, cv_inner$iters)
  } else {
    to_ = 1:cv_inner$iters
  }

  designs_cv_l = lapply(to_, function(i) { # 1:cv_inner$iters
    # debug
    # i = 1

    # with new mlr3 version I have to clone
    task_inner = task$clone()
    task_inner$filter(c(cv_inner$train_set(i), cv_inner$test_set(i)))

    # inner resampling
    custom_ = rsmp("custom")
    custom_$id = paste0("custom_", cv_inner$iters, "_", i)
    custom_$instantiate(task_inner,
                        list(cv_inner$train_set(i)),
                        list(cv_inner$test_set(i)))

    # objects for all autotuners
    measure_ = msr("adjloss2")
    tuner_   = tnr("hyperband", eta = 6)
    # tuner_   = tnr("mbo")
    # term_evals = 20

    # auto tuner rf
    at_rf = auto_tuner(
      tuner = tuner_,
      learner = graph_rf,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_rf,
      terminator = trm("none")
      # term_evals = term_evals
    )

    # auto tuner xgboost
    at_xgboost = auto_tuner(
      tuner = tuner_,
      learner = graph_xgboost,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_xgboost,
      terminator = trm("none")
      # term_evals = term_evals
    )

    # # auto tuner BART
    # at_bart = auto_tuner(
    #   tuner = tuner_,
    #   learner = graph_bart,
    #   resampling = custom_,
    #   measure = measure_,
    #   search_space = search_space_bart,
    #   terminator = trm("none")
    #   # term_evals = term_evals
    # )
    #
    # # auto tuner nnet
    # at_nnet = auto_tuner(
    #   tuner = tuner_,
    #   learner = graph_nnet,
    #   resampling = custom_,
    #   measure = measure_,
    #   search_space = search_space_nnet,
    #   terminator = trm("none")
    #   # term_evals = term_evals
    # )
    #
    # # auto tuner lightgbm
    # at_lightgbm = auto_tuner(
    #   tuner = tuner_,
    #   learner = graph_lightgbm,
    #   resampling = custom_,
    #   measure = measure_,
    #   search_space = search_space_lightgbm,
    #   terminator = trm("none")
    #   # term_evals = term_evals
    # )
    #
    # # auto tuner earth
    # at_earth = auto_tuner(
    #   tuner = tuner_,
    #   learner = graph_earth,
    #   resampling = custom_,
    #   measure = measure_,
    #   search_space = search_space_earth,
    #   terminator = trm("none")
    #   # term_evals = term_evals
    # )
    #
    # # auto tuner kknn
    # at_kknn = auto_tuner(
    #   tuner = tuner_,
    #   learner = graph_kknn,
    #   resampling = custom_,
    #   measure = measure_,
    #   search_space = search_space_kknn,
    #   terminator = trm("none")
    #   # term_evals = term_evals
    # )
    #
    # # auto tuner gbm
    # at_gbm = auto_tuner(
    #   tuner = tuner_,
    #   learner = graph_gbm,
    #   resampling = custom_,
    #   measure = measure_,
    #   search_space = search_space_gbm,
    #   terminator = trm("none")
    #   # term_evals = term_evals
    # )
    #
    # # auto tuner rsm
    # at_rsm = auto_tuner(
    #   tuner = tuner_,
    #   learner = graph_rsm,
    #   resampling = custom_,
    #   measure = measure_,
    #   search_space = search_space_rsm,
    #   terminator = trm("none")
    #   # term_evals = term_evals
    # )
    #
    # # auto tuner rsm
    # at_bart = auto_tuner(
    #   tuner = tuner_,
    #   learner = graph_bart,
    #   resampling = custom_,
    #   measure = measure_,
    #   search_space = search_space_bart,
    #   terminator = trm("none")
    #   # term_evals = term_evals
    # )
    #
    # # auto tuner catboost
    # at_catboost = auto_tuner(
    #   tuner = tuner_,
    #   learner = graph_catboost,
    #   resampling = custom_,
    #   measure = measure_,
    #   search_space = search_space_catboost,
    #   terminator = trm("none")
    #   # term_evals = term_evals
    # )
    #
    # # auto tuner glmnet
    # at_glmnet = auto_tuner(
    #   tuner = tuner_,
    #   learner = graph_glmnet,
    #   resampling = custom_,
    #   measure = measure_,
    #   search_space = search_space_glmnet,
    #   terminator = trm("none")
    #   # term_evals = term_evals
    # )
    #
    # # auto tuner glmnet
    # at_cforest = auto_tuner(
    #   tuner = tuner_,
    #   learner = graph_cforest,
    #   resampling = custom_,
    #   measure = measure_,
    #   search_space = search_space_cforest,
    #   terminator = trm("none")
    #   # term_evals = term_evals
    # )

    # outer resampling
    customo_ = rsmp("custom")
    customo_$id = paste0("custom_", cv_inner$iters, "_", i)
    customo_$instantiate(task, list(cv_outer$train_set(i)), list(cv_outer$test_set(i)))

    # nested CV for one round
    design = benchmark_grid(
      tasks = task,
      learners = list(at_rf, at_xgboost),
      # at_lightgbm, at_nnet, at_earth,
      # at_kknn, at_gbm, at_rsm, at_bart, at_catboost, at_glmnet), # , at_cforest
      resamplings = customo_
    )
  })
  designs_cv = do.call(rbind, designs_cv_l)
})
designs = do.call(rbind, designs_l)


# COMPUTATION -------------------------------------------------------------
# Define registry directory
dirname_ = "experiments_spyml"
if (dir.exists(dirname_)) system(paste0("rm -r ", dirname_))

# Create registry
packages = c("data.table", "gausscov", "paradox", "mlr3", "mlr3pipelines",
             "mlr3tuning", "mlr3misc", "future", "future.apply",
             "mlr3extralearners", "stats")
reg = makeExperimentRegistry(file.dir = dirname_, seed = 1, packages = packages)

# Populate registry with problems and algorithms to form the jobs
batchmark(designs, reg = reg)

# Save registry
saveRegistry(reg = reg)

