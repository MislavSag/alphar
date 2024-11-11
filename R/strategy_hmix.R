library(hmix)


# Example usage of hmix function:
result <- hmix(tail(dummy_set$AMZN, 100), horizon = 7, centers = 5, n_hidden = 3, n_tests = 5)
print(result$model)
result$model$hmm_model
print(result$error_sets)
# Random sampling for each point in predicted horizon
result$model$pred_funs$t1$rfun(10)
# ICDF for each point in horizon
result$model$pred_funs$t5$qfun(c(0, 1))
# PDF for each point in horizon
result$model$pred_funs$t8$dfun(tail(dummy_set$AMZN))
# CDF for each point in horizon
result$model$pred_funs$t10$pfun(tail(dummy_set$AMZN))
