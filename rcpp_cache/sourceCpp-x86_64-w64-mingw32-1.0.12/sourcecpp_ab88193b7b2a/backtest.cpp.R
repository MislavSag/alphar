`.sourceCpp_1_DLLInfo` <- dyn.load('C:/Users/Mislav/Documents/GitHub/alphar/rcpp_cache/sourceCpp-x86_64-w64-mingw32-1.0.12/sourcecpp_ab88193b7b2a/sourceCpp_3.dll')

backtest_cpp_gpt <- Rcpp:::sourceCppFunction(function(returns, indicator, threshold) {}, FALSE, `.sourceCpp_1_DLLInfo`, 'sourceCpp_1_backtest_cpp_gpt')
sliceDataFrameColumnWise <- Rcpp:::sourceCppFunction(function(df, start, end) {}, FALSE, `.sourceCpp_1_DLLInfo`, 'sourceCpp_1_sliceDataFrameColumnWise')
cbind_scalar_and_df <- Rcpp:::sourceCppFunction(function(x, df) {}, FALSE, `.sourceCpp_1_DLLInfo`, 'sourceCpp_1_cbind_scalar_and_df')
bind_scalar_and_vector_to_df <- Rcpp:::sourceCppFunction(function(x, vec) {}, FALSE, `.sourceCpp_1_DLLInfo`, 'sourceCpp_1_bind_scalar_and_vector_to_df')
opt <- Rcpp:::sourceCppFunction(function(df, params) {}, FALSE, `.sourceCpp_1_DLLInfo`, 'sourceCpp_1_opt')
wfo <- Rcpp:::sourceCppFunction(function(df, params, window, window_type = "rolling") {}, FALSE, `.sourceCpp_1_DLLInfo`, 'sourceCpp_1_wfo')

rm(`.sourceCpp_1_DLLInfo`)
