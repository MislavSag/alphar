#include <Rcpp.h>
using namespace Rcpp;


// [[Rcpp::export]]
double backtest_cpp_gpt(NumericVector returns, NumericVector indicator, double threshold) {
  int n = indicator.size();
  NumericVector sides(n, 1.0); // Initialize with 1s

  for(int z = 1; z < n + 1; ++z) { // Start from 1 since we look back one period
    if(!NumericVector::is_na(indicator[z-1]) && indicator[z-1] > threshold) {
      sides[z] = 0;
    }
  }

  double cum_returns = 1.0; // Start cumulative returns at 1
  for(int z = 0; z < n; ++z) {
    cum_returns *= (1 + returns[z] * sides[z]);
  }
  return cum_returns - 1; // Adjust for initial value
}

// [[Rcpp::export]]
DataFrame sliceDataFrameColumnWise(DataFrame df, int start, int end) {
  int n = df.size();  // Number of columns
  List sliced(n);     // List to store each sliced column

  // Loop over each column
  for(int i = 0; i < n; i++) {
    // Subset the column using Range and assign it to the list
    sliced[i] = as<NumericVector>(df[i])[Range(start, end)];
  }

  // Recreate the DataFrame with the new columns
  DataFrame result(sliced);
  result.attr("names") = df.attr("names");

  return result;
}

// [[Rcpp::export]]
DataFrame cbind_scalar_and_df(double x, DataFrame df) {
  // Get the number of rows in the DataFrame
  int n = df.nrows();

  // Create a numeric vector that repeats 'x', one for each row
  NumericVector new_col(n, x); // 'n' repetitions of 'x'

  // Prepare a list to construct the new DataFrame
  List new_df(df.size() + 1);

  // Add the new column to the list
  new_df[0] = new_col;

  // Add the original DataFrame columns to the list
  CharacterVector df_names = df.names();
  for(int i = 0; i < df.size(); i++) {
    new_df[i + 1] = df[i];
  }

  // Set names for the new DataFrame
  CharacterVector new_names(df.size() + 1);
  new_names[0] = "x"; // Name for the new column
  for(int i = 0; i < df.size(); i++) {
    new_names[i + 1] = df_names[i];
  }

  new_df.attr("names") = new_names;
  new_df.attr("class") = "data.frame";
  new_df.attr("row.names") = df.attr("row.names");

  return DataFrame(new_df);
}

// [[Rcpp::export]]
DataFrame bind_scalar_and_vector_to_df(double x, NumericVector vec) {
  int n = vec.size();

  // Create a numeric vector filled with the scalar value 'x', one for each element of vec
  NumericVector scalar_vec(n, x);

  // Create a DataFrame with two columns
  DataFrame result = DataFrame::create(Named("Scalar") = scalar_vec, Named("Vector") = vec);

  return result;
}

// [[Rcpp::export]]
NumericVector opt(DataFrame df, DataFrame params) {

  // Define help variables
  int n_params = params.nrow();
  NumericVector returns = df["returns"];
  CharacterVector indicators = params["variable"];
  NumericVector thresholds = params["thresholds"];

  // return 1;
  // Loop through all params rows and calculate the backtest results
  NumericVector results(n_params);
  for(int i = 0; i < n_params; ++i) {
    String ind_ = indicators[i];
    NumericVector indicator_ = df[ind_];
    results[i] = backtest_cpp_gpt(returns, indicator_, thresholds[i]);
  }
  return results;
}

// [[Rcpp::export]]
List wfo(DataFrame df, DataFrame params,
         int window,
         std::string window_type = "rolling") {

  // Define help variables
  int n = df.nrow();
  NumericVector time = df["time"];
  NumericVector returns = df["returns"];
  CharacterVector indicators = params["variable"];
  NumericVector thresholds = params["thresholds"];

  // Check if window size is valid relative to data size
  if (window > n) {
    stop("Window size exceeds available data.");
  }

  List results;
  if (window_type == "expanding") {
    results = List(n - window + 1);  // Start at the minimum window size
  } else {
    results = List(n - window + 1);
  }

  // if (window_type == "expanding") {
  //   // Start from 0 and end at the current index (i) for expanding window
  //   df_window = sliceDataFrameColumnWise(df, 0, i);
  // } else { // "rolling"
  //   df_window = sliceDataFrameColumnWise(df, i, i + window - 1);
  // }

  // Loop over data
  for(int i = 0; i < results.size(); ++i) {
    DataFrame df_window;

    // Select the window type
    if (window_type == "expanding") {
      // Expanding from minimum window size to full range
      df_window = sliceDataFrameColumnWise(df, 0, i + window);  // Start from index 0 to current index + initial window size
    } else {  // "rolling"
      df_window = sliceDataFrameColumnWise(df, i, i + window - 1);  // Rolling window
    }

    NumericVector sr = opt(df_window, params);

    // Bind time and results
    int time_index = (window_type == "expanding") ? i + window : i + window - 1;
    results[i] = bind_scalar_and_vector_to_df(time[time_index], sr);
  }

  return results;
}


#include <Rcpp.h>
#ifdef RCPP_USE_GLOBAL_ROSTREAM
Rcpp::Rostream<true>&  Rcpp::Rcout = Rcpp::Rcpp_cout_get();
Rcpp::Rostream<false>& Rcpp::Rcerr = Rcpp::Rcpp_cerr_get();
#endif

// backtest_cpp_gpt
double backtest_cpp_gpt(NumericVector returns, NumericVector indicator, double threshold);
RcppExport SEXP sourceCpp_1_backtest_cpp_gpt(SEXP returnsSEXP, SEXP indicatorSEXP, SEXP thresholdSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< NumericVector >::type returns(returnsSEXP);
    Rcpp::traits::input_parameter< NumericVector >::type indicator(indicatorSEXP);
    Rcpp::traits::input_parameter< double >::type threshold(thresholdSEXP);
    rcpp_result_gen = Rcpp::wrap(backtest_cpp_gpt(returns, indicator, threshold));
    return rcpp_result_gen;
END_RCPP
}
// sliceDataFrameColumnWise
DataFrame sliceDataFrameColumnWise(DataFrame df, int start, int end);
RcppExport SEXP sourceCpp_1_sliceDataFrameColumnWise(SEXP dfSEXP, SEXP startSEXP, SEXP endSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< DataFrame >::type df(dfSEXP);
    Rcpp::traits::input_parameter< int >::type start(startSEXP);
    Rcpp::traits::input_parameter< int >::type end(endSEXP);
    rcpp_result_gen = Rcpp::wrap(sliceDataFrameColumnWise(df, start, end));
    return rcpp_result_gen;
END_RCPP
}
// cbind_scalar_and_df
DataFrame cbind_scalar_and_df(double x, DataFrame df);
RcppExport SEXP sourceCpp_1_cbind_scalar_and_df(SEXP xSEXP, SEXP dfSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< double >::type x(xSEXP);
    Rcpp::traits::input_parameter< DataFrame >::type df(dfSEXP);
    rcpp_result_gen = Rcpp::wrap(cbind_scalar_and_df(x, df));
    return rcpp_result_gen;
END_RCPP
}
// bind_scalar_and_vector_to_df
DataFrame bind_scalar_and_vector_to_df(double x, NumericVector vec);
RcppExport SEXP sourceCpp_1_bind_scalar_and_vector_to_df(SEXP xSEXP, SEXP vecSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< double >::type x(xSEXP);
    Rcpp::traits::input_parameter< NumericVector >::type vec(vecSEXP);
    rcpp_result_gen = Rcpp::wrap(bind_scalar_and_vector_to_df(x, vec));
    return rcpp_result_gen;
END_RCPP
}
// opt
NumericVector opt(DataFrame df, DataFrame params);
RcppExport SEXP sourceCpp_1_opt(SEXP dfSEXP, SEXP paramsSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< DataFrame >::type df(dfSEXP);
    Rcpp::traits::input_parameter< DataFrame >::type params(paramsSEXP);
    rcpp_result_gen = Rcpp::wrap(opt(df, params));
    return rcpp_result_gen;
END_RCPP
}
// wfo
List wfo(DataFrame df, DataFrame params, int window, std::string window_type);
RcppExport SEXP sourceCpp_1_wfo(SEXP dfSEXP, SEXP paramsSEXP, SEXP windowSEXP, SEXP window_typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< DataFrame >::type df(dfSEXP);
    Rcpp::traits::input_parameter< DataFrame >::type params(paramsSEXP);
    Rcpp::traits::input_parameter< int >::type window(windowSEXP);
    Rcpp::traits::input_parameter< std::string >::type window_type(window_typeSEXP);
    rcpp_result_gen = Rcpp::wrap(wfo(df, params, window, window_type));
    return rcpp_result_gen;
END_RCPP
}
