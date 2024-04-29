#include <Rcpp.h>
using namespace Rcpp;


// [[Rcpp::export]]
double backtest_cpp_gpt(NumericVector returns, NumericVector indicator, double threshold) {
  int n = indicator.size();
  NumericVector sides(n, 1.0); // Initialize with 1s

  for(int z = 1; z < n; ++z) { // Start from 1 since we look back one period
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

DataFrame bind_scalar_and_vector_to_df(double x, NumericVector vec) {
  int n = vec.size();

  // Create a numeric vector filled with the scalar value 'x', one for each element of vec
  NumericVector scalar_vec(n, x);

  // Create a DataFrame with two columns
  DataFrame result = DataFrame::create(Named("Scalar") = scalar_vec, Named("Vector") = vec);

  return result;
}

DataFrame sliceDataFrameColumnWise(DataFrame df, int start, int end) {
  int n = df.size();  // Number of columns
  List sliced(n);     // List to store each sliced column


  for(int i = 0; i < n; i++) {
    // Check if the column can be converted
    if (TYPEOF(df[i]) == REALSXP) {
      NumericVector col = as<NumericVector>(df[i]);
      sliced[i] = col[Range(start, end)];
    } else {
      Rcpp::Rcout << "Non-numeric column encountered at index: " << i << std::endl;
    }
  }

  // Recreate the DataFrame with the new columns
  DataFrame result(sliced);
  result.attr("names") = df.attr("names");
  result.attr("row.names") = df.attr("row.names");

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
List wfo(DataFrame df,
         DataFrame params,
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

  // Loop over data
  for(int i = 0; i < results.size(); ++i) {

    DataFrame df_window;

    // Before processing each window, ensure the indices are valid
    if (i + window - 1 < df.nrow()) {
      if (window_type == "expanding") {
        df_window = sliceDataFrameColumnWise(df, 0, i + window - 1);
      } else {  // "rolling"
        df_window = sliceDataFrameColumnWise(df, i, i + window - 1);
      }
    } else {
      Rcpp::Rcout << "Skipping window " << i << " due to index out of range." << std::endl;
      continue;
    }

    NumericVector sr = opt(df_window, params);

    // Bind time and results
    int time_index = (window_type == "expanding") ? i + window - 1 : i + window - 1;
    results[i] = bind_scalar_and_vector_to_df(time[time_index], sr);
  }

  return results;
}
