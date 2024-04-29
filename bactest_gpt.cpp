#include <Rcpp.h>
using namespace Rcpp;

// [[Rcpp::plugins("cpp11")]]

// [[Rcpp::export]]
double backtest_cpp_gpt(NumericVector returns, NumericVector indicator, double threshold) {
  int n = indicator.size();
  NumericVector sides(n, 1.0);

  for(int i = 1; i < n; ++i) {
    if(!NumericVector::is_na(indicator[i-1]) && indicator[i-1] > threshold) {
      sides[i] = 0;
    }
  }

  double cum_returns = 1.0;
  for(int i = 0; i < n; ++i) {
    cum_returns *= (1 + returns[i] * sides[i]);
  }
  return cum_returns - 1;
}

// [[Rcpp::export]]
List combined_walk_forward_opt(NumericVector returns,
                               List indicators,
                               NumericVector thresholds,
                               int window) {
  int n = returns.size();
  if (window > n) {
    stop("Window size exceeds available data.");
  }

  int nWindows = n - window + 1;
  List results(nWindows);
  NumericVector sr(thresholds.size()); // Moved outside the loop

  for(int i = 0; i < nWindows; ++i) {
    NumericVector returns_window = returns[Range(i, i + window - 1)];
    for(int j = 0; j < thresholds.size(); ++j) {
      NumericVector indicator_ = as<NumericVector>(indicators[j]);
      sr[j] = backtest_cpp_gpt(returns_window, indicator_, thresholds[j]);
    }
    results[i] = clone(sr); // Clone the vector to save the result
  }

  return results;
}
