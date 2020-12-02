Rcpp::cppFunction('NumericVector create_signsC(NumericVector x) {
    int n = x.size();
    NumericVector sign(n);

    for(int i = 0; i < n; ++i) {
      if (i == 0) sign[i] = R_NaN;
      else if(x[i - 1] == FALSE) sign[i] = 1;
      else sign[i] = 0;
    }
    return sign;
}')


Rcpp::cppFunction('NumericVector create_signsC_2(NumericVector x, NumericVector returns_sum) {
    int n = x.size();
    NumericVector sign(n);

    for(int i = 0; i < n; ++i) {
      if (i == 0) sign[i] = R_NaN;
      else if(x[i - 1] == TRUE and returns_sum[i - 1] > 0) sign[i] = 0;
      else sign[i] = 1;
    }
    return sign;
}')
