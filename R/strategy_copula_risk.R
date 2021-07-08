library(data.table)
library(copula)
library(leanr)
library(mrisk)
library(tidyr)
library(runner)
library(parallel)


# prepare data
market_data <- leanr::import_lean("D:/market_data/equity/usa/hour/trades_adjusted")
market_data[, returns := close / shift(close) - 1, by = c("symbol")]
market_data[, losses := -returns]
market_data <- na.omit(market_data)
returns <- as.data.table(pivot_wider(market_data, id_cols = "datetime", names_from = "symbol", values_from = returns))
spy_index <- which(colnames(returns) == "SPY")
cols_index <- colnames(returns)[c(1, spy_index, setdiff(2:ncol(returns), spy_index))]
returns <- returns[, .SD, .SDcols = cols_index]

# spy t copuls
for (i in 3:ncol(returns)) {
  print(i)

  # define columns and create file name
  cols <- colnames(returns)[c(1, 2, i)]
  file_name <- paste0(paste0(cols[2:3], collapse = "-"), ".csv")

  # cont if already estimated
  if (file_name %in% list.files("D:/risks/copulas/hour")) {
    next()
  }

  # remove rows qith 0 and compute pseudo observations
  sample_ <- returns[, ..cols]
  sample_ <- na.omit(sample_)
  sample_ <- sample_[which(rowSums(sample_[, 2:ncol(sample_)] != 0) == (ncol(sample_) - 1))]
  print(dim(sample_))
  U <- as.matrix(pobs(sample_[, 2:3]))

  # st up clusters
  cl <- makeCluster(16)
  clusterExport(cl, c("U"), envir = environment())

  # rolling copula
  roll_tcopula <- runner(
    x = as.data.frame(1:nrow(U)),
    k = 100,
    f = function(x) {
      library(copula)
      fit.t <- tryCatch({
        fitCopula(tCopula(), data = U[x, ])
      }, error = function(e) NA)
      if (is.na(fit.t)) {
        est <- data.frame(rho = NA, df = NA, loglik = NA, var = NA)
      } else {
        var <- tryCatch(fit.t@var.est[1,1], error = function(e) NA)
        est <- cbind.data.frame(as.data.frame(t(fit.t@estimate)), fit.t@loglik, var)
        colnames(est) <- c("rho", "df", "loglik", "var")
      }
      est
    },
    na_pad = TRUE,
    cl = cl
  )
  stopCluster(cl)

  # tidy results
  tcopula_indicator <- lapply(roll_tcopula, as.data.table)
  tcopula_indicator <- rbindlist(tcopula_indicator, fill = TRUE)
  tcopula_indicator[, V1 := NULL]
  tcopula_indicator <- cbind(sample_, tcopula_indicator)

  # save
  full_file_name <- file.path("D:/risks/copulas/hour", file_name)
  fwrite(tcopula_indicator, full_file_name)
}
