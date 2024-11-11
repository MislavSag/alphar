suppressMessages(library(data.table))
suppressMessages(library(erf))
suppressMessages(library(janitor))
# suppressMessages(library(arrow))
# suppressMessages(library(dplyr))
suppressMessages(library(foreach))
suppressMessages(library(doParallel))


# setup
SAVEPATH = file.path("results")
if (!dir.exists(SAVEPATH)) dir.create(SAVEPATH, recursive = TRUE)

# Get index
i = as.integer(Sys.getenv('PBS_ARRAY_INDEX'))
# i = 1

# Import data
print("Import data")
file_ = paste0("data/prices_", i, ".csv")
dt = fread(file_, drop = c("close_raw", "close", "returns"))

# Select some date interval
print("Define dates")
dates = seq.Date(from = as.Date("2000-01-01"), to = Sys.Date(), by = "day")
dates = as.Date(intersect(dates, dt[, unique(date)]))
symbols = dt[, unique(symbol)]
# divide symbols in chunks so that I have 10.000 chunks. That is i want 10.000 elements in list
symbols_chunks = split(symbols, ceiling(seq_along(symbols) / ceiling(length(symbols) / 10000)))
length(symbols_chunks)
tail(symbols_chunks)

# Loop over date and symbols and train erf model extract predictions
print("Variables and params")
quantile_levels = c(0.0005, 0.01, 0.02, 0.05, 0.95, 0.98, 0.99, 0.995)
setorder(dt, symbol, date)
predictors = dt[, colnames(.SD),
                .SDcols = -c("id", "symbol", "date", "target")]

# Estimate
print("Estimate")
for (s in dt[, unique(symbol)]) {
  # debug
  # s = "a"
  print(s)

  # Check if already estimated
  file_name = file.path(SAVEPATH, paste0(s, ".csv"))
  if (file.exists(file_name)) next

  # sample data
  dt_ = dt[symbol == s]

  # estimation
  cl = makeCluster(8L)
  registerDoParallel(cl)
  l = foreach(i = 1:length(dates),
              .packages = c("data.table", "erf", "janitor"),
              .export = c("dt_", "s", "predictors")) %dopar% {
                d = dates[i]
                # d = dates[1]

                # Train data
                dtd = dt_[date < d]
                if (nrow(dtd) < 252) return(NULL)
                if (as.Date(d) - dt_[, as.Date(max(date))] > 2) return(NULL)

                # Test data
                test_data = dt_[date == d]
                if (nrow(test_data) == 0) return(NULL)

                # Fit model for upper
                train_data_upper = dtd[target > 0]
                erf_model_upper = erf(
                  X = as.matrix(train_data_upper[, .SD, .SDcols = predictors]),
                  Y = train_data_upper[, target],
                  min.node.size = 5,
                  lambda = 0.001,
                  intermediate_quantile = 0.8
                )

                # Fit model for lower
                train_data_lower = dt_[target < 0]
                erf_model_lower = erf(
                  X = as.matrix(train_data_lower[, .SD, .SDcols = predictors]),
                  Y = -train_data_lower[, target],
                  min.node.size = 5,
                  lambda = 0.001,
                  intermediate_quantile = 0.8
                )

                # Predict
                erf_predictions_upper = predict(
                  erf_model_upper,
                  as.matrix(test_data[, .SD, .SDcols = predictors]),
                  quantiles = quantile_levels
                )
                erf_predictions_lower = predict(
                  erf_model_lower,
                  as.matrix(test_data[, .SD, .SDcols = predictors]),
                  quantiles = quantile_levels
                )
                erf_predictions_upper = clean_names(as.data.frame(erf_predictions_upper))
                colnames(erf_predictions_upper) = paste0("upper_", colnames(erf_predictions_upper))
                erf_predictions_lower = clean_names(as.data.frame(erf_predictions_lower))
                colnames(erf_predictions_lower) = paste0("lower_", colnames(erf_predictions_lower))
                cbind(symbol = s, date = d, erf_predictions_upper, erf_predictions_lower,
                      targetr = test_data[, target])
              }
  stopCluster(cl)

  # Clean and save
  x = rbindlist(l)
  fwrite(x, file_name)
}
