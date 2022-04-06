library(data.table)
library(jsonlite)



# import backtsets
opt_path <- "C:/Users/Mislav/Documents/GitHub/lean_test/ExuberLocal/optimizations"
opt_files <- list.files(opt_path)
last_file <- which.max((as.POSIXct(opt_files, "%Y-%m-%d_%H-%M-%H")))
opt <- file.path(opt_path, opt_files[last_file])
backtests_files <- list.files(opt)
backtests_files <- na.omit(backtests_files[nchar(backtests_files) > 25])
backtests_files <- file.path(opt, backtests_files, paste0(backtests_files, ".json"))
backtests <- lapply(backtests_files, read_json)

#
backtests_results <- lapply(backtests, function(x) as.data.table(x$Statistics))
rbindlist(backtests_results)
