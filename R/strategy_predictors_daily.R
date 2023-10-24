library(arrow)
library(fs)
library(data.table)


# import data
backcusum_files = dir_ls("F:/equity/usa/predictors-daily/backcusum")
backusum = lapply(backcusum_files, read_parquet)
names(backusum) = fs::path_ext_remove(fs::path_file(names(backusum)) )
backusum = rbindlist(backusum, idcol = "symbol")

# free memory
gc()
