library(fitteR)
library(ExtDist)


# continous empirical data
x <- rnorm(1000, 50, 3)
s <- Sys.time()
r <- fitter(x, dom="c")
e <- Sys.time()
e - s

s <- Sys.time()
r <- fitter(x, dom="c", fast = TRUE)
e <- Sys.time()
e - s

printReport(r, type=c("csv"), file = "test.csv") # warning as 'file' is NULL,


# out <- printReport(r, type="shiny")
# names(out)
#
# out <- printReport(r, type=c("csv"), file = "test.csv") # warning as 'file' is NULL,
# str(out) # but table (data.frame) returned
