###  FXREGIME

# NOT RUN {
##### Example 1: HARRVCJ #####
dat <- sample5MinPricesJumps$stock1
dat <- makeReturns(dat) #Get the high-frequency return data

x <- HARmodel(dat, periods = c(1,5,10), periodsJ = c(1,5,10),
              RVest = c("rCov","rBPCov"),
              type = "HARRVCJ",transform = "sqrt", inputType = "returns")
# Estimate the HAR model of type HARRVCJ
class(x)
x
# plot(x)
predict(x)


##### Example 2: HARRV #####
# Forecasting daily Realized volatility for the S&P 500 using the basic HARmodel: HARRV
library(xts)
RV_SP500 <- as.xts(realizedLibrary$rv5, order.by = realizedLibrary$date)

x <- HARmodel(data = RV_SP500 , periods = c(1,5,22), RVest = c("rCov"),
              type = "HARRV", h = 1, transform = NULL, inputType = "RM")
class(x)
x
summary(x)
plot(x)
predict(x)


##### Example 3: HARRVQ #####
dat <- sample5MinPricesJumps$stock1
dat <- makeReturns(dat) #Get the high-frequency return data
#
x <- HARmodel(dat, periods = c(1,5,10), periodsJ = c(1,5,10),
              periodsQ = c(1), RVest = c("rCov", "rQuar"),
              type="HARRVQ", inputType = "returns")
## Estimate the HAR model of type HARRVQ
class(x)
x
plot(x)
predict(x)

##### Example 4: HARRVQJ with already computed realized measures #####
dat <- SP500RM[, c("RV", "BPV", "RQ")]
x <- HARmodel(dat, periods = c(1,5,22), periodsJ = c(1),
              periodsQ = c(1), type = "HARRVQJ")
## Estimate the HAR model of type HARRVQJ
class(x)
x
plot(x)
predict(x)

##### Example 5: CHARRV with already computed realized measures #####
dat <- SP500RM[, c("RV", "BPV")]

x <- HARmodel(dat, periods = c(1, 5, 22), type = "CHARRV")
# Estimate the HAR model of type CHARRV
class(x)
x
plot(x)
predict(x)

##### Example 6: CHARRVQ with already computed realized measures #####
dat <- SP500RM[, c("RV", "BPV", "RQ")]

x <- HARmodel(dat, periods = c(1,5,22), periodsQ = c(1), type = "CHARRVQ")
# Estimate the HAR model of type CHARRVQ
class(x)
x
# plot(x)
predict(x)

#' ##### Example 7: HARRV #####
# Forecasting weekly Realized volatility for the S&P 500 using the basic HARmodel: HARRV
library(xts)
RV_SP500 <- as.xts(realizedLibrary$rv5, order.by = realizedLibrary$date)

x <- HARmodel(data = RV_SP500 , periods = c(1,5,22), RVest = c("rCov"),
              type = "HARRV", h = 5, transform = NULL, inputType = "RM")
class(x)
x
summary(x)
plot(x)
predict(x)

# }
