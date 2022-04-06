library(data.table)
library(zoo)
library(fmpcloudr)
library(ggplot2)
# source('calculateReturns.R')
# source('calculateMaxDD.R')
# source('backshift.R')


# set fmpcloudr api token
API_KEY = "15cd5d0adf4bc6805a724b4417bbaafc"
fmpc_set_token(API_KEY)

# get data
DT <- fmpcloudr::fmpc_price_history_spldiv(c("GLD", "GDX"), startDate = Sys.Date() - 2000, Sys.Date())
DT <- as.data.table(DT)
DT <- DT[, .(symbol, date, adjClose)]
DT <- dcast(DT, date ~ symbol, value.var = "adjClose")

# plot
ggplot(DT, aes(x = date)) +
  geom_line(aes(y = GDX), color = "red") +
  geom_line(aes(y = GLD), color = "blue")

# define indices for training and test sets
trainset <- 1:1000
testset <- (length(trainset)+1):nrow(DT)

#
result <- lm(GLD ~ 0 + GDX, data = DT[trainset])
hedgeRatio <- coef(result) # 1.631
DT[, spread := GLD - hedgeRatio * GDX]
plot(DT$spread)
plot(DT[trainset, spread])
plot(DT[testset, spread])

# mean of spread on trainset
spreadMean <- mean(DT[trainset, spread])

# standard deviation of spread on trainset
spreadStd <- sd(DT[trainset, spread]) # 1.948731
zscore <- (DT$spread-spreadMean)/spreadStd
longs <- zscore <= -2 # buy spread when its value dropsbelow 2 standard deviations.
shorts <- zscore >= 2 # short spread when its value rises above 2 standard deviations.

# exit any spread position when its value is within 1standard deviation of its mean.
longExits <- zscore >= -1
shortExits <- zscore <= 1
posL <- matrix(NaN, nrow(DT), 2) # long positions
posS <- matrix(NaN, nrow(DT), 2) # short positions

# initialize to 0
posL[1,] <- 0
posS[1,] <- 0
posL[longs, 1] <- 1
posL[longs, 2] <- -1
posS[shorts, 1] <- -1
posS[shorts, 2] <- 1
posL[longExits, 1] <- 0
posL[longExits, 2] <- 0
posS[shortExits, 1] <- 0
posS[shortExits, 2] <- 0

# ensure existing positions are carried forward unless there is an exit signal
posL <- zoo::na.locf(posL)
posS <- zoo::na.locf(posS)
positions <- posL + posS
cl <- cbind(DT$GDX, DT$GLD) # last row is [385,] 77.32 46.36

# daily returns of price series
cols <- paste0("returns_", c("GDX", "GLD"))
DT[, (cols) := lapply(.SD, function(x) x / shift(x) - 1), .SDcols = c("GDX", "GLD")]
# DT[, positions := positions]
dailyret <- DT[, .(returns_GDX, returns_GLD)]
pnl <- rowSums(backshift(1, positions)*dailyret)
sharpeRatioTrainset <- sqrt(252)*mean(pnl[trainset], na.rm = TRUE)/sd(pnl[trainset], na.rm = TRUE)
sharpeRatioTestset <- sqrt(252)*mean(pnl[testset], na.rm = TRUE)/sd(pnl[testset], na.rm = TRUE)


# This codes makes use of the function backshift, which you can download as backshift.R.
backshift <- function(mylag, x) {
  rbind(matrix(NaN, mylag, ncol(x)),
        as.matrix(x[1:(nrow(x)-mylag),]))
}



# data1 <- read.delim("GLD.txt") # Tab-delimited
# data_sort1 <- data1[order(as.Date(data1[,1],
#                                   '%m/%d/%Y')),] # sort in ascending order of dates (1st
# column of data)
# tday1 <- as.integer(format(as.Date(data_sort1[,1],
#                                    '%m/%d/%Y'), '%Y%m%d'))
# adjcls1 <- data_sort1[,ncol(data_sort1)]
# data2 <- read.delim("GDX.txt") # Tab-delimited
# data_sort2 <- data2[order(as.Date(data2[,1],
#                                   '%m/%d/%Y')),] # sort in ascending order of dates (1st
# column of data)
#
# tday2 <- as.integer(format(as.Date(data_sort2[,1],
#                                    '%m/%d/%Y'), '%Y%m%d'))
# adjcls2 <- data_sort2[,ncol(data_sort2)]
#
# # find the intersection of the two data sets
# tday <- intersect(tday1, tday2)
# adjcls1 <- adjcls1[tday1 %in% tday]
# adjcls2 <- adjcls2[tday2 %in% tday]
#
# # define indices for training and test sets
# trainset <- 1:252
# testset <- length(trainset)+1:length(tday)

# # determines the hedge ratio on the trainset
# result <- lm(adjcls1 ~ 0 + adjcls2, subset=trainset )
# hedgeRatio <- coef(result) # 1.631
# spread <- adjcls1-hedgeRatio*adjcls2 # spread = GLD -
# hedgeRatio*GDX
# plot(spread)
# dev.new()
# plot(spread[trainset])
# dev.new()
# plot(spread[testset])
# mean of spread on trainset
# spreadMean <- mean(spread[trainset]) # 0.05219624
# # standard deviation of spread on trainset
# spreadStd <- sd(spread[trainset]) # 1.948731
# zscore <- (spread-spreadMean)/spreadStd
# longs <- zscore <= -2 # buy spread when its value drops
# below 2 standard deviations.
# shorts <- zscore >= 2 # short spread when its value
# rises above 2 standard deviations.
# # exit any spread position when its value is within 1
# standard deviation of its mean.
# longExits <- zscore >= -1
# shortExits <- zscore <= 1
# posL <- matrix(NaN, length(tday), 2) # long positions
# posS <- matrix(NaN, length(tday), 2) # short positions
# # initialize to 0
# posL[1,] <- 0
# posS[1,] <- 0
# posL[longs, 1] <- 1
# posL[longs, 2] <- -1
# posS[shorts, 1] <- -1
# posS[shorts, 2] <- 1
# posL[longExits, 1] <- 0
# posL[longExits, 2] <- 0
# posS[shortExits, 1] <- 0
#
# posS[shortExits, 2] <- 0

# # ensure existing positions are carried forward unless
# there is an exit signal
# posL <- zoo::na.locf(posL)
# posS <- zoo::na.locf(posS)
# positions <- posL + posS
# cl <- cbind(adjcls1, adjcls2) # last row is [385,]
# 77.32 46.36
# daily returns of price series
dailyret <- calculateReturns(cl, 1) # last row is
[385,] -0.0122636689 -0.0140365802
pnl <- rowSums(backshift(1, positions)*dailyret)
sharpeRatioTrainset <- sqrt(252)*mean(pnl[trainset],
                                      na.rm = TRUE)/sd(pnl[trainset], na.rm = TRUE)
sharpeRatioTrainset # 2.327844
sharpeRatioTestset <- sqrt(252)*mean(pnl[testset], na.rm
                                     = TRUE)/sd(pnl[testset], na.rm = TRUE)
sharpeRatioTestset # 1.508212
This codes makes use of the function backshift, which
you can download as backshift.R.
backshift <- function(mylag, x) {
  rbind(matrix(NaN, mylag, ncol(x)),
        as.matrix(x[1:(nrow(x)-mylag),]))
}
