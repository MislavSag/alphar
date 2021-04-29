library(data.table)
library(BigVAR)


# params
risk_path <- "D:/risks"


# import data and merge
gpd_risks_left_tail <- fread(file.path(risk_path, 'gpd_risks_left_tail.csv'), sep = ';')
gpd_risks_right_tail <- fread(file.path(risk_path, 'gpd_risks_right_tail.csv'))
cols <- colnames(gpd_risks_right_tail)[grep('q_|e_', colnames(gpd_risks_right_tail))]
gpd_risks_right_tail[, (cols) := lapply(.SD, as.numeric), .SDcols = cols]
gpd_risks_left_tail[, (cols) := lapply(.SD, as.numeric), .SDcols = cols]
colnames(gpd_risks_left_tail) <- paste0("left_", colnames(gpd_risks_left_tail))
colnames(gpd_risks_right_tail) <- paste0("right_", colnames(gpd_risks_right_tail))

# merge spy stocks and risk measures
sp500_stocks <- sp500_stocks[gpd_risks_left_tail[left_symbol != 'SPY'], on = c(.id = 'left_symbol', index = 'left_date')]
sp500_stocks <- sp500_stocks[gpd_risks_right_tail[right_symbol != 'SPY'], on = c(.id = 'right_symbol', index = 'right_date')]

#
library(BGVAR)
BGVAR::bgvar()

data(eerData)
names(eerData)
colnames(eerData$UK)
head(eerData$US)
bigX<-list_to_matrix(eerData)

# # prepare data fog bigvar
# DT <- gpd_risks_left_tail[left_symbol == "AAPL"]
# DT <- na.omit(DT)
# Y <- DT[, 3:ncol(DT)]
# Y <- Y[, lapply(.SD, as.numeric)]
# Y <- as.matrix(Y)
# Y <- Y[1:5000,]
# T1=floor(nrow(Y)/3)
# T2=floor(2*nrow(Y)/3)
# head(Y)
#
# m = constructModel(Y, p = 4, struct = "Basic", gran=c(50,10), verbose=FALSE, T1=T1, T2=T2, IC=FALSE)
# plot(m)
# results=cv.BigVAR(m)
# results
# plot(results)
# predict(results,n.ahead=1)
# SparsityPlot.BigVAR.results(results)
#
# data(Y)
# T1=floor(nrow(Y)/3)
# T2=floor(2*nrow(Y)/3)
# m1=constructModel(Y,p=4,struct="Basic",gran=c(50,10),verbose=FALSE,T1=T1,T2=T2,IC=FALSE)
# plot(m1)
# results=cv.BigVAR(m1)
# plot(results)
# predict(results,n.ahead=1)
#
