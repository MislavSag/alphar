#' CUSUM Filter for Price
#'
#' @param data data in form of bar (tick bar, time bar, volume bar ...)
#' @param h threshold value
#'
#' @return an index of data exceeding threshold value h
#' @export
#'
#' @examples CUSUM_Price(data1, 2)
CUSUM_Price <- function(data, h){
  POS <- NEG <- 0
  index <- NULL
  diff_data <- diff(data)
  for (i in 1:length(diff_data)){
    POS <- max(0,POS+diff_data[i])
    NEG <- min(0,POS+diff_data[i])
    if(max(POS,-NEG)>=h){
      index <- c(index, i)
      POS <- NEG <- 0
    }
  }
  return(index+1)
}
