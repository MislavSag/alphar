library(eventstudies)


# data
data(SplitDates)
str(SplitDates)
head(SplitDates)
data(StockPriceReturns)
str(StockPriceReturns)

es <- eventstudy(firm.returns = StockPriceReturns,
                 event.list = SplitDates,
                 event.window = 5,
                 type = "None",
                 to.remap = TRUE,
                 remap = "cumsum",
                 inference = TRUE,
                 inference.strategy = "bootstrap")

par(mai=c(.8,.8,.2,.2), cex=.7)
plot(es)

es$outcomes

data(OtherReturns)
es.mm <- eventstudy(firm.returns = StockPriceReturns,
                    event.list = SplitDates,
                    event.window = 5,
                    type = "marketModel",
                    to.remap = TRUE,
                    remap = "cumsum",
                    inference = TRUE,
                    inference.strategy = "bootstrap",
                    model.args = list(market.returns=OtherReturns$NiftyIndex)
                    )
data(RateCuts)
head(RateCuts)



firm.returns = StockPriceReturns
event.list = SplitDates
event.window = 10
is.levels =  FALSE
type = None
to.remap = TRUE
remap = "cumsum"
inference = TRUE
inference.strategy = "bootstrap"
model.args = NULL


prepare.returns <- function(event.list, event.window, ...) {
  returns <- unlist(list(...), recursive = FALSE)
  other.returns.names <- names(returns)[-match("firm.returns", names(returns))]

  if (length(other.returns.names) != 0) { # check for type = "None"
    # and "constantMeanReturn"
    returns.zoo <- lapply(1:nrow(event.list), function(i) {
      firm.name <- event.list[i, "name"]
      # to pick out the common dates
      # of data. can't work on event
      # time if the dates of data do
      # not match before converting
      # to event time.
      # all = FALSE: pick up dates
      # for which data is available
      # for all types of returns
      firm.merged <- do.call("merge.zoo",
                             c(list(firm.returns = returns$firm.returns[, firm.name]),
                               returns[other.returns.names],
                               all = FALSE, fill = NA))
      ## other.returns.names needs re-assignment here, since "returns"
      ## may have a data.frame as one of the elements, as in case of
      ## lmAMM.
      other.returns.names <- colnames(firm.merged)[-match("firm.returns",
                                                          colnames(firm.merged))]

      firm.returns.eventtime <- phys2eventtime(z = firm.merged,
                                               events = rbind(
                                                 data.frame(name = "firm.returns", when = event.list[i, "when"],
                                                            stringsAsFactors = FALSE),
                                                 data.frame(name = other.returns.names, when = event.list[i, "when"],
                                                            stringsAsFactors = FALSE)),
                                               width = event.window)

      if (any(firm.returns.eventtime$outcomes == "unitmissing")) {
        ## :DOC: there could be NAs in firm and other returns in the merged object
        return(list(z.e = NULL, outcomes = "unitmissing")) # phys2eventtime output object
      }

      if (any(firm.returns.eventtime$outcomes == "wdatamissing")) {
        return(list(z.e = NULL, outcomes = "wdatamissing")) # phys2eventtime output object
      }

      if (any(firm.returns.eventtime$outcomes == "wrongspan")) {
        ## :DOC: there could be NAs in firm and other returns in the merged object
        return(list(z.e = NULL, outcomes = "wrongspan")) # phys2eventtime output object
      }

      firm.returns.eventtime$outcomes <- "success" # keep one value

      colnames(firm.returns.eventtime$z.e) <- c("firm.returns", other.returns.names)
      ## :DOC: estimation period goes till event time (inclusive)
      attr(firm.returns.eventtime, which = "estimation.period") <-
        as.character(index(firm.returns.eventtime$z.e)[1]:(-event.window))

      return(firm.returns.eventtime)
    })
    names(returns.zoo) <- 1:nrow(event.list)

  } else {

    returns.zoo <- lapply(1:nrow(event.list),  function(i) {
      firm.returns.eventtime <- phys2eventtime(z = returns$firm.returns,
                                               events = event.list[i, ],
                                               width = event.window)
      if (any(firm.returns.eventtime$outcomes == "unitmissing")) {
        return(list(z.e = NULL, outcomes = "unitmissing"))
      }

      if (any(firm.returns.eventtime$outcomes == "wdatamissing")) {
        return(list(z.e = NULL, outcomes = "wdatamissing"))
      }

      if (any(firm.returns.eventtime$outcomes == "wrongspan")) {
        return(list(z.e = NULL, outcomes = "wrongspan"))
      }
      firm.returns.eventtime$outcomes <- "success"
      attr(firm.returns.eventtime, which = "estimation.period") <-
        as.character(index(firm.returns.eventtime$z.e)[1]:(-event.window))
      return(firm.returns.eventtime)
    })
    names(returns.zoo) <- 1:nrow(event.list)
  }
  return(returns.zoo)
}


if (type == "None" && !is.null(firm.returns)) {
  outputModel <- firm.returns
  if (length(model.args) != 0) {
    warning(deparse("type"), " = ", deparse("None"),
            " does not take extra arguments, ignoring them.")
  }
}

if (!(type %in% c("None", "constantMeanReturn")) && is.null(model.args)) {
  stop("model.args cannot be NULL when 'type' is not 'None' or 'constantMeanReturn'.")
}

if (is.levels == TRUE) {
  firm.returns <- diff(log(firm.returns)) * 100
}

## handle single series
if (is.null(ncol(firm.returns))) {
  stop("firm.returns should be a zoo series with at least one column. Use '[' with 'drop = FALSE'.")
}

stopifnot(!is.null(remap))

## compute estimation and event period
## event period starts from event time + 1
event.period <- as.character((-event.window + 1):event.window)



returns.zoo <- prepare.returns(event.list = event.list,
                               event.window = event.window,
                               list(firm.returns = firm.returns))
outcomes <- do.call(c, sapply(returns.zoo, '[', "outcomes"))
names(outcomes) <- gsub(".outcomes", "", names(outcomes))
if (all(unique(outcomes) != "success")) {
  message("Error: no successful events")
  to.remap = FALSE
  inference = FALSE
  outputModel <- NULL
} else {
  returns.zoo <- returns.zoo[which(outcomes == "success")]
  outputModel <- lapply(returns.zoo, function(firm) {
    if (is.null(firm$z.e)) {
      return(NULL)
    }
    estimation.period <- attributes(firm)[["estimation.period"]]
    abnormal.returns <- firm$z.e[event.period]
    return(abnormal.returns)
  })
  null.values <- sapply(outputModel, is.null)
  if (length(which(null.values)) > 0) {
    outputModel <- outputModel[names(which(!null.values))]
    outcomes[names(which(null.values))] <- "edatamissing"
  }

  if (length(outputModel) == 0) {
    warning("None() returned NULL\n")
    outputModel <- NULL
  } else {
    outputModel <- do.call(merge.zoo,
                           outputModel[!sapply(outputModel,
                                               is.null)])
  }
}

if (is.null(outputModel)) {
  final.result <- list(result = NULL,
                       outcomes = as.character(outcomes))
  class(final.result) <- "es"
  return(final.result)
} else if (NCOL(outputModel) == 1) {
  event.number <- which(outcomes == "success")
  message("Only one successful event: #", event.number)
  attr(outputModel, which = "dim") <- c(length(outputModel) , 1)
  attr(outputModel, which = "dimnames") <- list(NULL, event.number)
  if (inference == TRUE) {
    warning("No inference strategy for single successful event.","\n")
    inference <- FALSE
  }
}

if (to.remap == TRUE) {
  outputModel <- switch(remap,
                        cumsum = remap.cumsum(outputModel,
                                              is.pc = FALSE, base = 0),
                        cumprod = remap.cumprod(outputModel,
                                                is.pc = TRUE,
                                                is.returns = TRUE, base = 100),
                        reindex = remap.event.reindex(outputModel)
  )
  car <- outputModel
  if(inference == FALSE){
    if(NCOL(outputModel) != 1){
      outputModel <- rowMeans(outputModel)
    } else {
      mean(outputModel)
    }
  }
  remapping <- remap
} else {
  remapping <- "none"
}
