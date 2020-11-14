#' Apply data.tables frollapply in parallel
#'
#' @description Function applies data.tables frollapply function in parallel by divding vector to subsamples.
#'
#' @param y vector, list, data.frame or data.table of numeric or logical columns.
#' @param n_cores number of cores to use.
#' @param roll_window integer vector, for adaptive rolling function also list of integer vectors, rolling window size.
#' @param FUN the function to be applied in rolling fashion
#' @param fill numeric, value to pad by. Defaults to NA.
#' @param align character, define if rolling window covers preceding rows ("right"), following rows ("left") or centered ("center"). Defaults to "right".
#'
#' @return vector is returned
#' @export
#'
#' @importFrom data.table frollapply
#' @importFrom future.apply future_lapply
#'
#' @examples
frollapply_parallel <- function(y, n_cores = 4, roll_window = 600, FUN, fill = NA, align = 'right') {

  # measure time
  start <- Sys.time()

  # divide vector to subvectors
  seq_index <- as.integer(seq(1, length(y), length.out = n_cores))

  # test if there are enough data in subsamples
  if ((roll_window - (seq_index[2] - seq_index[1])) > 0) {
    stop('Subsample length is lower than the rolling length. Decrease the rolling window or the number of cores.')
  }

  # generate subsamples
  subsample <- list()
  loop_index <- seq_along(seq_index)
  for (i in loop_index) {
    if (i == 1) {
      subsample[[i]] <- y[seq_index[i]:seq_index[i+1]]
    } else if (i == loop_index[length(loop_index)]) {
      next()
    } else {
      subsample[[i]] <- y[((seq_index[i]+1) - roll_window+1):seq_index[i+1]]
    }
  }

  # apply rolling function on subsamples
  roll_par <- future_lapply(
    subsample,
    FUN = function(x_subsample) {
      data.table::frollapply(
        x = x_subsample,
        n = roll_window,
        FUN = FUN,
        fill = fill,
        align = align
      )
    })

  # measure required time
  end <- Sys.time()
  print(paste0('The fucntion took ', start - end))

  return(roll_par)
}


slider_parallel <- function(.x, .f, .before = 0L, .after = 0L, .complete = FALSE, n_cores = -1) {

  # measure time
  start <- Sys.time()

  # get length of rolling window
  roll_window <- 1 + .before + .after

  # max # cluster
  if (n_cores == -1) {
    n_cores <- as.integer(nrow(.x) / max(roll_window))
  }
  print(paste0('Number of subsamples (max used cores): ', n_cores))

  # divide vector to subvectors
  seq_index <- as.integer(seq(1, nrow(.x), length.out = n_cores))

  # test if there are enough data in subsamples
  if ((roll_window - (seq_index[2] - seq_index[1])) > 0) {
    stop('Subsample length is lower than the rolling length. Decrease the rolling window or the number of cores.')
  }

  # generate subsamples
  subsample <- list()
  loop_index <- seq_along(seq_index)
  for (i in loop_index) {
    if (i == 1) {
      subsample[[i]] <- .x[seq_index[i]:seq_index[i+1], ]
    } else if (i == loop_index[length(loop_index)]) {
      next()
    } else {
      subsample[[i]] <- .x[((seq_index[i]+1) - roll_window+1):seq_index[i+1], ]
    }
  }

  # apply rolling function on subsamples
  roll_par <- future_lapply(
    subsample,
    FUN = function(x_subsample) {
      x_subsample <- slider::slide(
        .x = x_subsample,
        .f = .f,
        .before = .before,
        .after = .after,
        .complete = .complete
        )
      })
  roll_par <- purrr::flatten(roll_par)

  # measure required time
  end <- Sys.time()
  print(paste0('The fucntion took ', start - end))

  return(roll_par)
}
