library(pushshiftR)
library(httr)

datetime_seq <- seq.POSIXt(as.POSIXct('2021-01-01 00:00:00'),
                           as.POSIXct('2021-01-01 01:00:00'),
                           by="min")
datetime_seq_unix <- as.numeric(datetime_seq)
x <- getPushshiftData(postType = "comment",
                      size = 1000,
                      after = "1546300800",
                      subreddit = "soccer",
                      nest_level = 1)
as.POSIXct(1546300000, origin = '1970-01-01')
as.POSIXct(1546400000, origin = '1970-01-01')

head(x)

gme <- getPushshiftDataRecursive(postType = "submission",
                                 title = "GME",
                                 size = 100,  # 100 is max
                                 after = datetime_seq_unix[1],
                                 subreddit = 'wallstreetbets',
                                 nest_level = 1
)

head(gme)


? jsonlite::fromJSON()

#' An operator for 'not in'
#' @param x
#' @param y
#' @return The complement of x %in% y
'%not_in%' <- function(x,y)!('%in%'(x,y))


#' Gets the pushshift data
#'
#' @param postType One of `submission` or `comment`
#' @param title A string to search for in post titles
#' @param size Number of results to return, maximum is 1000
#' @param q A query to search for
#' @param after Only search for posts made after this data, specified as a UNIX epoch time
#' @param before As `after`, but before
#' @param subreddit Only return posts made in this subreddit
#' @param nest_level How deep to search? `nest_level = 1` returns only top-level comments
#' @return A tibble of reddit submissions
#' @export
#' @importFrom jsonlite fromJSON
#' @importFrom magrittr %>%
#' @importFrom dplyr select filter
#' @import tibble
getPushshiftData <- function(postType, ...) {
  if(postType == "submission") {
    getPushshiftURL(postType, ...) %>%
      jsonlite::fromJSON() %>%
      .$data %>%
      jsonlite::flatten(recursive = TRUE) %>%
      select(author, title, selftext, created_utc, id, num_comments, score, subreddit) %>%
      as_tibble()
  } else {
    getPushshiftURL(postType, ...) %>%
      jsonlite::fromJSON() %>%
      .$data %>%
      jsonlite::flatten(recursive = TRUE) %>%
      select(author, body, parent_id, score, created_utc, subreddit) %>%
      as_tibble()
  }
}


x <- RETRY('GET', getPushshiftURL('submission', postType = "comment",
                           size = 1000,
                           after = "1546300800",
                           subreddit = "soccer",
                           nest_level = 1), )
jsonlite::fromJSON(httr::content(x, as = 'text'))$data

#' Gets the pushshift URL
#'
#' @param postType One of `submission` or `comment`
#' @param title A string to search for in post titles
#' @param size Number of results to return, maximum is 1000
#' @param q A query to search for
#' @param after Only search for posts made after this data, specified as a UNIX epoch time
#' @param before As `after`, but before
#' @param subreddit Only return posts made in this subreddit
#' @param nest_level How deep to search? `nest_level = 1` returns only top-level comments
#' @return A URL
#' @export
#' @importFrom jsonlite fromJSON
#' @importFrom magrittr %>%
getPushshiftURL <- function(postType = "submission",
                            title = NULL,
                            size = NULL,
                            q = NULL,
                            after = NULL,
                            before = NULL,
                            subreddit = NULL,
                            nest_level = NULL) {
  if(postType %not_in% c("submission", "comment")) {
    stop("postType must be one of `submission` or `comment`")
  }
  return(paste("https://api.pushshift.io/reddit/search/",
               postType,
               "?",
               ifelse(is.null(title), "", sprintf("&title=%s", title)),
               ifelse(is.null(size), "", sprintf("&size=%s", size)),
               ifelse(is.null(q), "", sprintf("&q=%s", q)),
               ifelse(is.null(after), "", sprintf("&after=%s", after)),
               ifelse(is.null(before), "", sprintf("&before=%s", before)),
               ifelse(is.null(subreddit), "", sprintf("&subreddit=%s", subreddit)),
               ifelse(is.null(subreddit), "", sprintf("&nest_level=%s", nest_level)),
               sep = ""))
}

#' Repeats getPushshiftData until desired period covered
#' @param postType One of `submission` or `comment`
#' @param title A string to search for in post titles
#' @param size Number of results to return, maximum is 1000
#' @param q A query to search for
#' @param after Only search for posts made after this data, specified as a UNIX epoch time
#' @param before As `after`, but before
#' @param subreddit Only return posts made in this subreddit
#' @param nest_level How deep to search? `nest_level = 1` returns only top-level comments
#' @param delay Number of seconds to wait between queries to avoid stressing out the Pushshift server (limit is somewhere around 200 queries per minute)
#' @return A tibble of the requested data
#' @export
#' @importFrom jsonlite fromJSON
#' @importFrom magrittr %>%
#' @importFrom dplyr last
#' @importFrom tibble add_case
getPushshiftDataRecursive <- function(postType = "submission",
                                      title = NULL,
                                      size = NULL,
                                      q = NULL,
                                      after = NULL,
                                      before = NULL,
                                      subreddit = NULL,
                                      nest_level = NULL,
                                      delay = 0) {
  tmp <- getPushshiftData(postType,
                          title,
                          size,
                          q,
                          after,
                          before,
                          subreddit,
                          nest_level)
  out <- tmp %>% filter(FALSE)
  on.exit(return(out), add = TRUE)
  after <- last(tmp$created_utc)
  while(nrow(tmp) > 0) {
    print(
      sprintf("%d %ss fetched, last date fetched: %s",
              nrow(tmp),
              postType,
              as.Date(as.POSIXct(as.numeric(after), origin = "1970-01-01"))))
    out <- rbind(out, tmp)
    after <- last(tmp$created_utc)
    tmp <- getPushshiftData(postType,
                            title,
                            size,
                            q,
                            after,
                            before,
                            subreddit,
                            nest_level)
    Sys.sleep(delay)
  }
}
