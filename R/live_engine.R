library(data.table)
library(httr)
library(findata)
library(jsonlite)
library(mailR)


# IB globals
ACCOUNT_ID  = "DU6463846"
CONTRACT_ID = 134770228
CONTRACT_ID_TLT = 134770764
IB_URL = "https://localhost:5000"

# init IB
# DU6463846	cgsdel955	19$wbG7cs4	Exuber Agg
# docker run --env IBEAM_ACCOUNT='cgsdel955' --env IBEAM_PASSWORD='19$wbG7cs4' -p 5000:5000 voyz/ibeam
parsed_iburl = httr::parse_url(IB_URL)
ib = IBREST2$new(domain = parsed_iburl$hostname,
                 port = as.integer(parsed_iburl$port))
get_req_test <- tryCatch(
  ib$ib_get(),
  error = function(e)
    NULL
)
post_req_test <- tryCatch(
  ib$ib_post(),
  error = function(e)
    NULL
)

# in docs it says we need to call this endoint in the begining
accounts_endpoint <- ib$ib_get(paste0(IB_URL, "/v1/api/iserver/accounts"))
accounts <- unlist(accounts_endpoint$accounts)
portfolio_accounts <- ib$ib_get(paste0(IB_URL, "/v1/api/portfolio/accounts"))
portfolio_accounts <- rbindlist(portfolio_accounts, fill = TRUE)

# send email
send_email <- function(sub = "Exuber strategy - Order",
                       message = "Buy SPY") {
  send.mail(
    from = "mislav.sagovac@contentio.biz",
    to = c("mislav.sagovac@contentio.biz"),
    subject = sub,
    body = message,
    encoding = "utf-8",
    smtp = list(
      host.name = "mail.contentio.biz",
      port = 587,
      user.name = "mislav.sagovac+contentio.biz",
      passwd = "s8^t5?}r-x&Q"
    ),
    authenticate = TRUE,
    html = TRUE
  )
}

# order wrapper
order = function(contractid, side, quantity, coid) {
  # debug
  # contractid = CONTRACT_ID
  # side = "BUY"
  # quantity = 1000
  # coid = paste0("Sell BIL CFD MANUALLY at ")

  # ping or init sessions
  print("Init sessions")
  ib$ib_get()
  ib$ib_post()
  ib$ib_get(paste0(IB_URL, "/v1/api/iserver/accounts"))
  ib$get_position(ACCOUNT_ID, CONTRACT_ID)
  ib$get_portfolio_summary(ACCOUNT_ID)
  Sys.sleep(1L)

  # order body
  print("Create body")
  body = list(
    acctId = ACCOUNT_ID,
    conid = as.integer(contractid),
    sectype = paste0(contractid, ":CFD"),
    orderType = "MKT",
    outsideRTH = FALSE,
    side = side,
    quantity = as.integer(quantity),
    tif = "GTC",
    cOID = ids::random_id()
  )

  # place orders order
  print("Place order")
  url <- paste0(modify_url(ib$baseurl, path = "/v1/api/iserver/account/"),
                ACCOUNT_ID, "/orders")
  print(url)
  body_json = toJSON(list(orders = list(body)), auto_unbox = TRUE)
  print(body_json)
  order_message <- ib$ib_post(url, body = body_json)
  Sys.sleep(1L)
  print("Reply")
  print(order_message)
  url <- paste0(modify_url(ib$baseurl, path = "/v1/api/iserver/reply/"),
                order_message[[1]]$id)
  confirmed <- ib$ib_post(url, body = toJSON(list(confirmed = TRUE), auto_unbox = TRUE))
  order_info = list(order_message = order_message, confirmed = confirmed)
  # order_info = ib$buy_and_confirm(ACCOUNT_ID, body)
  Sys.sleep(1L)

  # check order status and wait for 5 minute if it will be executed
  print("Check oders status")
  n_trails = 20
  repeat {
    # stop if order executes or number of trias grater than 10
    if (n_trails <= 0) {
      send_email(sub = "CGS Exuber - order problem",
                 message = "Order is not filled. Check why.")
      break()
    }

    # get order status
    url = paste0(
      "https://",
      parsed_iburl$hostname,
      ":",
      as.integer(parsed_iburl$port),
      "/v1/api/iserver/account/orders"
    )
    orders <- ib$ib_get(url)
    if (length(orders$orders) == 0) {
      Sys.sleep(1L)
      n_trails = n_trails - 1
      next()
    } else {
      last_ = lapply(orders$orders, '[', "lastExecutionTime")
      last_ = as.POSIXct(unlist(last_), format = "%y%m%d%H%M%S")
      orders_last = orders$orders[which.max(last_)]
      order_status = orders_last[[1]]$status

      # check order status
      if (order_status == "Filled") {
        # send e-mail notification
        confirmed = order_info$confirmed
        if (length(confirmed) == 0) {
          send_email(sub = "Exuber CGS Live",
                     message = "Order is not confirmed")
        } else {
          # Convert the list to an HTML formatted string
          my_list = orders_last[[1]]
          html_content <- "<html><body><h2>Order Details</h2><ul>"
          for (name in names(my_list)) {
            html_content <- paste0(html_content, "<li><b>", name, ":</b> ", my_list[[name]], "</li>")
          }
          html_content <- paste0(html_content, "</ul></body></html>")

          # Send email
          send_email(
            sub = "TEST ORDER INFO AFTER TRADE FROM PAPER ACCOUNT",
            message = html_content
          )
        }
        break
      } else {
        Sys.sleep(1L)
        n_trails = n_trails - 1
      }
    }
  }
  return(1L)

  # cancel order
  # url = paste0("https://", parsed_iburl$hostname, ":", as.integer(parsed_iburl$port),
  #              "/v1/api/iserver/account/", ACCOUNT_ID,
  #              "/order/", orders$orders[[2]]$orderId)
  # DELETE(url, config = httr::config(ssl_verifypeer = FALSE, ssl_verifyhost = FALSE))
}
