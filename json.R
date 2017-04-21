library(httr)
library(jsonlite)
library(dplyr)
library(tidyjson)
library(RJSONIO)
library(lubridate)


setwd("~/Data/RandD/RCode")
## options
options(stringsAsFactors=T)

ca_user <- "mediaskunkworks"
ca_pass <- "PSpakr4GSYfQ"
response <- GET("https://mediaskunkworks.cloudant.com/google/_design/rankings/_view/positions?start_key=[%22UK%22,%20%22loans%20for%2020000%22,%202017,%204,%201,%20%22a%22]&end_key=[%22UK%22,%20%22loans%20for%2020000%22,%202017,%204,%205,%20%22zzzzzzzzzzzzzzzzzzzzzzz%22]&limit=1000&reduce=false", authenticate(ca_user, ca_pass))

api_r <- content(response, as="text")
api_r <- jsonlite::fromJSON(api_r,simplifyVector = TRUE, flatten=TRUE)

d <- as.data.frame(api_r[3])
d





flattenlist <- function(x){
  morelists <- sapply(x, function(xprime) class(xprime)[1]=="list")
  out <- c(x[!morelists], unlist(x[morelists], recursive=FALSE))
  if(sum(morelists)){
    Recall(out)
  }else{
    return(out)
  }
}


d$flat <- flattenlist(d$rows.key)

