


library(httr)
library(jsonlite)
library(dplyr)
library(doParallel)
library(foreach)
library(RMySQL)
library(RColorBrewer)
library(parallel)
library(doSNOW)
library(doMC)

library(data.table)

options(stringsAsFactors=T)

setwd("~/Data/RandD/RCode")

# Set Variables
now <- as.Date(Sys.Date())
endDate <- now - 1
lag <- 90
startDate <- endDate - lag
limit <- "99999999"
clientID <- 63



# Define DB Connection
library(RMySQL)
options(mysql = list(
  "host" = "10.0.160.45",
  "port" = 3306,
  "user" = "paulreilly",
  "password" = "ut5bTEALj4Ff8WrZ"))
databaseName <- "ranking-report"
table <- "rankings-history"

loadKeywords <- function() {
  # Connect to the database
  db <- dbConnect(MySQL(), dbname = databaseName, host = options()$mysql$host,
                  port = options()$mysql$port, user = options()$mysql$user,
                  password = options()$mysql$password)
  #Contruct Query ========
  query <- sprintf("SELECT * FROM \`keywords\` WHERE \`client\` = \'%s\'", clientID)
  # Submit the fetch query and disconnect
  data <- dbGetQuery(db, query)
  dbDisconnect(db)
  data[sapply(data, is.character)] <- lapply(data[sapply(data, is.character)], as.factor)
  data
}


keywords <- unique(loadKeywords())




queryRankings <- function(country, keyword , startDate, endDate, limit) {
  ca_user <- "mediaskunkworks"
  ca_pass <- "PSpakr4GSYfQ"
  library(httr)

  #DeConstruct Query
  library(lubridate)
  library(data.table)
  startYear <- year(startDate)
  startMonth <- month(startDate)
  startDay <- day(startDate)

  endYear <- year(endDate)
  endMonth <- month(endDate)
  endDay <- day(endDate)


  #Construct Query
  query <-  sprintf("https://mediaskunkworks.cloudant.com/google/_design/rankings/_view/positions?start_key=[\"%s\", \"%s\", %s, %s, %s, \"a\"]&end_key=[\"%s\", \"%s\", %s, %s, %s, \"zzzzzzzzzzzzzzzzzzzzzzz\"]&limit=%s&reduce=false",
                    country, keyword, startYear, startMonth, startDay, country, keyword, endYear, endMonth, endDay, limit)
  encQuery <- URLencode(query)
  response <- GET(encQuery, authenticate(ca_user, ca_pass))
  api_r <- content(response, as="text")
  api_r <- jsonlite::fromJSON(api_r,simplifyVector = TRUE, flatten=TRUE)

  return(as.data.table(api_r[3]))
}


lenDF <- nrow(keywords)






pb <- txtProgressBar(min = 1, max = lenDF, style = 3)


library(doSNOW)
NumberOfCluster <- 30
cl <- makeCluster(NumberOfCluster, outfile = "")
registerDoSNOW(cl)
x0 <- NULL
tmp <- NULL
output <- NULL

i =  10


((i - lenDF) + lenDF)
lenDF - 240

lenDF - 379

x0 <- foreach(i = 250:379, .combine = rbind) %dopar% {
  starttime <- Sys.time()
  j <- keywords[i,]
  print(lenDF - i)
  tmp <- queryRankings(j[,2], j[,4] , startDate, endDate, limit)
  setTxtProgressBar(pb, i)
  endtime <- Sys.time()
  print( endtime - starttime)
  return(tmp)
}

close(pb)
stopCluster(cl)




setwd("~/Data/RandD/AppData/")

#Save Output
saveRDS(x0,"datatoday.rds")

setwd("~/Data/RandD/RCode")

