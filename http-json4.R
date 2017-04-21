


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

ctrmodel <- read.csv("~/Data/sales/ctrmodel.csv")
ctrmod <- select(ctrmodel, 2,3)

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




flattenlist <- function(x){
  morelists <- sapply(x, function(xprime) class(xprime)[1]=="list")
  out <- c(x[!morelists], unlist(x[morelists], recursive=FALSE))
  if(sum(morelists)){
    Recall(out)
  }else{
    return(out)
  }
}




pb <- txtProgressBar(min = 1, max = lenDF, style = 3)



#library(doSNOW)
#NumberOfCluster <- 30
#cl <- makeCluster(NumberOfCluster, outfile = "")
#registerDoParallel(cl)

x <- NULL
tmp <- NULL
output <- NULL

tmp <- as.data.frame(tmp)

require(stringr)
root <- "~/Data/RandD/AppData/"
setwd(root)


dirname <- paste(now, clientID, sep = "-")
newpath <- paste(root, dirname, sep = "" )

if (!dir.exists(dirname)) {
  dir.create(newpath, showWarnings = TRUE, recursive = FALSE, mode = "0777")

} 

setwd(newpath)


foreach(i = 1:lenDF-1) %do% {
  starttime <- Sys.time() 
    a <- i / 100 ; b <- (i-1) / 100;  c <- floor(b) ; d <- floor(a)
    trigger <- d - c #1 or 0 
    print(i)
    print(paste("trigger:", trigger, sep = " " ))
   
     if((trigger == 1) | (i == lenDF-1)){  
      
      j <- keywords[i,]
      print(colnames(j))
      print(lenDF - i)
      setTxtProgressBar(pb, i) 
      r <- rbind(queryRankings(j[,2], j[,4] , startDate, endDate, limit))
      
    
      
      print(colnames(output))
      print(colnames(r))
    
      print(clientID)
      
      print(file)
      } else if (trigger == 0) { 
      print(colnames(output))
      kw_no_space <-str_replace_all(j[,4],"[^a-zA-Z0-9]", "_") 
  
      file <- paste(clientID,j[,2], kw_no_space,startDate, endDate, ".csv", sep = "_")
  
      print(file)
      
      r2 <- r  %>% dplyr::select(2,3,4,5)
      
      names(r2) <- c("Key","Position", "Url", "Domain")
      r3 <- as.data.table(inner_join(r2, ctrmod, by = "Position" ))
      r4 <- r3[,Keyword:=lapply(Key, function(x) x[2])]
      r5 <- r4[,Year:=lapply(Key, function(x) x[3])]
      r6 <- r5[,Month:=lapply(Key, function(x) x[4])]
      r7 <- r6[,Day:=lapply(Key, function(x) x[5])]
      r8 <- r7[,Country:=lapply(Key, function(x) x[1])]
      r8$Date <- as.Date(paste(r8$Year, r8$Month, r8$Day, sep = "-"), format = "%Y-%m-%d")
      r8$Key  <- NULL
      r8$Year <- NULL
      r8$Month <- NULL
      r8$Day <- NULL
      
      write.csv(r8,file)
      endtime <- Sys.time()
      print(starttime - endtime)
      } 
}

    



# column_names <- data.frame(id = "rows.id", key = "key", 
#                           position = "position",
#                           url = "url",
#                           domain = "domain",
#                           kw = "kw")



close(pb)
#stopCluster(cl)








setwd("~/Data/RandD/RCode")

