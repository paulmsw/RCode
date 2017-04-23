#!/usr/bin/env Rscript
library("optparse")

option_list = list(
  make_option(c("-c", "--cores"), type="numeric", default=30, 
              help="set number of cores for parallelism", metavar="numeric"),
  make_option(c("-i", "--id"), type="numeric", default=63, 
              help="set the client ID [default= %default], (ie MoneyGuru)", metavar="numeric"),
  make_option(c("-d", "--days"), type="numeric", default=90, 
              help="set number of days to be processed [default= %default]", metavar="numeric")
  
); 

opt_parser = OptionParser(option_list=option_list);
opt = parse_args(opt_parser);



library(httr)
library(jsonlite)
library(dplyr)
library(doParallel)
library(foreach)
library(RMySQL)
library(RColorBrewer)
library(parallel)
library(doMC)
library(tidyr)
library(data.table)

options(scipen=999)

ctrmodel <- read.csv("~/Data/sales/ctrmodel.csv")
ctrmod <- select(ctrmodel, 2,3)
rm(ctrmodel)
options(stringsAsFactors=T)
NumberOfCluster <- opt$cores
setwd("~/Data/RandD/RCode")


# Set Variables
now <- as.Date(Sys.Date())
endDate <- now - 1
lag <- opt$days
startDate <- endDate - lag
limit <- "99999999"
clientID <- opt$id

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




cl <- makeCluster(NumberOfCluster, outfile = "")
registerDoMC(cores = NumberOfCluster)



require(stringr)
root <- "~/Data/RandD/AppData/"
setwd(root)


# File system shinannigans 
dirname <- paste(now, clientID, sep = "-")
newpath <- paste(root, dirname, sep = "" )

#Check for today's client folder and if it's not there, put it there.
if (!dir.exists(dirname)) {
  dir.create(newpath, showWarnings = TRUE, recursive = FALSE, mode = "0777")
  print(paste("creating dir:", newpath, sep = " " ))
} 

#Go to client directory
setwd(newpath)
print(paste("switched to:", newpath, sep = " " ))

#Check for logs dir...  and if it's not there, put it there.
if (!dir.exists(paste(dirname,"logs", sep="/"))) {
  dir.create(paste(newpath,"logs", sep="/"), showWarnings = TRUE, recursive = FALSE, mode = "0777")
  print(paste("creating dir:", newpath, sep = " " ))
} 


#create the log.txt
writeLines(c(""), "logs/log.txt")
# redirect output to logs
sink("logs/log.txt", append = TRUE)

totalstartTime <-  Sys.time()
foreach(i = 1:lenDF, .packages = "tidyr") %do% {

  loopstartTime <- Sys.time() 
  a <- i / 100 ; b <- (i-1) / 100;  c <- floor(b) ; d <- floor(a)
  trigger <- d - c #1 or 0 
  print(i)
  j <- keywords[i,]
  print(colnames(j))
  print(lenDF - i)
  setTxtProgressBar(pb, i) 
  r <- rbind(queryRankings(j[,2], j[,4] , startDate, endDate, limit))
  
  if((trigger == 1) | (i == lenDF)) {
  
    # Make Filename Usable
    kw_no_space <-str_replace_all(j[,4],"[^a-zA-Z0-9]", "_") 
    filename <- paste("out", kw_no_space,startDate, ".csv", sep = "_")
   
    
    r2 <- r  %>% dplyr::select(2,3,4,5)
    names(r2) <- c("Key","Position", "Url", "Domain")
    
    
    r3 <- as.data.table(inner_join(r2, ctrmod, by = "Position" ))
    r4 <- r3[,Keyword:=lapply(Key, function(x) x[2])]
    r5 <- r4[,Year:=lapply(Key, function(x) x[3])]
    r6 <- r5[,Month:=lapply(Key, function(x) x[4])]
    r7 <- r6[,Day:=lapply(Key, function(x) x[5])]
    r8 <- r7[,Country:=lapply(Key, function(x) x[1])]
    
    
    r8$Date <- as.Date(paste(r8$Day,  r8$Month, r8$Year, sep = "-"), format = "%Y-%m-%d")
    r8$Key  <- NULL
    r8$Year <- NULL
    r8$Month <- NULL
    r8$Day <- NULL
    
    r8$Keyword<- as.factor(unlist(r8$Keyword))
    r8$Country<- as.factor(unlist(r8$Country))
    
    
    print(paste("writing file:", filename, sep = "")) 
    write.csv(r8,filename)
    x <- r8  

  
  
    assign("x", filename, pos = parent.frame())
    
    #Remove old object and collect garbage 
    rm(r8); gc()
    loopendTime <- Sys.time()
    loopDuration <- (loopendTime - loopstartTime)
    print(paste("File output branch time:", loopDuration , sep = ""))
   
    loopDuration <<- loopDuration 
    
  } else {
    print("fetching more data")
    loopendTime <- Sys.time()
    loopDuration <- (loopendTime - loopstartTime)
    
    print(paste("Standard loop time:", loopDuration, sep = ""))
  }
  
  
}
sink()
totalendTime <-  Sys.time()
totalDuration <- totalendTime - totalstartTime
print(paste("Total processing time:", totalDuration, sep = ""))

close(pb)
stopCluster(cl)



library(sendmailR)


# sent to the cunt who's staying in the office till 7am playing slots and getting loaded

from <- "ranking_logs@localhost"
to <- "rightlaugh@gmail.com"
subject <- paste("logs processing for client:", opt$id, "at:", totalendTime, sep = " " )

l1 <- sprintf("Processing started at: %s and finished at: %s", totalstartTime, totalendTime)
l2 <- sprintf("Completed in: %s minutes, across %s cores", totalDuration, opt$cores)
l3 <- sprintf("Time frame extracted: %s days from %s to %s", opt$days, startDate, endDate)
l4 <- sprintf("CSV output at: %s", newpath)
l5 <- sprintf("Log files at: %s/logs/log.txt", newpath)
body <-  paste(l1, l2, l3, l4, l5, sep = "\n\n")


mailControl=list(smtpServer="127.0.0.1")

sendmail(from=from,to=to,subject=subject,msg=body,control=mailControl)

## printf '\033[2J'  <- clears terminal

