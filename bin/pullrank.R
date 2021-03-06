#!/usr/bin/env Rscript
library("optparse")


option_list = list(
  # make_option(c("-c", "--cores"), type="numeric", default=30,
  #              help="set number of cores for parallelism", metavar="numeric"),
  make_option(c("-i", "--id"), type="numeric", default=NULL,
              help="set the client ID eg: -i 63,  (will return results for MoneyGuru)", metavar="numeric"),
  make_option(c("-d", "--days"), type="numeric", default=90,
              help="set number of days to be processed [default= %default]", metavar="numeric"),
  make_option(c("-s", "--start"), type="numeric", default=1,
              help="set increment start value [default= %default]", metavar="numeric"),
  make_option(c("-o", "--ofs"), type="numeric", default=0,
              help="set lag in days[default= %default]", metavar="numeric")
);

opt_parser = OptionParser(option_list=option_list);
opt = parse_args(opt_parser);

if (is.null(opt$id)){
  print_help(opt_parser)
  stop("At least one argument must be supplied: clientID, using ut -i switch \n\nExample: pullrank.R -i 63 -d 90 (to pull MoneyGuru.com's data for the past 90 days)\n\n\n", call.=FALSE)
}



   #  opt <- NULL
   # opt$start <- 1
   #   opt$cores <- 20
   #  opt$id <- 54
   # opt$days <- 90
   #  opt$ofs <- 0

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
endDate <- now - opt$ofs
lag <- opt$days
startDate <- endDate - lag
limit <- "99999999"
clientID <- opt$id

print("startDate:")
print(as.character(startDate))

print("endDate:")
print(as.character(endDate))

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
start <- opt$start



pb <- txtProgressBar(min = 1, max = lenDF, style = 3)




#cl <- makeCluster(NumberOfCluster, outfile = "")
#registerDoMC(cores = NumberOfCluster)


r1<-NULL
r<-NULL

require(stringr)
root <- "~/Data/RandD/AppData/"
setwd(root)


# File system shinannigans
dirname <- paste(endDate, clientID, sep = "-")
newpath <- paste(root, dirname, sep = "" )
newpath
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

#redirect output to logs
sink("logs/log.txt", append = TRUE)

#loopstart ###################################################################################
totalstartTime <-  Sys.time()
foreach(i = opt$start:lenDF, .packages = c("tidyr", "data.table", "stringr")) %do% {
  
  loopstartTime <- Sys.time()
  
  
  j <- keywords[i,]
  print(j)
  print(as.character(j[,4]))
  print(lenDF - i)
  setTxtProgressBar(pb, i)
  
  r2 <- queryRankings(j[,2], as.character(j[,4]) , startDate, endDate, limit)
  
  category <- as.character(j[,5])
  
  
  
  cat <- c(category);
  r2 <- cbind(data.frame(date = r2), Category = cat);
  
  r <- rbindlist(list(r, r2))
  
  
  a <- i / 10 ; b <- (i-1) / 10;  c <- floor(b) ; d <- floor(a)
  trigger <- d - c #1 or 0
  
  ##################################
  if((trigger == 1) | (i == lenDF)) {
    
    print("triggered here ################################## ")
    print(i)
    
    # Make Filename Usable
    kw_no_space <-str_replace_all(as.character(j[,4]),"[^a-zA-Z0-9]", "_")
    filename <- paste("out", kw_no_space,now , ".RDS", sep = "_")
    fullpath  <- paste(newpath,filename, sep = "/")
    
    r1 <- r  %>% dplyr::select( 2,3,4,5, 6)
    names(r1) <- c("Key","Position", "Url", "Domain", "Category")
    r1 <- as.data.table(inner_join(r1, ctrmod, by = "Position" ))
    r1 <- r1[,Keyword:=lapply(Key, function(x) x[2])]
    r1 <- r1[,Year:=lapply(Key, function(x) x[3])]
    r1 <- r1[,Month:=lapply(Key, function(x) x[4])]
    r1 <- r1[,Day:=lapply(Key, function(x) x[5])]
    r1 <- r1[,Country:=lapply(Key, function(x) x[1])]
    
    r1$Date <- as.Date(paste(r1$Day, r1$Month, r1$Year, sep = "-"), format = "%d-%m-%Y")
    r1$Key  <- NULL
    r1$Year <- NULL
    r1$Month <- NULL
    r1$Day <- NULL
    
    r1$Keyword<- as.factor(unlist(r1$Keyword))
    r1$Country<- as.factor(unlist(r1$Country))
    r1$Domain<- as.factor(r1$Domain)
    r1$Category<- as.factor(r1$Category)
    
    
    print(paste("writing file: ", filename, sep = ""))
    
    
    
    rupper <<- r1
    
    saveRDS(rupper,fullpath)
    
    
    
    #Remove old object and collect garbage
    rm(r1)
    gc()
    r <- NULL
    loopendTime <- Sys.time()
    loopDuration <- (loopendTime - loopstartTime)
    print(paste("File output branch time:", loopDuration , sep = ""))
    
    loopDuration <<- loopDuration
    #############################################################
  } else {
    print("fetching more data")
    loopendTime <- Sys.time()
    loopDuration <- (loopendTime - loopstartTime)
    
    print(paste("main loop bottom time:", loopDuration, sep = ""))
  }
  
  #######################################################################
  
}



totalendTime <-  Sys.time()
totalDuration <- totalendTime - totalstartTime
print(paste("Total processing time:", totalDuration, sep = ""))

close(pb)
#stopCluster(cl)

sink()

library(sendmailR)


# send confirmation of completed job.

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

#
#
#