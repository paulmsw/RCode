#!/usr/bin/env Rscript
library("optparse")


option_list = list(
  # make_option(c("-c", "--cores"), type="numeric", default=30,
  #              help="set number of cores for parallelism", metavar="numeric"),
  make_option(c("-i", "--id"), type="numeric", default=NULL,
              help="set the client ID eg: -i 63,  (will return results for MoneyGuru)", metavar="numeric"),
  make_option(c("-o", "--out"), type="numeric", default=1,
              help="set log output eg: -o 0, (will turn off logging) \n  [default= %default / ie On]", metavar="numeric"),
  make_option(c("-l", "--lag"), type="numeric", default=0,
              help="set lag in days - [default= %default, ie today]", metavar="numeric")
  
);

opt_parser = OptionParser(option_list=option_list);
opt = parse_args(opt_parser);

if (is.null(opt$id)){
  print_help(opt_parser)
  stop("At least one argument must be supplied: clientID, using the -i switch \n\nExample: summarypipe -i 63 -o 0 (to process MoneyGuru.com's data for the past 90 days)\n\n\n", call.=FALSE)
}


opt <- NULL
# #
# opt <- NULL
 opt$start <- 1
#  opt$cores <- 20
  opt$id <- 63
  opt$lag <- 0
  opt$out <- 1
#  opt$days <- 90


#library(bookdown)
library(dplyr)
library(RcppRoll)
library(RollingWindow)
library(stringr)
library(foreach)
options(stringsAsFactors=T)

cwd <- "~/Data/RandD/AppData/"

kwpdir <- "~/Data/RandD/AppData/kwpdata/"
setwd(kwpdir)

#Change to correct folder

now <- Sys.Date() -(opt$lag)

now <- format(now, format="%Y-%m-%d")
clientID <- opt$id
clientID
dirname <- paste(now, clientID, sep="-")
dirname
fullpath <- paste(cwd, dirname,"/", sep = "")
fullpath
filepath <- paste(cwd, "kwpdata/", clientID, "_kwp.RDS", sep = "")
filepath
print("Processing...")
print(clientID)
print(filepath)


#Check for todays's client folder and if it's not there, put it there.
if (!dir.exists(fullpath)) {
  dir.create(fullpath, showWarnings = TRUE, recursive = FALSE, mode = "0777")
  print(paste("creating dir:", dirname, sep = " " ))
} else {
  
  
  print("Data directory exists")
}


# Do cleaning in preparation for the loop tings!

if (dir.exists(fullpath)) {
  cat("Getting list of files")
  
  filelist  <- list.files(path = fullpath, pattern = "^out", all.files = FALSE,
                          full.names = FALSE, recursive = FALSE,
                          ignore.case = FALSE, include.dirs = FALSE, no.. = FALSE)
  numfiles <- length(filelist)
  
} else {
  stop("No directory of that name run 'pullrank.R' to populate with ranking data\n\n", call.=FALSE)
}


kwp <- readRDS(paste(cwd, "kwpdata/", clientID, "_kwp.RDS", sep = ""))


#####################################

require(stringr)

# #Go to client directory
setwd(fullpath)
 print(paste("switched to:", fullpath, sep = " " ))


 
 
 #Check for logs dir...  and if it's not there, put it there.
 if (!dir.exists(paste(fullpath,"logs", sep="/"))) {
   dir.create(paste(fullpath,"logs", sep="/"), showWarnings = TRUE, recursive = FALSE, mode = "0777")
   print(paste("creating dir:", fullpath, sep = " " ))
 } else {
   print("Log directory exists ")
 }
 
 #Test for output switch to logging console output
 if (opt$out == 1){
   print(paste("piping logs to: ",fullpath,"logs/summarylog.txt", sep = ""))
   #create the summarylog.txt
   writeLines(c(""), paste(fullpath,"logs/summarylog.txt", sep = ""))
   #redirect output to summarylog.txt
   sink(paste(fullpath,"logs/summarylog.txt", sep = ""), append = TRUE)
 } 


foreach(i = 1:numfiles, .packages = c("dplyr", "RollingWindow")) %do% {
  file_i <- readRDS(paste(fullpath, filelist[i], sep = "/"))
  dfm <- inner_join(file_i, kwp, by = "Keyword" )
  dfm$Keyword <- as.factor(dfm$Keyword)
  

  
  dfm$Date <- as.Date(dfm$Date)
  
  minDate <- min(dfm$Date)
  maxDate <- max(dfm$Date)
  
  dataPeriod <- maxDate - minDate
  
  dfm$DayCount <- as.integer(dfm$Date - minDate) + 1
  dfm$Month <- as.numeric(format(dfm$Date, "%m"))
  dfm$Year <- as.numeric(format(dfm$Date, "%Y"))
  
  dfm$Revenue <- ((dfm$Impressions / 30) * dfm$CTR)* dfm$Bid
  dfm$Potential <- ((dfm$Impressions / 30) * 0.325)* dfm$Bid
  dfm$Clicks <- ((dfm$Impressions / 30) * dfm$CTR)
  
  # Sort Aggregations
  DF_Group <- dfm %>% group_by(DayCount, Category, Country, Date, Domain, Keyword, Url, Month, Year) %>%
    arrange(DayCount) %>%
    summarise(pmean = mean(Position),
              rmean = mean(Revenue),
              cmean = mean(Clicks),
              bmean = mean(Bid),
              potmean = mean(Potential),
              imean = mean(Impressions))
  print("finished initial group_by")
  
  #30 day rolling mean
  
  ma_7 <- 7
  ma_30 <- 30
  library(RcppRoll)
  
  
  DF <- ungroup(DF_Group)
  
  # Compute Moving Averages all wrong to date
  by_site <- group_by(DF, Keyword, Domain, Url, Month, Category)
  arrdf <- arrange(by_site, DayCount)
  
  arrdf$pmeanSubbed = NaSub(arrdf$pmean, last_obs = TRUE, maxgap = 3)
  arrdf$rmeanSubbed = NaSub(arrdf$rmean, last_obs = TRUE, maxgap = 3)
  arrdf$cmeanSubbed = NaSub(arrdf$cmean, last_obs = TRUE, maxgap = 3)
  arrdf$bmeanSubbed = NaSub(arrdf$bmean, last_obs = TRUE, maxgap = 3)
  arrdf$imeanSubbed = NaSub(arrdf$imean, last_obs = TRUE, maxgap = 3)
  arrdf$potmeanSubbed = NaSub(arrdf$potmean, last_obs = TRUE, maxgap = 3)
  
  dtroll <- as.data.frame(ungroup(arrdf))
  
  
  dtroll2 <- dtroll
  dtroll2[is.na(dtroll2)] <- 0
  
  
  kw_no_space <-str_replace_all(as.character(dtroll2[1,6]),"[^a-zA-Z0-9]", "_")
  
  
  dtroll2 <-  ungroup(dtroll2)
  #############################
  smon <- dtroll2 %>% group_by(Month, Keyword, Domain, Category ) %>% arrange(Month, Keyword) %>%
    summarise(rsum = sum(rmeanSubbed),
              pmean = mean(pmeanSubbed),
              csum = sum(cmeanSubbed),
              bmean = mean(bmeanSubbed),
              NumberKeywords = n(),
              isum = sum(imeanSubbed),
              potmean = mean(potmeanSubbed))
  
  
 
  
  print("Getting domain parts")
  sdayfn <- as.character(paste(fullpath, "smon_", kw_no_space,opt$id,".RDS", sep = ""))
  
  
  library(tldextract)
  urllist <- as.character(smon$Domain)
  # get most recent TLD listings
  tld <- getTLD() # optionally pass in a different URL than the default
  manyhosts <- c(urllist)
  newurls <- tldextract(urllist, tldnames=tld)
  smon$SLD <- as.factor(newurls$domain)
  smon$Domain <- as.factor(paste(newurls$domain, newurls$tld, sep="."))
  
  


  print("Saving smon ")
  print("for client ID:")
  print(opt$id)
  print("in location:")
  print(sdayfn)
  
  saveRDS(smon, sdayfn)
  #############################
  
  #############################
  dtroll2 <-  ungroup(dtroll2)
  sdaykw <- dtroll2 %>% group_by(DayCount, Month,Category ,Keyword, Domain ) %>%
    arrange(DayCount, Month, Keyword) %>%
    summarise(rsum = sum(rmean),
              pmean = mean(pmean),
              csum = sum(cmean),
              bmean = mean(bmean),
              NumberKeywords = n(),
              isum = sum(imean),
              potmean = mean(potmean))
  
  #Recreate Date Variable as.Date
  sdaykw$Date <- as.Date(minDate + sdaykw$DayCount - 1)
  

  urllist <- as.character(sdaykw$Domain)
  # get most recent TLD listings
  tld <- getTLD() # optionally pass in a different URL than the default
  manyhosts <- c(urllist)
  newurls <- tldextract(urllist, tldnames=tld)
  sdaykw$SLD <- as.factor(newurls$domain)
  sdaykw$Domain <- as.factor(paste(newurls$domain, newurls$tld, sep="."))
  
  
  sdaykwfn <- as.character(paste(fullpath, "sdaykw_", kw_no_space,opt$id,".RDS", sep = ""))
  print("Saving sdaykw")
  print(opt$id)
  print(sday2domfn)
  saveRDS(sdaykw, sdaykwfn)
  dtroll2 <-  ungroup(dtroll2)
  #############################
  
  
  

  sday2dom <- dtroll2 %>% group_by(DayCount, Month,Category ,Keyword, Domain ) %>%
    arrange(DayCount, Month, Keyword) %>%
    summarise(rsum = sum(rmean),
              pmean = mean(pmean),
              csum = sum(cmean),
              bmean = mean(bmean),
              NumberKeywords = n(),
              isum = sum(imean),
              potmean = mean(potmean))
  
  sday2dom$Date <- as.Date(minDate + sday2dom$DayCount - 1)
  
  sday2domfn <- as.character(paste(fullpath, "sday2dom_", kw_no_space,opt$id,".RDS", sep = ""))
  print("Saving sday2domain")
  print(opt$id)
  print(sday2domfn)
  saveRDS(sday2domain, sday2domfn)
  
  print("Saving sday2dom ")
  print("for client ID:")
  print(opt$id)
  print("in location:")
  print(sday2domfn)
  
  saveRDS(sday2dom, sday2domfn)
  
}







# sday2domain <- rbind(readRDS("/home/paul/Data/RandD/AppData/2017-04-24-63/sday2domainloans63.RDS"), readRDS("/home/paul/Data/RandD/AppData/2017-04-24-63/sday2domainpersonal_loans63.RDS"))
# sdaykw <- rbind(readRDS("/home/paul/Data/RandD/AppData/2017-04-24-63/sdaykwloans63.RDS"), readRDS("/home/paul/Data/RandD/AppData/2017-04-24-63/sdaykwpersonal_loans63.RDS"))
# smon <- rbind(readRDS("/home/paul/Data/RandD/AppData/2017-04-24-63/smonloans63.RDS"), readRDS("/home/paul/Data/RandD/AppData/2017-04-24-63/smonpersonal_loans63.RDS"))
# 
# 
# sdaykw$Keyword <- as.factor(sdaykw$Keyword)
# sdaykw$Domain <- as.factor(sdaykw$Domain)
# sdaykw$Category <- as.factor(sdaykw$Category)
# 
# KWCluster_1<- sdaykw %>%
#   group_by(Keyword) %>%
#   filter(pmean == 1) %>% summarise(Revenue = sum(rsum), Impressions = sum(isum), Clicks = sum(csum)) %>% arrange(desc(Revenue))
# 
#saveRDS(kwptemp, "/home/paul/Data/RandD/AppData/kwpdata/1_kwp.RDS")


# 
# 
# uniqueKWs <- as.factor(names(AtribLst(cluscards[cluscards==1], attrC="names", isNullC=NA)))
# 
# subset <- unique(sdaykw[sdaykw$Keyword %in% uniqueKWs, ])
# 
# cluster1 <- subset %>%
#   group_by(Keyword) %>%
#   filter(pmean <= 10) %>% summarise(Revenue = sum(rsum), Impressions = sum(isum), Clicks = sum(csum)) %>% arrange(desc(Revenue))
# 
# 
