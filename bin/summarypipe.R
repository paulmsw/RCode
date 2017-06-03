#!/usr/bin/env Rscript
library(optparse)
library(data.table)
library(dplyr)

option_list = list(
  #make_option(c("-c", "--cores"), type="numeric", default=30,
  #              help="set number of cores for parallelism", metavar="numeric"),
  make_option(c("-i", "--id"), type="numeric", default=NULL,
              help="set the client ID eg: -i 63,  (will return results for MoneyGuru)", metavar="numeric"),
  make_option(c("-o", "--out"), type="numeric", default=54,
              help="set log output eg: -o 0, (will turn off logging) \n  [default= %default / ie On]", metavar="numeric"),
  make_option(c("-l", "--lag"), type="numeric", default=4,
              help="set lag in days - [default= %default, ie today]", metavar="numeric")
);

opt_parser = OptionParser(option_list=option_list);
opt = parse_args(opt_parser);



if (is.null(opt$id)){
  print_help(opt_parser)
  stop("At least one argument must be supplied: clientID, using the -i switch \n\nExample: summarypipe -i 63 -o 0 (to process MoneyGuru.com's data for the past 90 days)\n\n\n", call.=FALSE)
}
# 
# # #http://10.0.101.8:8787/
# 
#      opt <- NULL
#      opt$start <- 1
#       opt$cores <- 20
#      opt$id <- 1
#      opt$lag <- 4
#        opt$out <- 1
#       opt$days <- 90


clientID <- opt$id
print("latest version")

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
kwp$Keyword <- as.factor(tolower(str_replace_all(kwp$Keyword, "[[:punct:]]", "")))
kwp$Bid[is.na(kwp$Bid)] <- 0
kwp$Competition[is.na(kwp$Competition)] <- 0



#####################################



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
 #  sink(paste(fullpath,"logs/summarylog.txt", sep = ""), append = TRUE)
 }
 


 
 HCL2 <- read.csv("~/Data/RandD/UK/HCL2.csv",stringsAsFactors = TRUE)
 HCL2$Keyword <- as.factor(tolower(str_replace_all(HCL2$Keyword, "[[:punct:]]", "")))
 HCL2 <- unique(HCL2) 
 HCL2[is.na(HCL2)] <- 0
 
 
 
foreach(i = 1:numfiles, .packages = c("dplyr", "RollingWindow")) %do% {

  
    file_i <- readRDS(paste(fullpath, filelist[i], sep = "/"))
    
    f_i <- unique(file_i %>% dplyr::select(-Category))
    
    

    
    dfm <- inner_join(f_i, kwp, by = "Keyword" )
    dfm$Keyword <- as.factor(dfm$Keyword)



  dfm$Date <- as.Date(dfm$Date)

  minDate <- min(dfm$Date)
  maxDate <- max(dfm$Date)

  dataPeriod <- maxDate - minDate

  dfm[is.na(dfm)] <- 0
  dfm$Month <- as.numeric(format(dfm$Date, "%m"))
  dfm$Year <- as.numeric(format(dfm$Date, "%Y"))

  dfm$Revenue <- ((dfm$Impressions / 30) * dfm$CTR)* dfm$Bid
  dfm$Potential<- ((dfm$Impressions / 30) * 0.325)* dfm$Bid
  dfm$PotC<- ((dfm$Impressions / 30) * 0.325)
  dfm$Clicks <- ((dfm$Impressions / 30) * dfm$CTR)
  
  ###### Sort Artificial Catagorical Dates
  last1start = max(file_i$Date) - 1
  last7start = max(file_i$Date) - 7
  first30end = min(file_i$Date) + 30
  second30end = min(file_i$Date) + 60
  endDate = max(file_i$Date) 
  startDate = min(file_i$Date) 
  dfm$DayCount <- as.integer(dfm$Date - startDate) + 1
  
  GamingTypes <- read.csv("~/Data/RandD/UK/GamingTypes.csv", stringsAsFactors = TRUE)
  gt <- GamingTypes[,c(1,2)]
  rm(GamingTypes)
  
  dfm$Keyword <- as.factor(str_replace_all(dfm$Keyword, "[[:punct:]]", ""))

  
  
  dt <- dplyr::inner_join(dfm, HCL2, by = "Keyword")
  dt$Keyword <- as.factor(dt$Keyword)

  dt  <- unique(dt)
  
  
  dfm <- dt
  

  # Sort Aggregations
  DF_Group <- dfm %>% dplyr::group_by(DayCount, Product, Country, Date, Domain, Keyword, Url, Month, Year) %>%
    dplyr::arrange(DayCount) %>%
    summarise(pmean = mean(Position),
              rmean = mean(Revenue),
              cmean = mean(Clicks),
              bmean = mean(Bid),
              potmean = mean(Potential),
              potmeanclicks = mean(PotC),
              imean = mean(Impressions))
  print("finished initial group_by")

  #30 day rolling mean

  

  DF <- unique(ungroup(DF_Group))

  # Compute Moving Averages all wrong to date
  by_site <- dplyr::group_by(DF, Keyword, Domain, Url, Month, Product)
  arrdf <- dplyr::arrange(by_site, DayCount)
  

  arrdf$pmeanSubbed = arrdf$pmean
  arrdf$rmeanSubbed = arrdf$rmean
  arrdf$cmeanSubbed = arrdf$cmean
  arrdf$bmeanSubbed = arrdf$bmean
  arrdf$imeanSubbed = arrdf$imean
  arrdf$potmeanSubbed = arrdf$potmean
  arrdf$potmeanclicksSubbed = arrdf$potmeanclicks

  dtroll <- as.data.frame(ungroup(arrdf))


  dtroll2 <- dtroll
  dtroll2[is.na(dtroll2)] <- 0


  kw_no_space <-str_replace_all(as.character(dtroll2[1,6]),"[^a-zA-Z0-9]", "_")

  
  dtroll2$monSlice <- NULL


  dtroll2[dtroll2$Date >= startDate & dtroll2$Date < first30end, "monSlice"] <- 1
  dtroll2[dtroll2$Date >= first30end & dtroll2$Date < second30end, "monSlice"] <- 2
  dtroll2[dtroll2$Date >= second30end & dtroll2$Date <= endDate, "monSlice"] <- 3
  
  dtroll2$monSlice <- as.factor(dtroll2$monSlice)

  dtroll2$last7 <- NULL
  dtroll2[dtroll2$Date >= last7start & dtroll2$Date < endDate, "last7"] <- 1
  dtroll2[!(dtroll2$Date >= last7start & dtroll2$Date < endDate), "last7"] <- 0
  dtroll2$last7 <- as.factor(dtroll2$last7)
  
  
  dtroll2$last1 <- NULL
  dtroll2[dtroll2$Date >= last1start & dtroll2$Date < endDate, "last1"] <- 1
  dtroll2[!(dtroll2$Date >= last1start & dtroll2$Date < endDate), "last1"] <- 0
  dtroll2$last1 <- as.factor(dtroll2$last1)
  
    
  dtroll2 <-  ungroup(dtroll2)
  #############################

  
  
    smon <- dtroll2 %>% dplyr::group_by(Month, Domain, Product ) %>% dplyr::arrange(Month, Keyword) %>%
    summarise(rsum = sum(rmeanSubbed),
              pmean = mean(pmeanSubbed),
              csum = sum(cmeanSubbed),
              bmean = mean(bmeanSubbed),
              NumberKeywords = n(),
              isum = sum(imeanSubbed),
              potmean = mean(potmeanSubbed),
              potmeanclicks = mean(potmeanclicksSubbed))



  print("Getting domain parts")
  sdayfn <- as.character(paste(fullpath, "smon_", kw_no_space,opt$id,".RDS", sep = ""))


  library(tldextract)
  urllist <- as.character(smon$Domain)
  # get most recent TLD listings
  tld <- getTLD() # optionally pass in a different URL than the default
  manyhosts <- c(urllist)
  newurls <- tldextract(urllist, tldnames=tld)
  smon$SLD <- as.factor(newurls$domain)
  smon$Host<- smon$Domain
  smon$Domain <- as.factor(paste(newurls$domain, newurls$tld, sep="."))


  print("Saving smon ")
  print("for client ID:")
  print(opt$id)
  print("in location:")
  print(sdayfn)

  saveRDS(smon, sdayfn)
  #############################

  
  #############################
  smonk <- dtroll2 %>% dplyr::group_by(monSlice,Keyword, Domain, Product ) %>% dplyr::arrange(monSlice, Keyword) %>%
    summarise(rsum = sum(rmeanSubbed),
              pmean = mean(pmeanSubbed),
              csum = sum(cmeanSubbed),
              bmean = mean(bmeanSubbed),
              NumberKeywords = n(),
              isum = sum(imeanSubbed),
              potmean = mean(potmeanSubbed),
              potmeanclicks = mean(potmeanclicksSubbed))
  
  
  
  print("Getting domain parts")
  sdayfn <- as.character(paste(fullpath, "smonk_", kw_no_space,opt$id,".RDS", sep = ""))
  
  
  library(tldextract)
  urllist <- as.character(smonk$Domain)
  # get most recent TLD listings
  tld <- getTLD() # optionally pass in a different URL than the default
  manyhosts <- c(urllist)
  newurls <- tldextract(urllist, tldnames=tld)
  smonk$Host<- smonk$Domain
  smonk$SLD <- as.factor(newurls$domain)
  smonk$Domain <- as.factor(paste(newurls$domain, newurls$tld, sep="."))
  
  
  print("Saving smonk ")
  print("for client ID:")
  print(opt$id)
  print("in location:")
  print(sdayfn)
  
  saveRDS(smonk, sdayfn)
  #############################
  
  
  
  
  
  
  #############################
  dtroll2 <-  ungroup(dtroll2)
  sdaykw <- dtroll2 %>% dplyr::group_by(DayCount, monSlice ,Product ,Keyword, Domain ) %>%
    dplyr::arrange(DayCount, Month, Keyword) %>%
    summarise(rsum = sum(rmean),
              pmean = mean(pmean),
              csum = sum(cmean),
              bmean = mean(bmean),
              NumberKeywords = n(),
              isum = sum(imean),
              potmean = mean(potmean),
              potmeanclicks = mean(potmeanclicksSubbed))

  #Recreate Date Variable as.Date
  sdaykw$Date <- as.Date(minDate + sdaykw$DayCount - 1)


  urllist <- as.character(sdaykw$Domain)
  # get most recent TLD listings
  tld <- getTLD() # optionally pass in a different URL than the default
  manyhosts <- c(urllist)
  newurls <- tldextract(urllist, tldnames=tld)
  sdaykw$Host<- sdaykw$Domain
  sdaykw$SLD <- as.factor(newurls$domain)
  sdaykw$Domain <- as.factor(paste(newurls$domain, newurls$tld, sep="."))


  sdaykwfn <- as.character(paste(fullpath, "sdaykw_", kw_no_space,opt$id,".RDS", sep = ""))
  print("Saving sdaykw")
  print(opt$id)
  saveRDS(sdaykw, sdaykwfn)
  
  dtroll2 <-  ungroup(dtroll2)
  #############################


  sday2dom <- dtroll2 %>% dplyr::group_by(DayCount, Month,Product ,Keyword, Domain ) %>%
    dplyr::arrange(DayCount, Month, Keyword) %>%
    dplyr::summarise(rsum = sum(rmean),
              pmean = mean(pmean),
              csum = sum(cmean),
              bmean = mean(bmean),
              NumberKeywords = n(),
              isum = sum(imean),
              potmean = mean(potmean),
              potmeanclicks = mean(potmeanclicksSubbed))

  sday2dom$Date <- as.Date(minDate + sday2dom$DayCount - 1)
  
  urllist <- as.character(sday2dom$Domain)
  # get most recent TLD listings
  tld <- getTLD() # optionally pass in a different URL than the default
  manyhosts <- c(urllist)
  newurls <- tldextract(urllist, tldnames=tld)
  sday2dom$Host<- sday2dom$Domain
  sday2dom$SLD <- as.factor(newurls$domain)
  sday2dom$Domain <- as.factor(paste(newurls$domain, newurls$tld, sep="."))
  

  sday2domfn <- as.character(paste(fullpath, "sday2dom_", kw_no_space,opt$id,".RDS", sep = ""))
  print("Saving sday2dom")
  print(opt$id)
  print(sday2domfn)
  saveRDS(sday2dom, sday2domfn)

  print("Saving sday2dom ")
  print("for client ID:")
  print(opt$id)
  print("in location:")
  print(sday2domfn)
  
  

  
  
  dtroll2 <-  ungroup(dtroll2)
  #############################
  
  ###### Last 7 days detailed
  sl7kw <- dtroll2 %>% dplyr::filter(last7=="1") %>% dplyr::group_by(DayCount ,Product ,Keyword, Domain ) %>%
    dplyr::arrange(DayCount, monSlice, Keyword) %>%
    dplyr::summarise(rsum = sum(rmean),
              pmean = mean(pmean),
              csum = sum(cmean),
              bmean = mean(bmean),
              NumberKeywords = n(),
              isum = sum(imean),
              potmean = mean(potmean),
              potmeanclicks = mean(potmeanclicksSubbed))
  
  sl7kw$Date <- as.Date(minDate + sl7kw$DayCount - 1)
  
  
  
  sl7kwfn <- as.character(paste(fullpath, "sl7kw_", kw_no_space,opt$id,".RDS", sep = ""))
  
  
  print("Saving sl7kw")
  print(opt$id)
  print(sl7kwfn)
  saveRDS(sl7kw, sl7kwfn)
  
  print("Saving sl7kw ")
  print("for client ID:")
  print(opt$id)
  print("in location:")
  print(sl7kwfn)
  
  
  
  
  
  dtroll2 <-  ungroup(dtroll2)
  #############################
  
  ###### Last 7 days summary
  sl7sum <- dtroll2 %>% dplyr::filter(last7=="1") %>% dplyr::group_by(DayCount ,Product, Domain ) %>%
    dplyr::arrange(monSlice, Keyword) %>%
    dplyr::summarise(rsum = sum(rmean),
              pmean = mean(pmean),
              csum = sum(cmean),
              bmean = mean(bmean),
              NumberKeywords = n(),
              isum = sum(imean),
              potmean = mean(potmean),
              potmeanclicks = mean(potmeanclicksSubbed))
  
  sl7sum$Date <- as.Date(minDate + sl7sum$DayCount - 1)
  
  sl7sumfn <- as.character(paste(fullpath, "sl7sum_", kw_no_space,opt$id,".RDS", sep = ""))
  print("Saving sl7sum")
  print(opt$id)
  print(sl7sumfn)
  saveRDS(sl7sum, sl7kwfn)
    print("Saving sl7sum ")
  print("for client ID:")
  print(opt$id)
  print("in location:")
  print(sl7sum)
  
  
  
  dtroll2 <-  ungroup(dtroll2)

  
    ###### yesterday summary
  sl1sum <- dtroll2 %>% dplyr::filter(last1=="1") %>% dplyr::group_by(DayCount, Product, Domain ) %>%
    dplyr::arrange(monSlice, Keyword) %>%
    summarise(rsum = sum(rmean),
              pmean = mean(pmean),
              csum = sum(cmean),
              bmean = mean(bmean),
              NumberKeywords = n(),
              isum = sum(imean),
              potmean = mean(potmean),
              potmeanclicks = mean(potmeanclicksSubbed))
  
  sl1sum$Date <- as.Date(minDate + sl1sum$DayCount - 1)
  
  sl1sumfn <- as.character(paste(fullpath, "sl1sum_", kw_no_space,opt$id,".RDS", sep = ""))
  print("Saving sl1sum")
  print(opt$id)
  print(sl1sumfn)
  saveRDS(sl1sum, sl1sumfn)
  print("Saving sl7sum ")
  print("for client ID:")
  print(opt$id)
  print("in location:")
  
  
  dtroll2 <-  ungroup(dtroll2)
  
  
  ###### yesterday summary
  sl1kw <- dtroll2 %>% dplyr::filter(last1=="1") %>% dplyr::group_by(DayCount, Product ,Keyword, Domain ) %>%
    dplyr::arrange(DayCount, Keyword) %>%
    summarise(rsum = sum(rmean),
              pmean = mean(pmean),
              csum = sum(cmean),
              bmean = mean(bmean),
              NumberKeywords = n(),
              isum = sum(imean),
              potmean = mean(potmean),
              potmeanclicks = mean(potmeanclicksSubbed))
  
  sl1kw$Date <- as.Date(minDate + sl1kw$DayCount - 1)
  
  sl1skwfn <- as.character(paste(fullpath, "sl1sum_", kw_no_space,opt$id,".RDS", sep = ""))
  print("Saving sl1sum")
  print(opt$id)
  print(sl1sumfn)
  saveRDS(sl1sum, sl1sumfn)
  print("Saving sl7sum ")
  print("for client ID:")
  print(opt$id)
  print("in location:")
  
  
}



sink()

#
#
# #
# #
# #
#  sdaydomains <- rbind(readRDS("/home/paul/Data/RandD/AppData/2017-04-28-63/sday2dom_loans63.RDS"), readRDS("/home/paul/Data/RandD/AppData/2017-04-28-63/sday2dom_personal_loans63.RDS"))
#  sdaykw <- rbind(readRDS("/home/paul/Data/RandD/AppData/2017-04-28-63/sdaykw_loans63.RDS"), readRDS("/home/paul/Data/RandD/AppData/2017-04-28-63/sdaykw_personal_loans63.RDS"))
#  smon <- rbind(readRDS("/home/paul/Data/RandD/AppData/2017-04-28-63/smon_loans63.RDS"), readRDS("/home/paul/Data/RandD/AppData/2017-04-28-63/smon_personal_loans63.RDS"))
# #
# # #
#  sdaydomains$Keyword <- as.factor(sdaydomains$Keyword)
#  sdaydomains$Domain <- as.factor(sdaydomains$Domain)
#  sdaydomains$Category <- as.factor(sdaydomains$Category)
# #
# #
#  sdaykw$Keyword <- as.factor(sdaykw$Keyword)
#  sdaykw$Domain <- as.factor(sdaykw$Domain)
#  sdaykw$Category <- as.factor(sdaykw$Category)
# #
#   smon$Keyword <- as.factor(smon$Keyword)
#   smon$Domain <- as.factor(smon$Domain)
#   smon$Category <- as.factor(smon$Category)
#
#
#
#






  # #
#
#   KWCluster_1<-
#
#     sdaykw %>%
#    group_by(Keyword) %>%
#    filter(pmean == 1, Month==4) %>% summarise(Revenue = sum(rsum), Impressions = sum(isum), Clicks = sum(csum)) %>% arrange(desc(Revenue))
#
#  top20_Rev <- smon %>%
#     group_by(Keyword, Domain, Category) %>%
#     filter(Category != "paydayloan", Month==2) %>%
#     summarise(Revenue = sum(rsum), Impressions = sum(isum), Clicks = sum(csum)) %>%
#     arrange(desc(Revenue)) %>%
#     top_n(10, Revenue)
#
#  uniquekw <- unique(top20_Rev$Keyword)
#
#  top20_Rev_Month <- smon %>%
#    group_by(Keyword, Domain, Category, Month) %>%
#    filter(Category != "paydayloan") %>%
#    summarise(Revenue = sum(rsum), Impressions = sum(isum), Clicks = sum(csum)) %>%
#    arrange(desc(Revenue)) %>%
#    top_n(10, Revenue)
#
#   levels(smon$Category)
# #
#   ggplot top20_Rev
#
#
# #saveRDS(kwptemp, "/home/paul/Data/RandD/AppData/kwpdata/1_kwp.RDS")
#
#
# #
# #
# # uniqueKWs <- as.factor(names(AtribLst(cluscards[cluscards==1], attrC="names", isNullC=NA)))
# #
# # subset <- unique(sdaykw[sdaykw$Keyword %in% uniqueKWs, ])
# #
# # cluster1 <- subset %>%
# #   group_by(Keyword) %>%
# #   filter(pmean <= 10) %>% summarise(Revenue = sum(rsum), Impressions = sum(isum), Clicks = sum(csum)) %>% arrange(desc(Revenue))
# #
# #
