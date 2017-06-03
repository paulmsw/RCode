Out_three[Out_three$Domain == "supercasino.com",]
Out_three[Out_three$Domain == "jackpot247.com",]

sc <- dt[dt$Domain == "supercasino.com",] %>% dplyr::select(1,2,3,4,5,6,9,14)
jp <- dt[dt$Domain == "jackpot247.com",] %>% dplyr::select(1,2,3,4,5,6,9,14)
