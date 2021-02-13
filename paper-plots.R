library(data.table)
library(ggplot2)
library(scales)
library(RColorBrewer)
library(ggthemes)
library(extrafont)
library(igraph)

# if you haven't registered Times New Roman with R, this is how you can do it:
# font_import(pattern="Times New Roman")
options(scipen=999)

###########################################################################
###########################################################################
###                                                                     ###
###              SECTION 1 DATA LOADING AND INITIALIZATION              ###
###                                                                     ###
###########################################################################
###########################################################################

# Choose the folders from which you want to load the data from
IDEXDir <- "output/fidex-t100-1h-1d-1w-1pmargin/"
EDDir <- "output/fetherdelta-t100-1h-1d-1w-1pmargin/"

#### READ TRADES ####
EtherDeltaTrades <- fread(paste0(EDDir, "trades_labeled.csv"))
EtherDeltaselfTrades <- fread(paste0(EDDir, "self_trades.csv"))
edMapping <- fread(paste0(EDDir, "scc-mapping.csv"))
ed_scc_dt <- fread(paste0(EDDir, "scc.csv"))
ed_scc_dt$DEX <- "EtherDelta"

IDEXTrades <- fread(paste0(IDEXDir, "trades_labeled.csv"))
IDEXselfTrades <- fread(paste0(IDEXDir, "self_trades.csv"))
idexMapping <- fread(paste0(IDEXDir, "scc-mapping.csv"))
idex_scc_dt <- fread(paste0(IDEXDir, "scc.csv"))
idex_scc_dt$DEX <- "IDEX"

output_folder <- "plots/"


# This is the Theme used for all the plots
theme_Publication <- function(base_size=14, base_family="Times New Roman") {
  library(grid)
  library(ggthemes)
  library(extrafont)
  (theme_foundation(base_size=base_size, base_family=base_family)
    + theme(plot.title = element_text(
      size = rel(1.2), hjust = 0.5),
      text = element_text(),
      panel.background = element_rect(colour = "black"),
      plot.background = element_rect(colour = NA),
      panel.border = element_rect(colour = NA),
      axis.title = element_text(size = rel(1)),
      axis.title.y = element_text(angle=90,vjust =2),
      axis.title.x = element_text(vjust = -0.2),
      axis.text = element_text(), 
      axis.line = element_line(colour="black"),
      axis.ticks = element_line(),
      panel.grid.major = element_line(colour="#f0f0f0"),
      panel.grid.minor = element_blank(),
      legend.key = element_rect(colour = NA),
      legend.position = "bottom",
      legend.direction = "horizontal",
      legend.key.size= unit(1, "cm"),
      legend.title = element_text(face="italic"),
      plot.margin=unit(c(10,5,5,5),"mm"),
      strip.background=element_rect(colour="#f0f0f0",fill="#f0f0f0"),
      strip.text = element_text(size = 14)
    ))
}

############################################################################
############################################################################
###                                                                      ###
###             FIGURE 2 PLOT TRADE COUNT VS TRADING PARTNERS            ###
###                                                                      ###
############################################################################
############################################################################

trader_stats <- rbindlist(list(EtherDeltaTrades[, .(user = eth_buyer, partner = eth_seller, amount = trade_amount_eth, DEX="EtherDelta")], # eth_buyer buys eth
                               EtherDeltaTrades[, .(user = eth_seller, partner = eth_buyer, amount = trade_amount_eth, DEX="EtherDelta")], # eth_seller sells eth
                               IDEXTrades[, .(user = eth_buyer, partner = eth_seller, amount = trade_amount_eth, DEX="IDEX")], # eth_buyer buys eth
                               IDEXTrades[, .(user = eth_seller, partner = eth_buyer, amount = trade_amount_eth, DEX="IDEX")])) # eth_seller sells eth


trader_stats <- trader_stats[, .(num_trade_partners = uniqueN(partner), 
                                 avg_amount = mean(amount), total_amount = sum(amount), # avg and total amounts of user being buyer or seller
                                 num_trades = .N), by = list(user, DEX)] # total trades as buyer or seller

trader_stats$DEX_f <- factor(trader_stats$DEX, levels = c("IDEX", "EtherDelta"))
# relation of number of trades to number of trading partners
ggplot(trader_stats, aes(x = num_trade_partners, y = num_trades)) +
  geom_point(size = 1.5, shape=4, alpha=0.25) +
  scale_x_log10(labels=comma) +
  scale_y_log10(labels=comma) +
  facet_grid(. ~ DEX_f) +
  labs(x = "Number of trade partners per trader account", y = "Number of trades per trader account") +
  theme_Publication()
ggsave(filename = paste0(output_folder, "trade_partners_trades.pdf"), width = 6, height = 4, device=cairo_pdf)


###########################################################################
###########################################################################
###                                                                     ###
###                FIGURE 3 PLOT TRADE SIZE DISTRIBUTIONS               ###
###                                                                     ###
###########################################################################
###########################################################################

tradeSizes <- rbind(EtherDeltaTrades[, .(trade_amount_eth, DEX="EtherDelta")],
                    IDEXTrades[, .(trade_amount_eth, DEX="IDEX")])
tradeSizes$DEX_f <- factor(tradeSizes$DEX, levels = c("IDEX", "EtherDelta"))

ggplot(tradeSizes[trade_amount_eth <= 10], aes(trade_amount_eth)) +
  geom_histogram(binwidth = 0.1, center = 0, closed = "left",
                 color = "black", fill = "black", size = 0) +
  scale_x_continuous(breaks = seq(0, 10, 1)) +
  scale_y_log10(labels = comma) +
  facet_grid(. ~ DEX_f) +
  labs(x = "Ether amount, bins of size 0.1", y = "Trade count") +
  theme_Publication()
ggsave(filename = paste0(output_folder, "trade_size_dist_0-10.pdf"),
       width = 6, height = 3, device=cairo_pdf)


###########################################################################
###########################################################################
###                                                                     ###
###                     FIGURE 5 PLOT SCC OCCURRENCE                    ###
###                                                                     ###
###########################################################################
###########################################################################
scc_dt <- rbind(ed_scc_dt, idex_scc_dt)
scc_dt$DEX_f <- factor(scc_dt$DEX, levels = c("IDEX", "EtherDelta"))
threshold <- 100

scc_size_plot <- ggplot(scc_dt) + stat_ecdf(aes(x=occurrence, y=1-..y.., color=DEX_f, linetype=DEX_f)) +
  annotate(geom = "rect", xmin = threshold, xmax = Inf, ymin = 0, ymax = Inf,
           fill = "grey", colour = "black", linetype="dashed", alpha = 0.5) +
  annotate("text", x = threshold*1.1, y = 0.1,
           label = paste0("Suspicously frequent SCCs\n(Occurrence ≥ ",threshold,")"),
           hjust = 0, family="Times New Roman") +
  scale_x_log10(labels = scales::comma) +
  annotation_logticks(sides="bl") +
  scale_y_log10(labels = scales::percent) +
  labs(x="Occurrence", y="Share of SCCs occurring\nmore than x times (CCDF)", color="DEX:", linetype="DEX:") +
  theme_Publication() +
  theme(strip.text.x = element_blank(),
        strip.background = element_rect(colour="white", fill="white"),
        legend.direction = "vertical", legend.key.size= unit(0.5, "cm"),
        legend.position=c(0.15,0.25))
scc_size_plot

ggsave(
  paste0(output_folder,"SCC-threshold.pdf"),
  scc_size_plot,
  width = 6, height = 3, device=cairo_pdf)



############################################################################
############################################################################
###                                                                      ###
###                 FIGURE 6 PLOT WASH TRADING STRUCTURES                ###
###                                                                      ###
############################################################################
############################################################################


result <- list()

IDEXSCChashes <- idex_scc_dt[occurrence > threshold]$scc_hash
IDEXSCCsWithWashTrading <- 0
IDEXSCCTokensWashed <- c()
for (SCChash in IDEXSCChashes) {
  trades <- IDEXTrades[eth_seller_id %in% idexMapping[hash == SCChash]$trader_id &
                         eth_buyer_id %in% idexMapping[hash == SCChash]$trader_id &
                         wash_label == T, list(eth_seller_id, eth_buyer_id, token)]
  if(nrow(trades) > 0) {
    IDEXSCCsWithWashTrading <- IDEXSCCsWithWashTrading + 1
    IDEXSCCTokensWashed <- c(IDEXSCCTokensWashed, length(unique(trades$token)))
  }
  new_g <- simplify(graph_from_data_frame(trades), remove.loops = F)
  
  listLength <- length(result)
  exists <- FALSE
  if(listLength > 0) {
    for (i in 1:listLength) {
      existing_g <- result[[i]][[1]]
      if(isomorphic(existing_g, new_g)) {
        result[[i]] <- sets::tuple(new_g, result[[i]][[2]]+1, result[[i]][[3]])
        exists <- TRUE
        break
      }
    }
  }
  if(!exists & vcount(new_g) > 0) {
    result[[listLength + 1]] <- sets::tuple(new_g, 1, 0)
  }
}

EtherDeltaSCChashes <- ed_scc_dt[occurrence > threshold]$scc_hash
EDSCCsWithWashTrading <- 0
EDSCCTokensWashed <- c()
for (SCChash in EtherDeltaSCChashes) {
  trades <- EtherDeltaTrades[eth_seller_id %in% edMapping[hash == SCChash]$trader_id &
                               eth_buyer_id %in% edMapping[hash == SCChash]$trader_id &
                               wash_label == T, list(eth_seller_id, eth_buyer_id, token)]
  if(nrow(trades) > 0) {
    EDSCCsWithWashTrading <- EDSCCsWithWashTrading + 1
    EDSCCTokensWashed <- c(EDSCCTokensWashed, length(unique(trades$token)))
  }
  new_g <- simplify(graph_from_data_frame(trades), remove.loops = F)
  
  listLength <- length(result)
  exists <- FALSE
  if(listLength > 0) {
    for (i in 1:listLength) {
      existing_g <- result[[i]][[1]]
      if(isomorphic(existing_g, new_g)) {
        result[[i]] <- sets::tuple(new_g, result[[i]][[2]], result[[i]][[3]]+1)
        exists <- TRUE
        break
      }
    }
  }
  if(!exists & vcount(new_g) > 0) {
    result[[listLength + 1]] <- sets::tuple(new_g, 0, 1)
  }
}

countSelfTradersIdex <- length(unique(IDEXselfTrades$eth_seller))
countSelfTradersEtherDelta <- length(unique(EtherDeltaselfTrades$eth_seller))

result[[listLength+1]] <- sets::tuple(graph_from_data_frame(data.frame(from=c(1), to=c(1))),
                                      countSelfTradersIdex, countSelfTradersEtherDelta)
result <- result[order(sapply(result, function(x) vcount(x[[1]])), sapply(result, function(x) ecount(x[[1]])))]

for (i in 1:length(result)) {
  x <- result[[i]]
  graph <- x[[1]]
  cairo_pdf(paste0(output_folder,"graph_",c(letters,"zz")[i],"_plot.pdf"), 100, 100)
  plot(graph, vertex.size = 25, edge.color = "black", vertex.label = NA, frame = T,
       layout=layout.circle, edge.curved = 0.25, edge.width = 50,
       edge.arrow.size=30, margin=c(0,0.15,0.3,0.15)) +
    title(paste0(c(letters,"zz")[i], ") IDEX: ",x[[2]], "\tED: ",x[[3]]), cex.main=56,
          family="Times New Roman", line = -50)
  dev.off()
}
# to create a pdf containing all structures run:
# pdfjam graph_* --nup 9x3 --landscape -o output.pdf; pdfcrop output.pdf


###########################################################################
###########################################################################
###                                                                     ###
###              FIGURE 7 SHARE OF TOKEN VOLUME WASH TRADED             ###
###                                                                     ###
###########################################################################
###########################################################################
wash_share_idex <- rbind(IDEXselfTrades[, list(token, trade_amount_eth, wash_label = T)],
                         IDEXTrades[, list(token, trade_amount_eth, wash_label)])
wash_share_idex <- wash_share_idex[, list(share = sum(.SD[wash_label == T]$trade_amount_eth)/sum(.SD$trade_amount_eth),
                                          DEX="IDEX"), by=token]

wash_share_ed <- rbind(EtherDeltaselfTrades[, list(token, trade_amount_eth, wash_label = T)],
                       EtherDeltaTrades[, list(token, trade_amount_eth, wash_label)])
wash_share_ed <- wash_share_ed[, list(share = sum(.SD[wash_label == T]$trade_amount_eth)/sum(.SD$trade_amount_eth),
                                      DEX="EtherDelta"), by=token]

wash_share <- rbind(wash_share_idex, wash_share_ed)
wash_share$DEX_f <- factor(wash_share$DEX, levels = c("IDEX", "EtherDelta"))
wash_share_plot <- ggplot(wash_share) +
  stat_ecdf(aes(x=share, y=1-..y.., linetype=DEX_f)) +
  scale_y_log10(label = scales::percent) +
  annotation_logticks(sides="l") +
  scale_x_continuous(label = scales::percent, breaks = seq(0,1,0.1)) +
  theme_Publication() +
  theme(strip.text.x = element_blank(),
        strip.background = element_rect(colour="white", fill="white"),
        legend.direction = "vertical", legend.key.size= unit(0.5, "cm"),
        legend.position=c(0.87,0.84)) +
  labs(x="Share of each token trading volume wash traded",
       y="Share of tokens traded on DEX (CCDF)", linetype="DEX:")
wash_share_plot

ggsave(
  paste0(output_folder,"token_wash_share.pdf"),
  wash_share_plot,
  width = 6, height = 4, device=cairo_pdf)



###########################################################################
###########################################################################
###                                                                     ###
###              FIGURE 8 WASH TRADING IN A TOKENS LIFESPAN             ###
###                                                                     ###
###########################################################################
###########################################################################
wash_timeframe_idex <- rbind(IDEXselfTrades[, list(token, timestamp, wash_label = T)],
                             IDEXTrades[, list(token, timestamp, wash_label)])
wash_timeframe_idex <- wash_timeframe_idex[, list(timeframe=(timestamp - min(timestamp))/(max(timestamp)-min(timestamp))),
                                           by=list(token, wash_label)][
                                             wash_label == T, list(time = median(timeframe), DEX="IDEX"), by=token]

wash_timeframe_ed <- rbind(EtherDeltaselfTrades[, list(token, timestamp, wash_label = T)],
                           EtherDeltaTrades[, list(token, timestamp, wash_label)])
wash_timeframe_ed <- wash_timeframe_ed[, list(timeframe=(timestamp - min(timestamp))/(max(timestamp)-min(timestamp))),
                                       by=list(token, wash_label)][
                                         wash_label == T, list(time = median(timeframe), DEX="EtherDelta"), by=token]
wash_timeframe <- rbind(wash_timeframe_idex, wash_timeframe_ed)
wash_timeframe[is.na(wash_timeframe)] <- 0 # where no median was computable, it happened at the beginning
wash_timeframe$DEX_f <- factor(wash_timeframe$DEX, levels = c("IDEX", "EtherDelta"))

wash_timeframe_plot <- ggplot(wash_timeframe) +
  geom_histogram(aes(x=time), color="black", fill="white", breaks=seq(0,1,0.1)) +
  facet_grid(DEX_f ~ ., scales = "free_y") +
  scale_x_continuous(label = scales::percent, breaks = seq(0,1,0.1)) +
  theme_Publication() +
  labs(x="Timeframe within a token's trading lifespan", y="Tokens with wash trading activity")
wash_timeframe_plot


ggsave(
  paste0(output_folder,"token_wash_timeframes.pdf"),
  wash_timeframe_plot,
  width = 6, height = 4, device=cairo_pdf)


###########################################################################
###########################################################################
###                                                                     ###
###                 FIGURE 9 MONTHLY WASH TRADING VOLUME                ###
###                                                                     ###
###########################################################################
###########################################################################
wash_share_idex <- rbind(IDEXselfTrades[, list(token, trade_amount_dollar, date, wash_label = T)],
                         IDEXTrades[, list(token, trade_amount_dollar, date, wash_label)])
wash_share_idex$DEX <- "IDEX"

wash_share_ed <- rbind(EtherDeltaselfTrades[, list(token, trade_amount_dollar, date, wash_label = T)],
                       EtherDeltaTrades[, list(token, trade_amount_dollar, date, wash_label)])
wash_share_ed$DEX <- "EtherDelta"
wash_share <- rbind(wash_share_idex, wash_share_ed)
monthly_wash_volume <- wash_share[wash_label == T, list(monthly_wash_volume = sum(trade_amount_dollar)),
                                  by=list(month = as.Date(cut(as.Date(date), "1 month")), DEX)]
monthly_wash_volume$DEX_f <- factor(monthly_wash_volume$DEX, levels = c("IDEX", "EtherDelta"))
ggplot(monthly_wash_volume) +
  geom_bar(aes(x=month, y=monthly_wash_volume), stat="identity", fill="black", color="white") +
  scale_x_date(labels = date_format("%Y-%m")) +
  scale_y_continuous(labels = scales::comma) +
  facet_grid(DEX_f ~ ., scales = "free_y") + 
  labs(x="Month", y="Wash trade volume in U.S. Dollars") + 
  theme_Publication()
ggsave(filename = paste0(output_folder, "monthly_wash_vol.pdf"), width = 6, height = 4, device=cairo_pdf)



###########################################################################
###########################################################################
###                                                                     ###
###            FIGURE 10 WASH TRADING VOLUME SHARE OVER TIME            ###
###                                                                     ###
###########################################################################
###########################################################################
wash_trades_per_week <- wash_share[, .(wash_vol = sum(.SD[wash_label == TRUE]$trade_amount_dollar),
                                       vol = sum(.SD$trade_amount_dollar),
                                       wash_trades = nrow(.SD[wash_label == TRUE]),
                                       trades = nrow(.SD)),
                                   by = list(week=cut(as.POSIXct(date, tz = "UTC"), "1 week"), DEX)]

wash_trades_per_week$date <- as.POSIXct(wash_trades_per_week$week)
wash_trades_per_week$wash_vol_percentage <- wash_trades_per_week$wash_vol / wash_trades_per_week$vol
wash_trades_per_week$wash_percentage <- wash_trades_per_week$wash_trades / wash_trades_per_week$trades
wash_trades_per_week$DEX_f <- factor(wash_trades_per_week$DEX, levels = c("IDEX", "EtherDelta"))

ggplot(wash_trades_per_week, aes(x = date, y = wash_vol_percentage)) +
  geom_line() +
  labs(x = "Date", y = "Weekly wash volume share") +
  scale_x_datetime() +#labels = date_format("%Y-%m-%d")) +
  scale_y_continuous(labels = scales::percent) +
  theme_Publication() + 
  facet_grid(DEX_f ~ ., scales = "free_y") +
  theme(axis.text.x = element_text(angle = 90, vjust=0.5, hjust=1))
ggsave(filename = paste0(output_folder, "weekly_wash_vol_share_time_series.pdf"),
       width = 6, height = 4, device=cairo_pdf)


###########################################################################
###########################################################################
###                                                                     ###
###                     TABLE 2 WASH TRADES SUMMARY                     ###
###                                                                     ###
###########################################################################
###########################################################################
printStats <- function(name, IDEXcount, EDcount) {
  print(paste(name, "IDEX:", IDEXcount, "EtherDelta:", EDcount))
}

IDEXselfTradeCount <- nrow(IDEXselfTrades)
edSelfTradeCount <- nrow(EtherDeltaselfTrades)
IDEXTradeCount <- nrow(IDEXTrades) + IDEXselfTradeCount
edTradeCount <- nrow(IDEXTrades) + edSelfTradeCount
printStats("# Self-Trades", IDEXselfTradeCount, edSelfTradeCount)
printStats("# Wash Trades", nrow(IDEXTrades[wash_label==T]) + IDEXselfTradeCount,
                            nrow(EtherDeltaTrades[wash_label==T]) + edSelfTradeCount)
printStats("Self-Trades Share (Of All Trades)",
           IDEXselfTradeCount / IDEXTradeCount,
           edSelfTradeCount / edTradeCount)
printStats("Wash Trades Share (Of All Trades)",
           (nrow(IDEXTrades[wash_label==T]) + IDEXselfTradeCount) / IDEXTradeCount,
           (nrow(EtherDeltaTrades[wash_label==T]) + edSelfTradeCount) / edTradeCount)
printStats("Total Self-Traded Volume ETH",
           sum(IDEXselfTrades$trade_amount_eth),
           sum(EtherDeltaselfTrades$trade_amount_eth))
printStats("Total Wash Volume ETH",
           sum(IDEXselfTrades$trade_amount_eth) + sum(IDEXTrades[wash_label==T]$trade_amount_eth),
           sum(EtherDeltaselfTrades$trade_amount_eth) + sum(EtherDeltaTrades[wash_label==T]$trade_amount_eth))
printStats("Total Self-Traded Volume USD",
           sum(IDEXselfTrades$trade_amount_dollar),
           sum(EtherDeltaselfTrades$trade_amount_dollar))
printStats("Total Wash Volume USD",
           sum(IDEXselfTrades$trade_amount_dollar) + sum(IDEXTrades[wash_label==T]$trade_amount_dollar),
           sum(EtherDeltaselfTrades$trade_amount_dollar) + sum(EtherDeltaTrades[wash_label==T]$trade_amount_dollar))
printStats("Wash Trade Fees Received USD",
           (sum(IDEXselfTrades$trade_amount_dollar) + sum(IDEXTrades[wash_label==T]$trade_amount_dollar)) * 0.003,
           (sum(EtherDeltaselfTrades$trade_amount_dollar) + sum(EtherDeltaTrades[wash_label==T]$trade_amount_dollar)) * 0.003)
printStats("# Self-Traded Tokens",
           length(unique(IDEXselfTrades$token)),
           length(unique(EtherDeltaselfTrades$token)))
printStats("# Wash Tokens",
           length(unique(c(IDEXselfTrades$token, IDEXTrades[wash_label==T]$token))),
           length(unique(c(EtherDeltaselfTrades$token, EtherDeltaTrades[wash_label==T]$token))))
printStats("Wash Token Share",
           length(unique(c(IDEXselfTrades$token, IDEXTrades[wash_label==T]$token))) / length(unique(c(IDEXselfTrades$token, IDEXTrades$token))),
           length(unique(c(EtherDeltaselfTrades$token, EtherDeltaTrades[wash_label==T]$token))) / length(unique(c(EtherDeltaselfTrades$token, EtherDeltaTrades$token))))

IDEXselfTraders <- unique(c(IDEXselfTrades$eth_buyer, IDEXselfTrades$eth_seller))
EDselfTraders <- unique(c(EtherDeltaselfTrades$eth_buyer, EtherDeltaselfTrades$eth_seller))
IDEXwashTraders <- unique(c(IDEXTrades[wash_label==T]$eth_buyer, IDEXTrades[wash_label==T]$eth_seller))
EDwashTraders <- unique(c(EtherDeltaTrades[wash_label==T]$eth_buyer, EtherDeltaTrades[wash_label==T]$eth_seller))

printStats("# Self Trader Accounts", length(IDEXselfTraders), length(EDselfTraders))
printStats("# Wash Trader Accounts",
           length(unique(c(IDEXselfTraders, IDEXwashTraders))),
           length(unique(c(EDselfTraders, EDwashTraders))))

printStats("# Analyzed SCC", nrow(idex_scc_dt[occurrence >= 100]), nrow(ed_scc_dt[occurrence >= 100]))
printStats("# SCC with Wash Trading", IDEXSCCsWithWashTrading, EDSCCsWithWashTrading)
printStats("Mean # Tokens Washed per SCC", mean(IDEXSCCTokensWashed), mean(EDSCCTokensWashed))
