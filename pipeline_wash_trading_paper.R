#!/usr/bin/env Rscript

##### PIPELINE #####

# IMPORTS
library("optparse")
library(data.table)
library(igraph)
library(rjson)
library(Rcpp)

library(hash)

# SOURCE NECESSARY FILES
sourceCpp(file = "volume_matching.cpp")

# GLOBAL CONSTANTS AND VARIABLES
global_ether_id <- "0x0000000000000000000000000000000000000000"
global_max_hash_env_size <- 49000
global_hash_env_names <- list()
global_inv_hash_env_names <- list()
global_num_hash_envs <- 0
global_num_scc <- 0
global_trader_hashes <- data.table(trader_address = character(), trader_id = character())

global_scc_traders_map <- hash()

# GLOBAL FUNCTIONS
assign_hash <- Vectorize(assign, vectorize.args = c("x", "value"))
exists_hash <- Vectorize(exists, vectorize.args = "x")
get_hash <- Vectorize(get, vectorize.args = "x")




#### PREPARE TRADES ####

load_trades <- function(file_csv = "data/IDEXTradesNew_realAmounts.csv") {
  trades <- fread(file = file_csv)
  print(paste0("Info: read file ", file_csv, " as data.table with ", nrow(trades), " rows."))
  print(paste0("Columns are: ", paste(colnames(trades), collapse = ", ")))
  return(trades)
}

get_successful_and_complete_trades <- function(trades, status_column, status_success) {
  n <- nrow(trades)
  if(!missing(status_column) & !missing(status_success)) {
    trades <- trades[eval(status_column) == status_success]
  }
  trades <- trades[complete.cases(trades)]
  print(paste0("Info: dropped ", (n - nrow(trades)), " rows, which had missing/unsuccessful status, ",
             "or any missing values. ", nrow(trades), " rows remaining."))
  return(trades)
}

# get trades where Ether and some token were traded
# (drops trades between two tokens and trades where tokenBuy and tokenSell are the same)
get_ether_token_trades <- function(trades, token_column1, token_column2) {
  n <- nrow(trades)
  trades <- trades[eval(token_column1) == global_ether_id | eval(token_column2) == global_ether_id]
  trades <- trades[eval(token_column1) != eval(token_column2)]
  print(paste0("Info: dropped ", (n - nrow(trades)), " rows, which are trades between two tokens or ",
             "trades between the same currency. ", nrow(trades), " rows remaining."))
  return(trades)
}

# assumption: file must have three columns in order: date, timestamp, USD value; date must be of format "%m/%d/%Y"
# assumption: trades must have columns: timestamp, tokenBuy, tokenSell, amountBoughtReal, amountSoldReal
# (order amounts are dropped)
# assumption: no token-token-trade (they are dropped otherwise)
merge_trades_with_daily_usd_price <- function(trades, price_file_csv = "data/EtherDollarPrice.csv") {
  
  # read file
  ether_dollar <- fread(file = price_file_csv)
  colnames(ether_dollar) <- c("date", "timestamp", "dollar")
  ether_dollar$date <- as.Date(ether_dollar$date, format = "%m/%d/%Y")
  
  ## add timestamp of date to trades for merging
  # get greatest Dollar timestamp that is smaller-equal than the smallest trades timestamp
  min_trades_timestamp <- min(trades$timestamp)
  min_dollar_timestamp <- ether_dollar[timestamp <= min_trades_timestamp][order(timestamp)]
  min_dollar_timestamp <- min_dollar_timestamp[nrow(min_dollar_timestamp)]$timestamp
  # get smallest Dollar timestamp that is greater-equal than the greatest trades timestamp
  max_trades_timestamp <- max(trades$timestamp)
  max_dollar_timestamp <- ether_dollar[timestamp >= max_trades_timestamp]
  max_dollar_timestamp <- max_dollar_timestamp[1]$timestamp
  # get left sides of intervals
  intervals_left <- ether_dollar[timestamp >= min_dollar_timestamp & timestamp <= max_dollar_timestamp]$timestamp
  # cut IDEX timestamps based on intervals
  trades$cut <- cut(trades$timestamp, breaks = intervals_left, labels = intervals_left[1:(length(intervals_left)-1)],
                    include.lowest = T, right = F, dig.lab = 15)
  trades$cut <- as.numeric(levels(trades$cut))[trades$cut]
  
  # merge buy eth trades with eth-dollar price
  trades_buyeth <- trades[tokenBuy == global_ether_id]
  trades_buyeth <- merge(trades_buyeth, ether_dollar[, .(timestamp, eth_price = dollar, date)], 
                         by.x = "cut", by.y = "timestamp")
  trades_buyeth <- trades_buyeth[, .(date, cut, blockNumber, timestamp, transactionHash,
                                     eth_buyer = maker, eth_seller = taker, ether = tokenBuy, token = tokenSell, 
                                     trade_amount_eth = amountBoughtReal, trade_amount_dollar = amountBoughtReal * eth_price,
                                     trade_amount_token = amountSoldReal, token_price_in_eth = 1/price,
                                     fee_eth_buyer = feeMake, fee_eth_seller = feeTake)]
  # merge sell eth trades with eth-dollar price
  trades_selleth <- trades[tokenSell == global_ether_id]
  trades_selleth <- merge(trades_selleth, ether_dollar[, .(timestamp, eth_price = dollar, date)], 
                          by.x = "cut", by.y = "timestamp")
  trades_selleth <- trades_selleth[, .(date, cut, blockNumber, timestamp, transactionHash,
                                       eth_buyer = taker, eth_seller = maker, ether = tokenSell, token = tokenBuy,
                                       trade_amount_eth = amountSoldReal, trade_amount_dollar = amountSoldReal * eth_price,
                                       trade_amount_token = amountBoughtReal, token_price_in_eth = price,
                                       fee_eth_buyer = feeTake, fee_eth_seller = feeMake)]
  # bind
  trades_eth <- rbindlist(list(trades_buyeth, trades_selleth))[order(blockNumber)]
  
  return(trades_eth)
}

merge_EtherDelta_trades_with_daily_usd_price <- function(trades, price_file_csv = "data/EtherDollarPrice.csv") {
  
  # read file
  ether_dollar <- fread(file = price_file_csv)
  colnames(ether_dollar) <- c("date", "timestamp", "dollar")
  ether_dollar$date <- as.Date(ether_dollar$date, format = "%m/%d/%Y")
  
  ## add timestamp of date to trades for merging
  # get greatest Dollar timestamp that is smaller-equal than the smallest trades timestamp
  min_trades_timestamp <- min(trades$timestamp)
  min_dollar_timestamp <- ether_dollar[timestamp <= min_trades_timestamp][order(timestamp)]
  min_dollar_timestamp <- min_dollar_timestamp[nrow(min_dollar_timestamp)]$timestamp
  # get smallest Dollar timestamp that is greater-equal than the greatest trades timestamp
  max_trades_timestamp <- max(trades$timestamp)
  max_dollar_timestamp <- ether_dollar[timestamp >= max_trades_timestamp]
  max_dollar_timestamp <- max_dollar_timestamp[1]$timestamp
  # get left sides of intervals
  intervals_left <- ether_dollar[timestamp >= min_dollar_timestamp & timestamp <= max_dollar_timestamp]$timestamp
  # cut IDEX timestamps based on intervals
  trades$cut <- cut(trades$timestamp, breaks = intervals_left, labels = intervals_left[1:(length(intervals_left)-1)],
                    include.lowest = T, right = F, dig.lab = 15)
  trades$cut <- as.numeric(levels(trades$cut))[trades$cut]
  
  # merge buy eth trades with eth-dollar price
  trades_buyeth <- trades[tokenBuy == global_ether_id]
  trades_buyeth <- merge(trades_buyeth, ether_dollar[, .(timestamp, eth_price = dollar, date)], 
                         by.x = "cut", by.y = "timestamp")
  trades_buyeth <- trades_buyeth[, .(date, cut, blockNumber, timestamp, transactionHash,
                                     eth_buyer = maker, eth_seller = taker, ether = tokenBuy, token = tokenSell, 
                                     trade_amount_eth = amountBoughtReal, trade_amount_dollar = amountBoughtReal * eth_price,
                                     trade_amount_token = amountSoldReal, token_price_in_eth = 1/price)]
  # merge sell eth trades with eth-dollar price
  trades_selleth <- trades[tokenSell == global_ether_id]
  trades_selleth <- merge(trades_selleth, ether_dollar[, .(timestamp, eth_price = dollar, date)], 
                          by.x = "cut", by.y = "timestamp")
  trades_selleth <- trades_selleth[, .(date, cut, blockNumber, timestamp, transactionHash,
                                       eth_buyer = taker, eth_seller = maker, ether = tokenSell, token = tokenBuy,
                                       trade_amount_eth = amountSoldReal, trade_amount_dollar = amountSoldReal * eth_price,
                                       trade_amount_token = amountBoughtReal, token_price_in_eth = price)]
  # bind
  trades_eth <- rbindlist(list(trades_buyeth, trades_selleth))[order(blockNumber)]
  
  return(trades_eth)
}



#### SELF TRADES ####

filter_self_trades <- function(trades, save = TRUE, folder = "output", filename = "self_trades") {
  self_trades <- trades[eth_buyer == eth_seller]
  non_self_trades <- trades[eth_buyer != eth_seller]
  print(paste0("Info: filtered ", nrow(self_trades), " self-trades. ",
             nrow(non_self_trades), " non-self-trades remaining."))
  if(save) {
    # remove file type if given
    filename <- gsub("\\..*", "", filename)
    fwrite(self_trades, file = paste0(folder, "/", filename, ".csv"))
  }
  return(list(self_trades = self_trades, 
              non_self_trades = non_self_trades))
}

summarize_self_trades <- function(self_trades, save = TRUE, folder = "output", 
                                  filename = "self_trades_summary") {
  summary <- self_trades[, .(tx_count = .N, tx_sum_eth = sum(trade_amount_eth), tx_sum_dollar = sum(trade_amount_dollar), 
                             tx_sum_token = sum(trade_amount_token), start_date = min(.SD$date), end_date = max(.SD$date)),
                         by = .(trader = eth_buyer, token)]
  if(save) {
    # remove file type if given
    filename <- gsub("\\..*", "", filename)
    fwrite(summary, file = paste0(folder, "/", filename, ".csv"))
  }
  return(summary)
}




#### DETECT SCC PER TIME WINDOW ####

# assumptions:
# - trades columns: eth_buyer, eth_seller
add_trader_hashes <- function(trades) {
  # if there are no trader hashes yet, add all traders of the given trades
  if(nrow(global_trader_hashes) == 0) {
    num_traders <- length(unique(c(unique(trades$eth_buyer), unique(trades$eth_seller))))
    global_trader_hashes <<- data.table(trader_address = sort(unique(c(trades$eth_buyer, trades$eth_seller))),
                                        trader_id = as.character(1:num_traders))
  } else { # add additional traders
    additional_traders <- setdiff(x = sort(unique(c(trades$eth_buyer, trades$eth_seller))),
                                  y = global_trader_hashes$trader_address)
    if(length(additional_traders) != 0) {
      n_old <- nrow(global_trader_hashes)
      n_new <- n_old + length(additional_traders)
      global_trader_hashes <<- rbindlist(list(global_trader_hashes,
                                              data.table(trader_address = additional_traders, 
                                                         trader_id = seq(n_old+1, n_new, 1))))
    }
  }
  trades <- merge(trades, global_trader_hashes[, .(eth_buyer_id = trader_id, trader_address)], 
                  by.x = "eth_buyer", by.y = "trader_address")
  trades <- merge(trades, global_trader_hashes[, .(eth_seller_id = trader_id, trader_address)], 
                  by.x = "eth_seller", by.y = "trader_address")[order(timestamp)]
  return(trades)
}

# creates hash environments that are bound by (assigned to) the Global Env
create_scc_hash_environment <- function() {
  # create uniform variable name for hash environment
  hash_env_name <- paste("hash_scc", global_num_hash_envs+1, sep = "_")
  hash_env_name_inv <- paste("hash_scc", global_num_hash_envs+1, "inv", sep = "_")
  # create and assign hash environments
  assign(hash_env_name,
         new.env(hash = TRUE, parent = emptyenv(), size = 50000L), 
         envir = globalenv())
  assign(hash_env_name_inv,
         new.env(hash = TRUE, parent = emptyenv(), size = 50000L),
         envir = globalenv())
  # save globally a number and list of existing hash environments
  global_num_hash_envs <<- global_num_hash_envs + 1
  global_hash_env_names <<- c(global_hash_env_names, hash_env_name)
  global_inv_hash_env_names <<-c(global_inv_hash_env_names, hash_env_name_inv)
}

# for given window trades, build graph and detect strongly connected components
# assumptions:
# - trades column names: eth_buyer_id, eth_seller_id, trade_amount_eth
get_scc <- function(window_trades) {
  
  # create graph for window trades
  window_g <- graph.data.frame(d = unique(window_trades[, .(eth_seller_id, eth_buyer_id)]), directed = TRUE)
  window_scc <- components(window_g, mode = "strong")
  interesting_scc_ids <- unique(window_scc$membership[window_scc$membership %in% which(window_scc$csize > 1)])
  # result vectors:
  scc_hashes <- character()
  num_traders <- numeric()
  tx_count <- numeric()
  tx_sum_eth <- numeric()
  tx_sum_dollar <- numeric()
  
  # per relevant SCC (of size > 1):
  for (i in interesting_scc_ids) {
    # get trader names
    named_vertices <- names(which(window_scc$members == i))
    # add number of traders to result set
    num_traders <- c(num_traders, length(named_vertices))
    # get trades belonging to this SCC
    tx <- window_trades[eth_buyer_id %in% named_vertices & eth_seller_id %in% named_vertices]
    # add number of trades to result set
    tx_count <- c(tx_count, nrow(tx))
    # add total trade amount to result set
    tx_sum_eth <- c(tx_sum_eth, sum(tx$trade_amount_eth))
    tx_sum_dollar <- c(tx_sum_dollar, sum(tx$trade_amount_dollar))
    
    # HASHING
    # hash_key contains the IDs of all traders within SCC, sorted
    # hash_value is a counter of all already seen SCC
    hash_key <- traders_to_hash(named_vertices)
    
    found_scc <- F
    
    # check all currently existing hash environments if SCC is already known
    for (i in seq_len(global_num_hash_envs)) {
      # call hash environments, which are bound by (assigned to) the global environment
      if(exists_hash(x = hash_key, 
                     envir = get(global_hash_env_names[[i]], envir = globalenv()))) {
        hash_value <- get_hash(x = hash_key, 
                      envir = get(global_hash_env_names[[i]], envir = globalenv()))
        # add known SCC ID to result set
        scc_hashes <- c(scc_hashes, hash_value)
        found_scc <- T
        break()
      }
    }
    # if we haven't found the SCC in any hash environment, add new SCC:
    if(!found_scc) {
      # if number of existing SCC is a multiple of max size of environments, i.e. environments are full, 
      # create new environment:
      if(global_num_scc %% global_max_hash_env_size == 0) {
        create_scc_hash_environment()
      }
      # increment number of already seen SCC and use this as hash value
      global_num_scc <<- global_num_scc + 1
      hash_value <- global_num_scc
      # append to current hash environment
      assign_hash(x = hash_key, value = hash_value,
                  envir = get(global_hash_env_names[[global_num_hash_envs]], envir = globalenv()))
      # append to inverse hash environment, to find traders by SCC ID later
      assign_hash(x = as.character(hash_value), value = hash_key,
                  envir = get(global_inv_hash_env_names[[global_num_hash_envs]], envir = globalenv()))
      # add known SCC ID to result set
      scc_hashes <- c(scc_hashes, hash_value)
    }
  }
  return(list(scc_hashes, num_traders, tx_count, tx_sum_eth, tx_sum_dollar))
}

# assumption: tumbling window
detect_scc_for_token_and_time_window <- function(trades, window_size_in_seconds, window_size_name, window_start = NULL,
                                                save = TRUE, folder = "output", filename = "scc") {
  # if window start is not given, take start of first day of given trades
  if(missing(window_start)) {
    window_start <- min(trades$cut)
  }
  # breaks from start to last timestamp, by given steps in seconds
  breaks <- seqlast(window_start, max(trades$timestamp), window_size_in_seconds)
  # group trades by token and time windows
  # (time windows are defined as [break, next_break) )
  token_scc <- trades[, get_scc(.SD), by = .(token,
                                             bins = cut(timestamp, breaks, right = F, include.lowest = T, dig.lab = 12))]
  colnames(token_scc) <- c("token", "time", "scc_hash", "num_traders", "tx_count", "tx_sum_eth", "tx_sum_dollar")
  
  if(save) {
    # remove file type
    filename <- gsub("\\..*", "", filename)
    fwrite(token_scc, file = paste0(folder, "/", filename, "_", window_size_name, ".csv"))
  }
  return(token_scc)
}

detect_scc_for_tokens_layered <- function(trades, save = TRUE, folder = "output", filename = "scc") {
  
  tokenVector <- unique(trades$token)
  result <- c()
  
  pb <- txtProgressBar(min = 0, max = length(tokenVector), style = 3)
  for (token_index in 1:length(tokenVector)) {
    tokenName <- tokenVector[token_index]
    g <- graph_from_data_frame(trades[token == tokenName, list(eth_buyer_id, eth_seller_id, weight=1)])
    gs <- simplify(g, edge.attr.comb = length)
    
    while(vcount(gs) > 0) {
      comps <- components(gs, "strong")
      ids_larger_one <- which(comps$csize > 1)
      if(length(ids_larger_one) == 0) {
        gs <- delete_vertices(gs, V(gs))
        next
      }
      for(c_id in seq(1, length(ids_larger_one))) {
        c_v_ids <- which(comps$membership %in% ids_larger_one[c_id])
        c_v_names <- vertex_attr(gs, "name", c_v_ids)
        sorted_members <- sort(c_v_names)
        c_hash <- paste(digest::digest2int(paste0(sorted_members, collapse=",")))
        global_scc_traders_map[[c_hash]] <- sorted_members
        result <- c(result, c_hash)
      }
      edge_attr(gs, "weight") <- edge_attr(gs, "weight") - 1
      gs <- delete_edges(gs, which(edge_attr(gs, "weight") == 0))
      gs <- delete_vertices(gs, degree(gs)==0)
    }
    setTxtProgressBar(pb, token_index)
  }
  scc_dt <- data.table(scc_hash=result)[, list(occurrence = .N), by=scc_hash]
  scc_dt$num_traders <- sapply(scc_dt$scc_hash, function(x) {length(global_scc_traders_map[[x]])})
  
  if(save) {
    # remove file type
    filename <- gsub("\\..*", "", filename)
    fwrite(scc_dt, file = paste0(folder, "/", filename, ".csv"))
    mapping <- do.call(rbind, lapply(keys(global_scc_traders_map),
                                     function(x) {data.table(hash=x, trader_id=global_scc_traders_map[[x]])}))
    fwrite(mapping, file = paste0(folder, "/", filename, "-mapping.csv"), row.names = F)
  }
  
  return(scc_dt)
}

get_summary_of_scc <- function(scc_for_token_and_time_window, window_size_name, 
                               save = TRUE, folder = "output", filename = "scc_summary") {
  summary <- scc_for_token_and_time_window[, .(rank_token = .N, 
                                               rank = uniqueN(time), 
                                               tx_count = sum(tx_count), 
                                               tx_count_per_trader = sum(tx_count)/num_traders,
                                               tx_sum_eth = sum(tx_sum_eth), 
                                               tx_sum_eth_per_trader = sum(tx_sum_eth)/num_traders, 
                                               tx_sum_dollar = sum(tx_sum_dollar), 
                                               tx_sum_dollar_per_trader = sum(tx_sum_dollar)/num_traders, 
                                               num_tokens = uniqueN(token)), 
                                           by = .(scc_hash, num_traders)]
  if(save){
    # remove file type
    filename <- gsub("\\..*", "", filename)
    fwrite(summary, file = paste0(folder, "/", filename, "_", window_size_name, ".csv"))
  }
  return(summary)
}




#### DETECT WASH TRADES ####

# assumption: column names of scc_summary
# PROBABLY OBSOLETE
get_relevant_scc_by_thresholds <- function(scc_summary, 
                                           min_token_rank = 0, 
                                           min_tx_count_per_trader = 0, 
                                           min_tx_sum_eth_per_trader = 0) {
  return(scc_summary[rank_token >= min_token_rank &
                       tx_count_per_trader >= min_tx_count_per_trader &
                       tx_sum_eth_per_trader >= min_tx_sum_eth_per_trader]$scc_hash)
}

get_relevant_scc_by_threshold <- function(scc_dt, threshold) {
  #relevant <- head(scc_dt[order(occurrence, decreasing = T)], ceiling(nrow(scc_dt)*threshold))
  relevant <- scc_dt[occurrence >= threshold]
  print(paste("Info: Determined", nrow(relevant), "unique SCCs to be relevant at threshold", threshold))
  print(paste("Info: Minimum occurrence is", min(relevant$occurrence)))
  return(relevant$scc_hash)
}

detect_and_label_wash_trades_for_scc_using_multiple_passes <- function(trades, relevant_scc, window_sizes_in_seconds, window_start, ether = TRUE, margin,
                                                                       save = TRUE, folder = "output", filename = "wash_trades_multiple_windows") {
  # copy trades in order to label them
  print(paste("Starting wash trade labeling with", length(window_sizes_in_seconds), "passes."))
  trades$wash_label <- NA
  
  # if window start is not given, take start of first day of given trades
  if(missing(window_start)) {
    window_start <- min(trades$cut)
  }
  
  wash_trades <- list()
  
  # run for all given window sizes
  window_size_count <- length(window_sizes_in_seconds)
  relevant_scc_count <- length(relevant_scc)
  pb <- txtProgressBar(min = 0, max = window_size_count*relevant_scc_count, style = 3)
  for (window_size_index in 1:length(window_sizes_in_seconds)) {
    window_size <- window_sizes_in_seconds[window_size_index]
    # breaks from start to last timestamp (incl.), by given steps in seconds
    breaks <- seqlast(window_start, max(trades$timestamp), window_size)
    
    # for each relevant SCC
    for (scc.id.index in 1:length(relevant_scc)) {
      scc.id <- relevant_scc[scc.id.index]
      scc.traders <- global_scc_traders_map[[scc.id]]
      # get trades within scc that have not been labeled as wash trades yet
      scc.trades <- trades[eth_seller_id %in% scc.traders & eth_buyer_id %in% scc.traders &
                             (wash_label == FALSE | is.na(wash_label))][order(cut)]
      
      if(nrow(scc.trades) == 0) {
        wash_trades[[scc.id]][[as.character(window_size)]] <- list()
        next()
      }
      
      # label these trades as FALSE in original trade set to indicate they have been checked
      trades[transactionHash %in% scc.trades$transactionHash]$wash_label <- FALSE
      # prepare trades
      if (ether) {
        temp_trades <- scc.trades[, .(transactionHash, token, date, timestamp, buyer = eth_buyer, seller = eth_seller, 
                                      amount = trade_amount_eth, trade_amount_dollar, wash_label)]
      } else {
        temp_trades <- scc.trades[, .(transactionHash, token, date, timestamp, buyer = eth_seller, seller = eth_buyer, 
                                      amount = trade_amount_token, trade_amount_dollar, wash_label)]
      }
      # split trades by token and given time window size and run wash trade detect function
      # (time windows are defined as [break, next_break) using right=F and include.lowest=T)
      temp_trades_per_token_and_window <- split(temp_trades, list(temp_trades$token, 
                                                                  cut(temp_trades$timestamp, breaks, right = F, include.lowest = T, dig.lab = 12)), 
                                                drop = TRUE)
      scc.wash_trades_all <- lapply(temp_trades_per_token_and_window,
                                    FUN = detect_label_wash_trades,
                                    margin = margin)
      # add to final result
      wash_trades[[scc.id]][[as.character(window_size)]] <- scc.wash_trades_all
      # label wash trades in original trade set
      checked_trades <- rbindlist(scc.wash_trades_all)
      trades[transactionHash %in% checked_trades[wash_label == TRUE]$transactionHash]$wash_label <- TRUE
      
      setTxtProgressBar(pb, (window_size_index - 1) * length(relevant_scc) + scc.id.index)
    }
  }
  if (save) {
    # remove file type if given
    filename <- gsub("\\..*", "", filename)
    save(wash_trades, file = paste0(folder, "/", filename, ".RData"))
    fwrite(trades, file = paste0(folder, "/trades_labeled.csv"))
  }
  return(list(wash_trades, trades))
}

get_summary_of_wash_trades_per_scc_and_timewindow <- function(wash_trades, window_size_name, multiple_passes = FALSE, 
                                                              save = TRUE, folder = "output", filename = "wash_trades_summary") {
  # define result data.table
  print("Info: producing wash trading summary...")
  wash_trades_dt <- data.table(scc_hash = character(),
                               token = character(),
                               window_size = character(),
                               time = character(),
                               num_wash_trades = numeric(),
                               num_trades = numeric(),
                               total_amount_wash = numeric(),
                               total_amount = numeric(),
                               total_amount_dollar_wash = numeric(),
                               total_amount_dollar = numeric())
  # if there are multiple passes, i.e. multiple time windows:
  if (multiple_passes) {
    # for each SCC
    for (scc in names(wash_trades)) {
      # for each time window size
      for (window_size in names(wash_trades[[scc]])) {
        # for each time interval
        for (w in seq_len(length(wash_trades[[scc]][[window_size]]))) {
          # list names contain token and time window
          temp <- strsplit(names(wash_trades[[scc]][[window_size]])[w], split = "\\.")[[1]]
          token <- temp[1]
          window <- temp[2]
          num_wash <- nrow(wash_trades[[scc]][[window_size]][[w]][wash_label == TRUE])
          num_all <- nrow(wash_trades[[scc]][[window_size]][[w]])
          amount_wash <- sum(wash_trades[[scc]][[window_size]][[w]][wash_label == TRUE]$amount)
          amount_all <- sum(wash_trades[[scc]][[window_size]][[w]]$amount)
          amount_dollar_wash <- sum(wash_trades[[scc]][[window_size]][[w]][wash_label == TRUE]$trade_amount_dollar)
          amount_dollar_all <- sum(wash_trades[[scc]][[window_size]][[w]]$trade_amount_dollar)
          wash_trades_dt <- rbindlist(list(wash_trades_dt, 
                                           list(scc, token, window_size, window, num_wash, num_all, amount_wash, 
                                                amount_all, amount_dollar_wash, amount_dollar_all)))
        }
      }
    }
  } else { # if the wash trades were done just for one time window, i.e. there are no multiple passes:
    # remove variable for time window size
    wash_trades_dt$window_size <- NULL
    # for each SCC
    for (scc in names(wash_trades)) {
      # for each time interval
      for (w in seq_len(length(wash_trades[[scc]]))) {
        # list names contain token and time window
        temp <- strsplit(names(wash_trades[[scc]])[w], split = "\\.")[[1]]
        token <- temp[1]
        window <- temp[2]
        num_wash <- nrow(wash_trades[[scc]][[w]][wash_label == TRUE])
        num_all <- nrow(wash_trades[[scc]][[w]])
        amount_wash <- sum(wash_trades[[scc]][[w]][wash_label == TRUE]$amount)
        amount_all <- sum(wash_trades[[scc]][[w]]$amount)
        amount_dollar_wash <- sum(wash_trades[[scc]][[w]][wash_label == TRUE]$trade_amount_dollar)
        amount_dollar_all <- sum(wash_trades[[scc]][[w]]$trade_amount_dollar)
        wash_trades_dt <- rbindlist(list(wash_trades_dt, 
                                         list(scc, token, window, num_wash, num_all, amount_wash, 
                                              amount_all, amount_dollar_wash, amount_dollar_all)))
      }
    }
  }
  # save file
  if (save) {
    # remove file type
    filename <- gsub("\\..*", "", filename)
    fwrite(wash_trades_dt, file = paste0(folder, "/", filename, "_", window_size_name, ".csv"))
  }
  return(wash_trades_dt)
}




#### HELPER FUNCTIONS ####

# get sequences including last element, even if it does not match the window sizes
seqlast <- function (from, to, by) {
  vec <- do.call(what = seq, args = list(from, to, by))
  if ( tail(vec, 1) != to ) {
    return(c(vec, to))
  } else {
    return(vec)
  }
}

# create uniform hashes from trader names
traders_to_hash <- function(traders) {
  return(paste(c("h", sort(traders)), collapse = "_"))
}

# turn uniform hashes back into trader names
hash_to_traders <- function(trader_hash) {
  # split by "_"
  temp <- strsplit(trader_hash, "_")[[1]]
  # drop h
  return(temp[temp != "h"])
}

# get address clusters by SCC hash
get_address_clusters <- function(trades, scc_ids, save = TRUE, folder = "output", filename = "address_clusters") {
  address_clusters <- list()
  
  # for each SCC
  for (scc.id in scc_ids) {
    scc.traders <- global_scc_traders_map[[scc.id]]
    # add to address clusters
    address_clusters[[as.character(scc.id)]] <- global_trader_hashes[trader_id %in% scc.traders]$trader_address
  }
  # save file
  if (save) {
    # remove file type
    filename <- gsub("\\..*", "", filename)
    write(toJSON(address_clusters), file = paste0(folder, "/", filename, ".json"))
  }
  return(address_clusters)
}

# get window size name from seconds for specified sizes
get_window_size_name <- function(seconds) {
  seconds <- as.character(seconds)
  switch (seconds,
          "604800" = "week",
          "172800" = "2days",
          "86400" = "day",
          "43200" = "12hrs",
          "21600" = "6hrs",
          "3600" = "hour",
          "1800" = "30mins",
          "900" = "15mins",
          "300" = "5mins",
          "60" = "minute")
}


#### DEFINE ENTIRE PIPELINE ####
call_IDEX_pipeline <- function(IDEXtrades_file = "data/IDEX-preprocessed.csv",
                               EtherDollarPrice_file = "data/EtherDollarPrice.csv", 
                               output_folder = "output_IDEX",
                               scc_threshold_rank = 100,
                               wash_trade_detection_ether = TRUE,
                               wash_trade_detection_margin = 0.1,
                               wash_window_sizes_seconds = c(60*60*24*7)) {
  
  # create directory for output
  dir.create(output_folder)
  
  # load and prepare IDEX trades
  trades <- load_trades(file_csv = IDEXtrades_file)
  trades <- get_successful_and_complete_trades(trades = trades, 
                                               status_column = quote(status), 
                                               status_success = 1)
  trades <- get_ether_token_trades(trades = trades, 
                                    token_column1 = quote(tokenBuy), 
                                    token_column2 = quote(tokenSell))
  trades <- merge_trades_with_daily_usd_price(trades = trades,
                                              price_file_csv = EtherDollarPrice_file)
  
  # filter self trades
  l <- filter_self_trades(trades = trades,
                          save = TRUE,
                          folder = output_folder)
  self_trades <- l[["self_trades"]]
  self_trades_summary <- summarize_self_trades(self_trades = self_trades,
                                               save = TRUE,
                                               folder = output_folder)
  trades <- l[["non_self_trades"]]
  
  # detect SCC
  trades <- add_trader_hashes(trades = trades)
  scc_dt <- detect_scc_for_tokens_layered(trades = trades, save = TRUE, folder = output_folder)
  
  relevant_scc_ids <- get_relevant_scc_by_threshold(scc_dt, scc_threshold_rank)
  
  wash_trades_multiple_passes <- detect_and_label_wash_trades_for_scc_using_multiple_passes(trades = trades,
                                                                                            relevant_scc = relevant_scc_ids,
                                                                                            window_sizes_in_seconds = wash_window_sizes_seconds,
                                                                                            ether = wash_trade_detection_ether,
                                                                                            margin = wash_trade_detection_margin,
                                                                                            save = TRUE,
                                                                                            folder = output_folder)
  trades_labeled <- wash_trades_multiple_passes[[2]]
  wash_trades_multiple_passes <- wash_trades_multiple_passes[[1]]
  wash_trades_multiple_passes_summary <- get_summary_of_wash_trades_per_scc_and_timewindow(wash_trades = wash_trades_multiple_passes,
                                                                                           window_size_name = "multiple_windows",
                                                                                           multiple_passes = TRUE,
                                                                                           save = TRUE,
                                                                                           folder = output_folder)
  
  # get address clusters
  get_address_clusters(trades = trades,
                       scc_ids = relevant_scc_ids,
                       save = TRUE,
                       folder = output_folder)
  
  return()
}

call_EtherDelta_pipeline <- function(EtherDeltaTrades_file = "data/EtherDeltaTrades-preprocessed.csv",
                                     EtherDollarPrice_file = "data/EtherDollarPrice.csv", 
                                     output_folder = "output_EtherDelta",
                                     scc_threshold_rank = 100,
                                     wash_trade_detection_ether = TRUE,
                                     wash_trade_detection_margin = 0.1,
                                     wash_window_sizes_seconds = c(60*60*24*7)) {
  
  # create directory for output
  dir.create(output_folder)
  
  # load and prepare EtherDelta trades
  trades <- load_trades(file_csv = EtherDeltaTrades_file)
  trades <- get_successful_and_complete_trades(trades = trades)
  trades <- get_ether_token_trades(trades = trades,
                                    token_column1 = quote(tokenBuy),
                                    token_column2 = quote(tokenSell))
  trades <- merge_EtherDelta_trades_with_daily_usd_price(trades = trades,
                                                         price_file_csv = EtherDollarPrice_file)
  
  # filter self trades
  l <- filter_self_trades(trades = trades,
                          save = TRUE,
                          folder = output_folder)
  self_trades <- l[["self_trades"]]
  self_trades_summary <- summarize_self_trades(self_trades = self_trades,
                                               save = TRUE,
                                               folder = output_folder)
  trades <- l[["non_self_trades"]]
  
  # detect SCC
  trades <- add_trader_hashes(trades = trades)
  scc_dt <- detect_scc_for_tokens_layered(trades = trades, save = TRUE, folder = output_folder)
  
  relevant_scc_ids <- get_relevant_scc_by_threshold(scc_dt, scc_threshold_rank)
  wash_trades_multiple_passes <- detect_and_label_wash_trades_for_scc_using_multiple_passes(trades = trades,
                                                                                            relevant_scc = relevant_scc_ids,
                                                                                            window_sizes_in_seconds = wash_window_sizes_seconds,
                                                                                            ether = wash_trade_detection_ether,
                                                                                            margin = wash_trade_detection_margin,
                                                                                            save = TRUE,
                                                                                            folder = output_folder)
  trades_labeled <- wash_trades_multiple_passes[[2]]
  wash_trades_multiple_passes <- wash_trades_multiple_passes[[1]]
  wash_trades_multiple_passes_summary <- get_summary_of_wash_trades_per_scc_and_timewindow(wash_trades = wash_trades_multiple_passes,
                                                                                           window_size_name = "multiple_windows",
                                                                                           multiple_passes = TRUE,
                                                                                           save = TRUE,
                                                                                           folder = output_folder)
  
  # get address clusters
  get_address_clusters(trades = trades,
                       scc_ids = relevant_scc_ids,
                       save = TRUE,
                       folder = output_folder)
  
  return()
}

#### MAIN CALL ####

# parse arguments
option_list <- list(
  make_option(c("-d", "--dex"), type="character", default="IDEX", 
              help="name of DEX, must be either 'IDEX' or 'EtherDelta' [default= %default]"),
  make_option(c("-t", "--trades"), type="character", default="data/IDEXTrades-preprocessed.csv", 
              help="trade dataset file name [default= %default]"),
  make_option(c("-p", "--prices"), type="character", default="data/EtherDollarPrice.csv", 
              help="Ether-Dollar-Price file name [default= %default]"),
  make_option(c("-o", "--output"), type="character", default="output_IDEX", 
              help="output folder name [default= %default]"),
  make_option(c("--sccthresholdrank"), type="integer", default=100, 
              help="threshold for relevant SCC: rank [default= %default]"),
  make_option(c("--washdetectionether"), type="logical", action = "store", default=TRUE, 
              help="should wash trades be detected for Ether amounts (TRUE) or Token amounts (FALSE) [default= %default]"),
  make_option(c("-m", "--margin"), type = "double", default = 0.1,
              help = "margin of mean left trader position for wash trade detection [default= %default]"),
  make_option(c("--washwindowsizesecondspass1"), type="integer", default=60*60*24*7, 
              help="wash trade detection window size for first pass in seconds [default= %default]"),
  make_option(c("--washwindowsizesecondspass2"), type="integer", default=NULL, 
              help="wash trade detection window size for second pass in seconds [default= %default]"),
  make_option(c("--washwindowsizesecondspass3"), type="integer", default=NULL, 
              help="wash trade detection window size for third pass in seconds [default= %default]")
)

opt <- parse_args(OptionParser(option_list=option_list))

# prepare wash window sizes
wash_window_sizes_args <- c(opt$washwindowsizesecondspass1)
if (!is.null(opt$washwindowsizesecondspass2)) {
  wash_window_sizes_args <- c(wash_window_sizes_args, opt$washwindowsizesecondspass2)
}
if (!is.null(opt$washwindowsizesecondspass3)) {
  wash_window_sizes_args <- c(wash_window_sizes_args, opt$washwindowsizesecondspass3)
}

# call IDEX or EtherDelta pipeline
if (opt$dex == "IDEX") {
  call_IDEX_pipeline(IDEXtrades_file = opt$trades,
                     EtherDollarPrice_file = opt$prices,
                     output_folder = opt$output,
                     scc_threshold_rank = opt$sccthresholdrank,
                     wash_trade_detection_ether = opt$washdetectionether,
                     wash_trade_detection_margin = opt$margin,
                     wash_window_sizes_seconds = wash_window_sizes_args)
} else if (opt$dex == "EtherDelta") {
  call_EtherDelta_pipeline(EtherDeltaTrades_file = opt$trades,
                           EtherDollarPrice_file = opt$prices,
                           output_folder = opt$output,
                           scc_threshold_rank = opt$sccthresholdrank,
                           wash_trade_detection_ether = opt$washdetectionether,
                           wash_trade_detection_margin = opt$margin,
                           wash_window_sizes_seconds = wash_window_sizes_args)
}
