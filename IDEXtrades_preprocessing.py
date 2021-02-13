#!/usr/bin/env python
# coding: utf-8

# # IDEX Trades Preprocessing
# 
# This script preprocesses the IDEX trades.
# 
# ##### Input
# A csv file of IDEX trades with following columns:
# - transaction_hash
# - status
# - block_number
# - gas
# - gas_price
# - timestamp
# - amountBuy
# - amountSell
# - expires
# - nonce
# - amount
# - tradeNonce
# - feeMake
# - feeTake
# - tokenBuy
# - tokenSell
# - maker
# - taker
# 
# A file containing decimals for tokens, containing at least following columns:
# - address
# - decimals
# 
# ##### Preprocessing Steps
# 1. The amount fields (amountBuy, amountSell, amount) are given in a certain decimal representation, depending on the decimals of the token. 
#    Convert all amounts to the correct value according to given decimals per token. If no decimal is given for a token, 18 is assumed.
# 2. amountBuy is the order amount of tokenBuy; amountSell is the order amount of tokenSell; amount is the actual amount bought of tokenBuy. 
#    Calculate the price of the tokenBuy in units of tokenSell from the ratio of amountBuy and amountSell. Using the price, the actual amount sold is calculated.
# 
# (source: https://gist.github.com/raypulver/2f318db5dc497cab8019d3ae391af1d29

import pandas as pd
import numpy as np
import sys
import getopt

def main(argv):

  # get input arguments
  idexfile = ''
  decimalsfile = ''
  outputfile = ''
  short_options = "hi:d:o:"
  long_options = ["help", "idexfile=", "decimalsfile=" "outputfile="]
  
  try:
    opts, args = getopt.getopt(argv, short_options, long_options)
  except getopt.GetoptError:
    print('IDEXtrades_preprocessing.py -i <idexfile> -d <decimalsfile> -o <outputfile>')
    sys.exit(2)
  for opt, arg in opts:
    if opt == '-h':
      print('IDEXtrades_preprocessing.py -i <idexfile> -d <decimalsfile> -o <outputfile>')
      sys.exit()
    elif opt in ("-i", "--idexfile"):
      idexfile = arg
    elif opt in ("-d", "--decimalsfile"):
      decimalsfile = arg
    elif opt in ("-o", "--outputfile"):
      outputfile = arg
  # if any argument is missing, stop
  if (idexfile == '' or decimalsfile == '' or outputfile == ''):
    print('Warning: missing arguments. Arguments need to be supplied as follows:')
    print('IDEXtrades_preprocessing.py -i <idexfile> -d <decimalsfile> -o <outputfile>')
    sys.exit(2)

  print("Input file for IDEX trades is ", idexfile)
  print("Decimals file for token decimals is ", decimalsfile)
  print("Output file for preprocessed IDEX trades is ", outputfile)

  # read IDEX trades
  trades = pd.read_csv(idexfile, header=0)

  orig_len = len(trades)

  print("Info: read", orig_len, "rows from IDEX file.")

  ### DECIMALS

  # read token decimals file
  token_decimals = pd.read_json(decimalsfile, orient="index")
  token_decimals.reset_index(inplace=True)
  token_decimals.drop(labels=["index", "name", "slug"], axis=1, inplace=True)

  print("Info: read decimal information for", len(token_decimals), "tokens from Decimals file.")

  ## Merge with tokens in IDEX data and fill missing decimals with 18
  IDEX_tokens = pd.DataFrame(pd.unique(pd.concat([trades['tokenBuy'], trades['tokenSell']], axis=0)),
                             columns = ['address'])
  # check if IDEX tokens are missing in decimals file
  missing_IDEX_tokens = set(IDEX_tokens['address'].values) - set(token_decimals['address'].values)

  if(len(missing_IDEX_tokens) > 0):
    print("Warning:", len(missing_IDEX_tokens), "tokens in IDEX data have no decimal information in respective file. " +
      "As a default, 18 will be taken as their decimal.")

  token_decimals_all = pd.merge(token_decimals, IDEX_tokens,
                                how = 'right') # keep only IDEX tokens

  # replace NaN decimals with 18
  token_decimals_all[['decimals']] = token_decimals_all[['decimals']].fillna(value = 18)
  
  ### TRANSFORM AND MERGE
  # 
  # Challenges:
  # - safely convert the amount columns, which can have very large amounts (up to 55 digits)
  # - transform values according to decimals
  # 
  # Steps
  # 1. merge with decimals
  # 2. convert amounts to float using astype
  # 3. divide amounts by decimals
  
  ### TOKENBUY

  # left-merge trades with token_decimals on tokenBuy, preserving key order of trades_real
  trades_real = trades.merge(token_decimals_all, how="left", left_on="tokenBuy", right_on="address")

  # drop now duplicated column
  trades_real.drop(labels=["address"], axis=1, inplace=True)

  # convert to float & divide amount by decimals:
  # copy columns
  trades_real['amountBuyReal'] = trades_real['amountBuy']
  trades_real['amountBoughtReal'] = trades_real['amount']
  # convert to float
  trades_real = trades_real.astype({'amountBuyReal': float, 'amountBoughtReal': float})
  # divide
  trades_real['amountBuyReal'] = trades_real['amountBuyReal'].divide(10**trades_real['decimals'])
  trades_real['amountBoughtReal'] = trades_real['amountBoughtReal'].divide(10**trades_real['decimals'])

  # drop decimal column
  trades_real.drop(labels=['decimals'],
                  axis=1, inplace=True)

  print("Info: converted column 'amountBuy' to 'amountBuyReal' as the order amount of 'tokenBuy', " +
    "and 'amount' to 'amountBoughtReal' as the trade amount of 'tokenBuy', according to token decimals.")

  ### TOKENSELL

  # left-merge trades with token_decimals on tokenSell, preserving key order of trades
  trades_real = trades_real.merge(token_decimals_all, how="left", left_on="tokenSell", right_on="address")

  # drop now duplicated column
  trades_real.drop(labels=["address"], axis=1, inplace=True)

  # convert to float & divide amount by decimals:
  # copy columns
  trades_real['amountSellReal'] = trades_real['amountSell']
  # convert to float
  trades_real = trades_real.astype({'amountSellReal': float})
  # divide
  trades_real['amountSellReal'] = trades_real['amountSellReal'].divide(10**trades_real['decimals'])

  # drop decimal column
  trades_real.drop(labels=['decimals'],
                  axis=1, inplace=True)

  print("Info: converted column 'amountSell' to 'amountSellReal' as the order amount of 'tokenSell', according to token decimals.")

  ### PRICE AND AMOUNTSOLD

  # divide tokenSell by tokenBuy to get the price of tokenBuy in units of tokenSell
  # (1 tokenBuy = <price> tokenSell)
  trades_real['price'] = trades_real['amountSellReal'].divide(trades_real['amountBuyReal'])

  # price * actual amount bought = actual amount sold
  trades_real['amountSoldReal'] = trades_real['amountBoughtReal'].mul(trades_real['price'])

  print("Info: computed new column 'price' as price of tokenBuy in units of tokenSell, and column 'amountSoldReal' as the trade amount of 'tokenSell'.")

  ### FEES
  # assumption: feeMake and feeTake are a percentage, represented as a number with 18 decimals
  # (source: https://gist.github.com/raypulver/2f318db5dc497cab8019d3ae391af1d2)

  trades_real = trades_real.astype({'feeMake': float,
                                    'feeTake': float})
  trades_real['feeMake'] = trades_real['feeMake'].divide(10**18)
  trades_real['feeTake'] = trades_real['feeTake'].divide(10**18)

  print("Info: divided columns 'feeMake' and 'feeTake' by 10^18. " +
    "For more information, see https://gist.github.com/raypulver/2f318db5dc497cab8019d3ae391af1d2.")

  ### EXPORT

  # rename some columns
  trades_real = trades_real.rename(columns={'transaction_hash':'transactionHash',
                                            'block_number':'blockNumber',
                                            'gas_price':'gasPrice'
                                           })

  # reorder columns
  trades_real = trades_real[['blockNumber',
                             'timestamp',
                             'transactionHash',
                             'status',
                             'maker',
                             'taker',
                             'tokenBuy',
                             'tokenSell',
                             'amountBuyReal',
                             'amountBoughtReal',
                             'amountSellReal',
                             'amountSoldReal',
                             'price',
                             'feeMake',
                             'feeTake',
                             'gas',
                             'gasPrice',
                             'nonce',
                             'tradeNonce',
                             'expires']]

  # still same number of rows
  if len(trades_real) != orig_len:
    print("Warning: dropped", orig_len - len(trades_real), "rows during Preprocessing.")

  # save as csv
  trades_real.to_csv(outputfile, index=False)
  print("Info: saved file to " + outputfile)


if __name__ == "__main__":
   main(sys.argv[1:])
