#!/usr/bin/env python
# coding: utf-8

# # EtherDelta Trades Preprocessing
# 
# This script preprocesses the EtherDelta trades.
# 
# ##### Input
# A csv file of EtherDelta trades with following columns:
# - transaction_hash
# - block_number
# - timestamp
# - tokenGet
# - amountGet
# - tokenGive
# - amountGive
# - get (maker)
# - give (taker)
# 
# A file containing decimals for tokens, containing at least following columns:
# - address
# - decimals
# 
# ##### Preprocessing Steps
# 1. The amount fields (amountGet, amountGive) are given in a certain decimal representation, depending on the decimals of the token. 
#    Convert all amounts to the correct value according to given decimals per token. If no decimal is given for a token, 18 is assumed.
# 2. amountGet is the trade amount of tokenGet; amountGive is the trade amount of tokenGive
#    Calculate the price of the tokenGet in units of tokenGive from the ratio of amountGet and amountGive.
# 
# (source: https://gist.github.com/raypulver/2f318db5dc497cab8019d3ae391af1d29

import pandas as pd
import numpy as np
import sys
import getopt

def main(argv):

  # get input arguments
  etherdeltafile = ''
  decimalsfile = ''
  outputfile = ''
  short_options = "hi:d:o:"
  long_options = ["help", "etherdeltafile=", "decimalsfile=" "outputfile="]
  
  try:
    opts, args = getopt.getopt(argv, short_options, long_options)
  except getopt.GetoptError:
    print('EtherDeltatrades_preprocessing.py -i <etherdeltafile> -d <decimalsfile> -o <outputfile>')
    sys.exit(2)
  for opt, arg in opts:
    if opt == '-h':
      print('EtherDeltatrades_preprocessing.py -i <etherdeltafile> -d <decimalsfile> -o <outputfile>')
      sys.exit()
    elif opt in ("-i", "--etherdeltafile"):
      etherdeltafile = arg
    elif opt in ("-d", "--decimalsfile"):
      decimalsfile = arg
    elif opt in ("-o", "--outputfile"):
      outputfile = arg
  # if any argument is missing, stop
  if (etherdeltafile == '' or decimalsfile == '' or outputfile == ''):
    print('Warning: missing arguments. Arguments need to be supplied as follows:')
    print('EtherDeltatrades_preprocessing.py -i <etherdeltafile> -d <decimalsfile> -o <outputfile>')
    sys.exit(2)

  print("Input file for EtherDelta trades is ", etherdeltafile)
  print("Decimals file for token decimals is ", decimalsfile)
  print("Output file for preprocessed EtherDelta trades is ", outputfile)

  # read EtherDelta trades
  trades = pd.read_csv(etherdeltafile, header=0)

  orig_len = len(trades)

  print("Info: read", orig_len, "rows from EtherDelta file.")

  ### DECIMALS

  # read token decimals file
  token_decimals = pd.read_json(decimalsfile, orient="index")
  token_decimals.reset_index(inplace=True)
  token_decimals.drop(labels=["index", "name", "slug"], axis=1, inplace=True)

  print("Info: read decimal information for", len(token_decimals), "tokens from Decimals file.")

  ## Merge with tokens in EtherDelta data and fill missing decimals with 18
  EtherDelta_tokens = pd.DataFrame(pd.unique(pd.concat([trades['tokenGet'], trades['tokenGive']], axis=0)), columns = ['address'])
  # check if EtherDelta tokens are missing in decimals file
  missing_EtherDelta_tokens = set(EtherDelta_tokens['address'].values) - set(token_decimals['address'].values)

  if(len(missing_EtherDelta_tokens) > 0):
    print("Warning:", len(missing_EtherDelta_tokens), "tokens in EtherDelta data have no decimal information in respective file. " +
      "As a default, 18 will be taken as their decimal.")

  token_decimals_all = pd.merge(token_decimals, EtherDelta_tokens,
                                how = 'right') # keep only EtherDelta tokens

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
  
  ### TOKENGET

  # left-merge trades with token_decimals on tokenGet, preserving key order of trades_real
  trades_real = trades.merge(token_decimals_all, how="left", left_on="tokenGet", right_on="address")

  # drop now duplicated column
  trades_real.drop(labels=["address"], axis=1, inplace=True)

  # convert to float & divide amount by decimals:
  # copy columns
  trades_real['amountBoughtReal'] = trades_real['amountGet']
  # convert to float
  trades_real = trades_real.astype({'amountBoughtReal': float})
  # divide
  trades_real['amountBoughtReal'] = trades_real['amountBoughtReal'].divide(10**trades_real['decimals'])

  # drop decimal column
  trades_real.drop(labels=['decimals'], axis=1, inplace=True)

  print("Info: converted column 'amountGet' to 'amountBoughtReal' as the trade amount of 'tokenGet', according to token decimals.")

  ### TOKENGIVE

  # left-merge trades with token_decimals on tokenGive, preserving key order of trades
  trades_real = trades_real.merge(token_decimals_all, how="left", left_on="tokenGive", right_on="address")

  # drop now duplicated column
  trades_real.drop(labels=["address"], axis=1, inplace=True)

  # convert to float & divide amount by decimals:
  # copy columns
  trades_real['amountSoldReal'] = trades_real['amountGive']
  # convert to float
  trades_real = trades_real.astype({'amountSoldReal': float})
  # divide
  trades_real['amountSoldReal'] = trades_real['amountSoldReal'].divide(10**trades_real['decimals'])

  # drop decimal column
  trades_real.drop(labels=['decimals'], axis=1, inplace=True)

  print("Info: converted column 'amountGive' to 'amountSoldReal' as the trade amount of 'tokenGive', according to token decimals.")

  ### PRICE

  # divide tokenGive by tokenGet to get the price of tokenGet in units of tokenGive
  # (1 tokenGet = <price> tokenGive)
  trades_real['price'] = trades_real['amountSoldReal'].divide(trades_real['amountBoughtReal'])

  print("Info: computed new column 'price' as price of tokenGet in units of tokenGive.")

  ### EXPORT

  # rename some columns
  trades_real = trades_real.rename(columns={'transaction_hash':'transactionHash',
                                            'block_number':'blockNumber',
                                            'tokenGet':'tokenBuy',
                                            'tokenGive':'tokenSell',
                                            'get':'maker',
                                            'give':'taker'
                                           })

  trades_real = trades_real[['blockNumber',
                             'timestamp',
                             'transactionHash',
                             'maker',
                             'taker',
                             'tokenBuy',
                             'tokenSell',
                             'amountBoughtReal',
                             'amountSoldReal',
                             'price']]

  # still same number of rows
  if len(trades_real) != orig_len:
    print("Warning: dropped", orig_len - len(trades_real), "rows during Preprocessing.")

  # save as csv
  trades_real.to_csv(outputfile, index=False)
  print("Info: saved file to " + outputfile)


if __name__ == "__main__":
   main(sys.argv[1:])
