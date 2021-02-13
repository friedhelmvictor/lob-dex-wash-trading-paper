# Detecting and Quantifying Wash Trading on Decentralized Cryptocurrency Exchanges

This repository contains the implementation of a wash trade detection process for data from the limit order book-based decentralized exchanges IDEX and EtherDelta.

## Instructions

You will need at least 32GB RAM, or large enough swap space to run everything.
It took about 2.5 hours to run everything on a Ryzen 5 3600.

### Data

The following data files can be downloaded from Zenodo, and should be placed in the `data` directory:
* `EtherDeltaTrades.csv`: EtherDelta trades
* `IDEXTrades.csv`: IDEX trades
* `EtherDollarPrice.csv`: daily ETH-USD prices
* `token_decimals.json`: decimals used by tokens for precision

### Preprocess Trades

In order to preprocess the trades, you must have Python 3 installed on your machine.

For IDEX, run:

```
IDEXtrades_preprocessing.py -i <idexfile> -d <decimalsfile> -o <outputfile>
```

and for EtherDelta, run:

```
EtherDeltatrades_preprocessing.py -i <etherdeltafile> -d <decimalsfile> -o <outputfile>
```

providing the paths to the data files mentioned above.


### Run Pipeline

In order to run the pipeline, you must have R version 3.5.1 installed on your machine, as well as the following R packages:
* optparse
* data.table
* igraph
* rjson
* Rcpp
* hash

The pipeline can be called by running ```Rscript pipeline_wash_trading_paper.R [options]```, using the following options.
The trades must be the preprocessed ones.

```
Options:
	-d DEX, --dex=DEX
		name of DEX, must be either 'IDEX' or 'EtherDelta' [default= IDEX]

	-t TRADES, --trades=TRADES
		trade dataset file name [default= data/IDEXTrades-preprocessed.csv]

	-p PRICES, --prices=PRICES
		Ether-Dollar-Price file name [default= data/EtherDollarPrice.csv]

	-o OUTPUT, --output=OUTPUT
		output folder name [default= output_IDEX]

	--sccthresholdrank=SCCTHRESHOLDRANK
		threshold for relevant SCC: rank [default= 100]

	--washdetectionether=WASHDETECTIONETHER
		should wash trades be detected for Ether amounts (TRUE) or Token amounts (FALSE) [default= TRUE]

	-m MARGIN, --margin=MARGIN
		margin of mean left trader position for wash trade detection [default= 0.1]

	--washwindowsizesecondspass1=WASHWINDOWSIZESECONDSPASS1
		wash trade detection window size for first pass in seconds [default= 604800]

	--washwindowsizesecondspass2=WASHWINDOWSIZESECONDSPASS2
		wash trade detection window size for second pass in seconds [default= NULL]

	--washwindowsizesecondspass3=WASHWINDOWSIZESECONDSPASS3
		wash trade detection window size for third pass in seconds [default= NULL]

	-h, --help
		Show this help message and exit
```

## How we ran it for the paper

Preprocessing:
```
python IDEXtrades_preprocessing.py -i data/IDEXTrades.csv -d data/token_decimals.json -o data/IDEXTrades-preprocessed.csv
python EtherDeltatrades_preprocessing.py -i data/EtherDeltaTrades.csv -d data/token_decimals.json -o data/EtherDeltaTrades-preprocessed.csv
```

### Wash Trade Detection
We use the following parameters:

- Perform the analysis based on token amounts, not Ether amounts
- Use an SCC threshold of minimum 100 occurences
- A margin of 1%
- Analysis windows: 1 hour, 1 day, 1 week

Executing wash trade detection for IDEX (this takes about 2 hours):
```
Rscript pipeline_wash_trading_paper.R -d "IDEX" \
-t data/IDEXTrades-preprocessed.csv \
-p data/EtherDollarPrice.csv \
-o output/idex-t100-1h-1d-1w-1pmargin \
--sccthresholdrank=100 \
--washdetectionether=FALSE \
-m 0.01 \
--washwindowsizesecondspass1=3600 \
--washwindowsizesecondspass2=86400 \
--washwindowsizesecondspass3=604800
```

Executing wash trade detection for IDEX (this takes about 15 minutes)
```
Rscript pipeline_wash_trading_paper.R -d "EtherDelta" \
-t data/EtherDeltaTrades-preprocessed.csv \
-p data/EtherDollarPrice.csv \
-o output/etherdelta-t100-1h-1d-1w-1pmargin \
--sccthresholdrank=100 \
--washdetectionether=FALSE \
-m 0.01 \
--washwindowsizesecondspass1=3600 \
--washwindowsizesecondspass2=86400 \
--washwindowsizesecondspass3=604800
```
### Statistics and Plots
For plotting, you will need the following additional R packages:
* ggplot2
* scales
* RColorBrewer
* ggthemes
* extrafont

To create the plots, run `paper_plots.R`.
Depending on your output file location, you may need to adjust some variables at the beginning of that file.
