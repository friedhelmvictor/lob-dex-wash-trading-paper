#include <Rcpp.h>
using namespace Rcpp;
// [[Rcpp::plugins(cpp11)]]

Function subset("[.data.frame"); // to easily subset data frames...

// [[Rcpp::export]]
DataFrame detect_label_wash_trades(DataFrame df, double margin = 0.1){
  StringVector buyers = df["buyer"];
  StringVector sellers = df["seller"];
  NumericVector amounts = df["amount"];

  IntegerVector indices = seq(0, df.nrows()-1);
  
  std::map<std::string, double> balanceMap;
  NumericVector tradeAmounts; // remember for mean trade vol
  for(int idx : indices) {
    double amount = amounts[idx];
    tradeAmounts.push_back(amount);
    
    std::string key = Rcpp::as<std::string>(buyers(idx));
    balanceMap[key] += amount;
    
    key = Rcpp::as<std::string>(sellers(idx));
    balanceMap[key] -= amount;
  }
  
  // take away one trade after the other; starting from last one
  for(int idx=df.nrows()-1; idx > 0; idx--) {
    // check if current set is wash
    NumericVector balances; // would be better to have the final size allocated already
    for(auto &ibalance: balanceMap)
      balances.push_back(ibalance.second);
    
    double mean_trade_vol = mean(tradeAmounts);
    balances = abs(balances / mean_trade_vol);
    LogicalVector log_balances = balances <= margin;
    
    if(is_true(all(log_balances))) {
      IntegerVector wash_indices = seq(0, idx);
      LogicalVector wash_labels = df["wash_label"]; // extract original wash labels
      wash_labels[wash_indices] = true; // set labels for wash trades to true
      df["wash_label"] = wash_labels; // replace wash labels
      return df;
    }
    
    // remove from balances
    double amount = amounts[idx];
    tradeAmounts.erase(idx);
    
    std::string key = Rcpp::as<std::string>(buyers(idx));
    balanceMap[key] -= amount;
    
    key = Rcpp::as<std::string>(sellers(idx));
    balanceMap[key] += amount;
  }
  
  return df;
}