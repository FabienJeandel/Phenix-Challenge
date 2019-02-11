# phenix-challenge

To run this program, you have to precise two arguments which correspond to the directory of your data 
& the day o you want calculate KPI.  

Notice : Rolling Indicators are calculated on this date and the seven days before.

Example : Date choosen is 20190210 so the period of this program became 20190203 to 20190210.

Program arguments are :
- Date with the following format : yyyymmdd
- Directory of sources files with data folder filled properly : 
  - data/transactions 
  - data/articles
  - data/intermediateResults
  - data/results

We write intermediate files to improve the performances. you can find thses files in the corresponding folder for each distinct stores & dates treated,the files are also split by the two indicators wanted : CA and Sales.

The results files can be available in "results" folder. 

Thank you
