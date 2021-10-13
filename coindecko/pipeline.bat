echo off
title Pull and clean CoinGecko data
:: Runs all py files to clean and organize data files

echo NOTICE: data storage requires about 7 gigs on local drive. 
echo Please kill script if insufficient storage available.


python pyscript/ingest_data.py

echo .
echo Querying coindecko API ...
echo Please be patient :)
echo Raw data imported successfully! 
echo Available in data/raw/
echo .

python pyscript/clean_process_data.py

echo .
echo Data processing complete! :)
echo Available in data/processed
echo .

echo Creating MySQL Storage and uploading...

python pyscript/load_into_sql.py

echo .
echo SQL storage successful!
echo .
echo ALL DONE! Have a good day :)