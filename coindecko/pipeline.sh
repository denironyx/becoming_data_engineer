echo off
title Pull and clean Chicago crime data
:: Runs all py files to clean and organize data files

echo NOTICE: data storage requires about 7 gigs on local drive. 
echo Please kill script if insufficient storage available.


python pyscript/pull_raw_data.py

echo .
echo .
echo .
echo Querying coindecko API ...
echo Please be patient :)
echo Raw data imported successfully! 
echo Available in data/raw/
echo .
echo .
echo .

python pyscript/process_data.py

echo .
echo Data processing complete! :)
echo Available in data/processed
echo .
echo .

echo Creating MySQL Storage and uploading...

python pyscript/coindeck_db.py

echo .
echo .
echo .
echo SQL storage successful!
echo .
echo .
echo .
echo ALL DONE! Have a good day :)