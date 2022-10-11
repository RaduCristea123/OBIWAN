cd obiwan-main
python setup.py develop
py -3 -m pip install -e .
cd ..\SCC-access
python setup.py develop
cd ..\lidar\lidarchive-main
python setup.py develop
py -3 -m pip install -e .