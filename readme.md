Arbitrator aggregator

deploy a virtual environment
```
python3 -m venv .env
source .env/bin/activate
pip install
```

# Wheels
<!-- declare -a wheels=("https://files.pythonhosted.org/packages/07/08/a549ba8b061005bb629b76adc000f3caaaf881028b963c2e18f811c6edc1/numpy-1.18.2-cp36-cp36m-manylinux1_x86_64.whl" \
"https://files.pythonhosted.org/packages/e7/f9/f0b53f88060247251bf481fa6ea62cd0d25bf1b11a87888e53ce5b7c8ad2/pytz-2019.3-py2.py3-none-any.whl" \
"https://files.pythonhosted.org/packages/bb/71/8f53bdbcbc67c912b888b40def255767e475402e9df64050019149b1a943/pandas-1.0.3-cp36-cp36m-manylinux1_x86_64.whl" \
"https://files.pythonhosted.org/packages/1a/70/1935c770cb3be6e3a8b78ced23d7e0f3b187f5cbfab4749523ed65d7c9b1/requests-2.23.0-py2.py3-none-any.whl" \
"https://files.pythonhosted.org/packages/bc/a9/01ffebfb562e4274b6487b4bb1ddec7ca55ec7510b22e4c51f14098443b8/chardet-3.0.4-py2.py3-none-any.whl" \
"https://files.pythonhosted.org/packages/b9/63/df50cac98ea0d5b006c55a399c3bf1db9da7b5a24de7890bc9cfd5dd9e99/certifi-2019.11.28-py2.py3-none-any.whl" \
"https://files.pythonhosted.org/packages/e8/74/6e4f91745020f967d09332bb2b8b9b10090957334692eb88ea4afe91b77f/urllib3-1.25.8-py2.py3-none-any.whl" \
"https://files.pythonhosted.org/packages/89/e3/afebe61c546d18fb1709a61bee788254b40e736cff7271c7de5de2dc4128/idna-2.9-py2.py3-none-any.whl" \
"https://files.pythonhosted.org/packages/28/5c/cf6a2b65a321c4a209efcdf64c2689efae2cb62661f8f6f4bb28547cf1bf/joblib-0.14.1-py2.py3-none-any.whl") -->

declare -a wheels=("https://files.pythonhosted.org/packages/07/08/a549ba8b061005bb629b76adc000f3caaaf881028b963c2e18f811c6edc1/numpy-1.18.2-cp36-cp36m-manylinux1_x86_64.whl" \
"https://files.pythonhosted.org/packages/e7/f9/f0b53f88060247251bf481fa6ea62cd0d25bf1b11a87888e53ce5b7c8ad2/pytz-2019.3-py2.py3-none-any.whl" \
"https://files.pythonhosted.org/packages/bb/71/8f53bdbcbc67c912b888b40def255767e475402e9df64050019149b1a943/pandas-1.0.3-cp36-cp36m-manylinux1_x86_64.whl")


# download layer deps
for w in ${wheels[@]}; do
    wget -nc $w -P layers/wheels/
done


# unzip contents
mkdir -p layers/aggregator_common/python/lib/python3.6/site-packages/
for wheel in layers/wheels/*; do
    unzip -o $wheel -d layers/aggregator_common/python/lib/python3.6/site-packages/
done

# Zip up the layer contents
# mkdir -p layers/zips

rm layers/zips/arbitrator_aggregator_common.zip
pushd layers/aggregator_common
zip -o -r -p ../zips/arbitrator_aggregator_common.zip python
popd

# push the layer
# source awsmfa xxxxx 
aws s3 sync layers/zips/ s3://arbitrator-store/lambda/layers/arbitrator_aggregator/ --delete

# Build/update layer absaoss-fraud-py36-np-pd-tfl
aws lambda publish-layer-version \
--layer-name arbitrator-aggregator-common \
--description "Python 3.6.9 layer: pydeps and collector_functions" \
--content S3Bucket=arbitrator-store,S3Key=lambda/layers/arbitrator_aggregator/arbitrator_aggregator_common.zip \
--compatible-runtimes python3.6
