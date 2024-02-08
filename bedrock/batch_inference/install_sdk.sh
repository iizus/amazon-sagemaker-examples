SDK=sdk
ZIP=$SDK.zip
wget --no-check-certificate -O $ZIP https://d2eo22ngex1n9g.cloudfront.net/Documentation/SDK/bedrock-python-sdk-reinvent.zip

unzip -n -d $SDK $ZIP
find $SDK -type f -name boto*.whl

pip install $SDK/botocore-1.32.4-py3-none-any.whl
pip install $SDK/boto3-1.29.4-py3-none-any.whl