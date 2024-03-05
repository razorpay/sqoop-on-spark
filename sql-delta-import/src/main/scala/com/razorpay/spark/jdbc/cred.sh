yes | apt install python3-pip

echo "Install specific versions of botocore, s3transfer, and awscli"
pip3 install botocore==1.31.4 s3transfer==0.6.0 awscli==1.29.4

echo "Install credstash"
pip3 install credstash