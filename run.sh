#!/bin/bash

# Prep work, adding config file
mkdir -p ~/.lsst
cd /opt/lsst/software/server
cp ./db-auth.yaml ~/.lsst/

# Clearing out the /tmp/data/u folder
rm -r /tmp/data/

# Assigning correct permissions and group
cd ~/
chmod -R 600 ./.lsst
chown -R lsst:lsst ./.lsst

# Changing to the user the LSST stack expects to operate with
# su lsst

# Return to server dir for env setup
cd /opt/lsst/software/server

# Pull path bins
source /opt/lsst/software/stack/loadLSST.bash

# Create virtual environment
# echo "installing PEP 517"
# pip3 install pep517

echo "establishing virtual env ~ venv"
python3 -m venv /tmp/venv
source /tmp/venv/bin/activate

echo "upgrading pip"
pip3 install --upgrade pip setuptools wheel PEP517 p5py

# Install sphgeom
echo "installing the sphgeom"
pip3 install git+https://github.com/lsst/sphgeom@main#egg=lsst_sphgeom

# Install butler dependency
echo "installing dependencies for butler"
pip3 install pydantic botocore boto3 psycopg2-binary

# Install daf_butler
echo "installing the butler"
pip3 install git+https://github.com/lsst/daf_butler@main#egg=daf_butler

# Retrieve artifacts
mkdir -p /tmp/data
echo "about to execute the butler command"
butler --long-log retrieve-artifacts --collections=$2 s3://butler-config /tmp/data

echo "about to print content of /tmp/data"

for entry in /tmp/data/*
do
  echo "$entry"
done
