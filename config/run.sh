#!/bin/bash

rm -rf key_dohmh_nyc.pem

python createKeyPair.py

chmod 400 key_dohmh_nyc.pem

python createAWSResources.py

./addAuthorizedUsers.sh

echo 'Done :)'
