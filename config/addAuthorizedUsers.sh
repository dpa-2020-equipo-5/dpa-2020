#!/bin/bash

BASTION_IP=$(cat bastion_data.json | jq -r '.elastic_ip')
PRIVATE_EC2_IP=$(cat bastion_data.json | jq -r '.private_ec2_ip')

###############################################################################
# Creating users in the bastion EC2 instance.
ssh -i "key_dohmh_nyc.pem" ubuntu@$BASTION_IP << EOF
sudo adduser ely --gecos " , , , " --disabled-password
echo "ely:ely" | sudo chpasswd
sudo usermod -aG sudo ely

sudo adduser eddie --gecos " , , , " --disabled-password
echo "eddie:eddie" | sudo chpasswd
sudo usermod -aG sudo eddie

sudo adduser karla --gecos " , , , " --disabled-password
echo "karla:karla" | sudo chpasswd
sudo usermod -aG sudo karla

sudo adduser leo --gecos " , , , " --disabled-password
echo "leo:leo" | sudo chpasswd
sudo usermod -aG sudo leo

sudo adduser mathus --gecos " , , , " --disabled-password
echo "mathus:mathus" | sudo chpasswd
sudo usermod -aG sudo mathus

sudo adduser liliana --gecos " , , , " --disabled-password
echo "liliana:liliana" | sudo chpasswd
sudo usermod -aG sudo liliana

sudo sed -i 's|#PubkeyAuthentication yes|PubkeyAuthentication yes|g' /etc/ssh/sshd_config
sudo sed -i 's|PasswordAuthentication no|PasswordAuthentication yes|g' /etc/ssh/sshd_config

sudo service sshd restart
EOF

###############################################################################
# Copying pem files for each user previously created in the bastion.
ssh-copy-id -f -i './authorized_keys/public_key_ely.pub' ely@$BASTION_IP
ssh-copy-id -f -i './authorized_keys/public_key_eddie.pub' eddie@$BASTION_IP
ssh-copy-id -f -i './authorized_keys/public_key_karla.pub' karla@$BASTION_IP
ssh-copy-id -f -i './authorized_keys/public_key_leo.pub' leo@$BASTION_IP
ssh-copy-id -f -i './authorized_keys/public_key_mathus.pub' mathus@$BASTION_IP
ssh-copy-id -f -i './authorized_keys/public_key_liliana.pub' liliana@$BASTION_IP

###############################################################################
# Setting bastion permission to only allow PubkeyAuthentication.
ssh -i "key_dohmh_nyc.pem" ubuntu@$BASTION_IP << EOF
sudo sed -i 's|PasswordAuthentication yes|PasswordAuthentication no|g' /etc/ssh/sshd_config
sudo service sshd restart
EOF

###############################################################################
# Copying the pem file into the bastion so it can later log in into the private EC2 instance.
scp -i ./key_dohmh_nyc.pem key_dohmh_nyc.pem ubuntu@$BASTION_IP:/home/ubuntu/key_dohmh_nyc.pem

###############################################################################
# Installing required software in the private EC2 instance.
ssh -t -t -i key_dohmh_nyc.pem ubuntu@$BASTION_IP "ssh -t -t -i key_dohmh_nyc.pem ubuntu@$PRIVATE_EC2_IP sudo apt-get update"
ssh -t -t -i key_dohmh_nyc.pem ubuntu@$BASTION_IP "ssh -t -t -i key_dohmh_nyc.pem ubuntu@$PRIVATE_EC2_IP sudo apt update"
ssh -t -t -i key_dohmh_nyc.pem ubuntu@$BASTION_IP "ssh -t -t -i key_dohmh_nyc.pem ubuntu@$PRIVATE_EC2_IP sudo apt install postgresql-client-common"
ssh -t -t -i key_dohmh_nyc.pem ubuntu@$BASTION_IP "ssh -t -t -i key_dohmh_nyc.pem ubuntu@$PRIVATE_EC2_IP sudo apt-get install postgresql-client"
ssh -t -t -i key_dohmh_nyc.pem ubuntu@$BASTION_IP "ssh -t -t -i key_dohmh_nyc.pem ubuntu@$PRIVATE_EC2_IP sudo apt install git"
ssh -t -t -i key_dohmh_nyc.pem ubuntu@$BASTION_IP "ssh -t -t -i key_dohmh_nyc.pem ubuntu@$PRIVATE_EC2_IP sudo apt install python-pip"
ssh -t -t -i key_dohmh_nyc.pem ubuntu@$BASTION_IP "ssh -t -t -i key_dohmh_nyc.pem ubuntu@$PRIVATE_EC2_IP pip install luigi"
ssh -t -t -i key_dohmh_nyc.pem ubuntu@$BASTION_IP "ssh -t -t -i key_dohmh_nyc.pem ubuntu@$PRIVATE_EC2_IP git clone https://github.com/dpa-2020-equipo-5/dpa-2020.git /home/ubuntu/dpa-2020/ "
ssh -t -t -i key_dohmh_nyc.pem ubuntu@$BASTION_IP "ssh -t -t -i key_dohmh_nyc.pem ubuntu@$PRIVATE_EC2_IP git clone https://github.com/dpa-2020-equipo-5/nyc-ccci-etl.git /home/ubuntu/nyc-ccci-etl/ "

#psql -h hostname -p portNumber -U userName dbName -W
#psql -h dohmhnyc.cov4opzncr57.us-east-2.rds.amazonaws.com -p 5432 -U dohmh_nyc db_dohmh_nyc -W
