#!/bin/bash
# Update the system
sudo apt-get update -y
sudo apt-get upgrade -y

# Install Git
sudo apt-get install -y git

# Install Docker
sudo apt-get install -y docker.io
sudo systemctl start docker
sudo systemctl enable docker

# Install postgres
sudo apt-get install -y postgresql-client


# Add current user to the Docker group
sudo usermod -aG docker ubuntu

git clone https://github.com/UW-Climate-Risk-Lab/climate-risk-map-api.git /home/ubuntu/climate-risk-map-api