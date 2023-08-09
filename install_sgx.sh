#!/bin/sh

#needed to install SGX PSW (for using the hardware)
sudo apt-get install libssl-dev libcurl4-openssl-dev libprotobuf-dev -y

#needed to install SGX SDK

sudo apt-get install build-essential python -y

mkdir -p ~/install_sgx
cd install_sgx

#download sgx driver, sdk and psw
wget https://download.01.org/intel-sgx/sgx-linux/2.8/distro/ubuntu18.04-server/sgx_linux_x64_sdk_2.8.100.3.bin
wget https://download.01.org/intel-sgx/sgx-linux/2.8/distro/ubuntu18.04-server/sgx_linux_x64_driver_2.6.0_51c4821.bin

#add permissions
chmod u+x sgx_linux_x64_driver_2.6.0_51c4821.bin
chmod u+x sgx_linux_x64_sdk_2.8.100.3.bin


#install driver
sudo ./sgx_linux_x64_driver_2.6.0_51c4821.bin


echo 'deb [arch=amd64] https://download.01.org/intel-sgx/sgx_repo/ubuntu bionic main' | sudo tee /etc/apt/sources.list.d/intel-sgx.list
wget -qO - https://download.01.org/intel-sgx/sgx_repo/ubuntu/intel-sgx-deb.key | sudo apt-key add -
sudo apt-get update

#depending what we want to use we can do the installation costumised
sudo apt-get install libsgx-launch libsgx-urts -y
sudo apt-get install libsgx-epid libsgx-urts -y
sudo apt-get install libsgx-quote-ex libsgx-urts -y

#install sdk
echo -e "no\n /opt/intel" | sudo ./sgx_linux_x64_sdk_2.8.100.3.bin
#will be prompted a question where to install the sgxsdk
#should answer /opt/intel
source /opt/intel/sgxsdk/environment


#there are default libraries to where both sdk and psw are installed
#psw goes to /usr/lib ....