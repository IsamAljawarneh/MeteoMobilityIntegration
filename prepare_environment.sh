PROGNAME=$0


usage() {
  cat << EOF >&2
Usage: $PROGNAME [-k <private_key_path>] [-u <ssh_user>] [-i <vm_ip_address>]

-k <private_key_path>: absolute path of the private key used to connect to the VM through SSH
-u <ssh_user>: user used to connect to the VM through SSH
-i <vm_ip_address>: public IP address of the VM

EOF
  exit 1
}

SSH_USER=azureuser

dir=default_dir file=default_file verbose_level=0
while getopts k:u:i: o; do
  case $o in
    (k) PRIVATE_KEY_PATH=$OPTARG;;
    (u) SSH_USER=$OPTARG;;
    (i) VM_IP_ADDRESS=$OPTARG;;
    (*) usage
  esac
done

export METEOMOBILITY_HOME=$(find $HOME -name MeteoMobilityIntegration | grep MeteoMobility)

if [ -z "$PRIVATE_KEY_PATH" ] || [ -z "$SSH_USER" ] || [ -z "$VM_IP_ADDRESS" ]
then
	echo "Not all parameters have been set, exiting the program"
	echo "PRIVATE_KEY_PATH: $PRIVATE_KEY_PATH"
	echo "SSH_USER: $SSH_USER"
	echo "VM_IP_ADDRESS: $VM_IP_ADDRESS"
	echo
	usage
fi

echo "Setting the right permissions to the private key (if not already done)"
chmod 600 "$PRIVATE_KEY_PATH"

echo "Copying the CSD API credentials to the VM user home (in local machine they sould be placed at ~/.cdsapirc)"
scp -i "$PRIVATE_KEY_PATH" ~/.cdsapirc azureuser@"$VM_IP_ADDRESS":~/.cdsapirc

echo "Copying the MeteoMobilityIntegration folder to the VM at path ~/MeteoMobilityIntegration"
scp -r -i "$PRIVATE_KEY_PATH" "$METEOMOBILITY_HOME" azureuser@"$VM_IP_ADDRESS":/home/azureuser 2> /dev/null

echo "Executing commands in ssh:"
echo "- Install the cdsapi module for python"
echo "- Install packages supporting eccodes"
echo "- Istall the eccodes package"

ssh -i "$PRIVATE_KEY_PATH" azureuser@"$VM_IP_ADDRESS" << EOF

# Install the CDS API fordownloading CAMS data with Python

pip install cdsapi

sudo killall apt apt-get
sudo rm /var/lib/apt/lists/lock
sudo rm /var/cache/apt/archives/lock
sudo rm /var/lib/dpkg/lock*

sudo dpkg --configure -a
sudo apt update

# Install the eccodes package to hread GRIB meteorological files and help transform them to CSV

sudo apt-get -y update && sudo apt-get -y install libnetcdf-dev libnetcdff-dev libopenjp2-7-dev gcc gfortran make unzip git cmake
sudo apt -y autoremove

echo "export ECCODES_DEFINITION_PATH=/usr/share/eccodes/definitions" >> .bashrc
echo "export GRIB_DEFINITION_PATH=/usr/share/eccodes/definitions" >> .bashrc && source .bashrc

sudo apt -y install libeccodes-tools
sudo dpkg --configure -a

EOF
if [ $? == 0 ]
then
	echo
	echo "Preparation of the environment on the VM at $VM_IP_ADDRESS complete"
	echo "Now you can connect to the VM by running:"
	echo
	echo "ssh -i \"$PRIVATE_KEY_PATH\" azureuser@\"$VM_IP_ADDRESS\""
	echo
else
	echo
	echo "Something went wrong with the SSH commands, exiting the program"
	echo
fi
