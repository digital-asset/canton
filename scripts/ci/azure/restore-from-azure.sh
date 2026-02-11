#!/usr/bin/env bash

echo "Checking AzCopy is installed"

if [ "$(azcopy)" ]; then
  echo "azcopy already installed"
else
  echo "AZcopy is not in path or is not installed. If using nix AzCopy must be in PATH. Exiting..."
  exit 1
fi
checksum()
{
	md5sum "${1}" | cut -f 1 -d ' '
}

ORB_KEY="$(eval echo "${ORB_KEY}")"
ORB_CONTAINER="$(circleci env subst "${CONTAINER_URL}")"
IFS=":" read -r -a array <<< "${ORB_KEY}" # Using : as field seperator when multiple keys are specified
TEMP_FILE_NAME="compressed_cache.tar.gz"
# Exit if AzCopy context (credentials) is not present
if [ -z "${ORB_CONTAINER}" ] ; then
  echo "AzCopy login/context is missing. Exiting..."
  exit 1
else
  echo "AzCopy context found"
fi

for key in "${array[@]}"; do
	TAR_NAME="$(echo "${key}"|xargs).tar.gz"  # Remove leading and trailing whitespace
	echo "Processing key: ${TAR_NAME}"
	azcopy list "${ORB_CONTAINER}"/"${TAR_NAME}" --log-level ERROR | grep -iqa "Content Length" && FILE_PRESENT_CHECK="Present" || FILE_PRESENT_CHECK=""
	if [ -z "${FILE_PRESENT_CHECK}" ] ; then
 		echo "${TAR_NAME} does not exist, continuing"
 	else 
 		echo "Copying file ${TAR_NAME}"
		azcopy copy "${ORB_CONTAINER}/${TAR_NAME}" "${TEMP_FILE_NAME}" --log-level ERROR --output-level essential
		tar xPvzf "${TEMP_FILE_NAME}" 
		rm "${TEMP_FILE_NAME}"  
		echo "Removed ${TAR_NAME} from local system"
		echo "Restore complete .... Exiting"
		echo "Copied file with $(azcopy list "${ORB_CONTAINER}"/"${TAR_NAME}" --mega-units --log-level ERROR | grep -ia "Content Length:" | grep -oP ";\K.*" )"

		exit 0
 	fi 
done
echo "Restore job complete"
