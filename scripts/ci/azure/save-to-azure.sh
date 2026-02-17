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

ORB_PATH="$(eval echo "${ORB_PATH}")"
ORB_KEY="$(eval echo "${ORB_KEY}")"
ORB_CONTAINER="$(circleci env subst "${CONTAINER_URL}")"
TEMP_FILE_NAME="compressed_cache.tar.gz"
AZURE_TAR_NAME="${ORB_KEY}.tar.gz"
IFS=":" read -r -a array <<< "${ORB_PATH}" # Using : as field seperator when multiple paths are passed

# Exit if AzCopy context (credentials) is not present
if [ -z "${ORB_CONTAINER}" ] ; then
  echo "AzCopy login/context is missing. Exiting..."
  exit 1
else
  echo "AzCopy context found"
fi

echo "Saving to KEY: ${AZURE_TAR_NAME}"
azcopy list "${ORB_CONTAINER}"/"${AZURE_TAR_NAME}" --log-level ERROR | grep -iqa "Content Length" && FILE_PRESENT_CHECK="Present" || FILE_PRESENT_CHECK=""
echo "${FILE_PRESENT_CHECK}"
if [ -z "${FILE_PRESENT_CHECK}" ] ; then 
	echo "Compressing"
	tar cPzf "${TEMP_FILE_NAME}" "${array[@]}" --ignore-failed-read
	echo "Copying"
	set +e
	azcopy copy "${TEMP_FILE_NAME}" "${ORB_CONTAINER}/${AZURE_TAR_NAME}"  --overwrite=false --output-level essential --log-level ERROR || (echo "Save to cache could have filed please check logs" && true)
	rm "${TEMP_FILE_NAME}"
	echo "Saved to KEY: ${AZURE_TAR_NAME} with $(azcopy list "${ORB_CONTAINER}"/"${AZURE_TAR_NAME}" --mega-units --log-level ERROR | grep -ia " Content Length" | grep -oP ";\K.*" )"
	set -e
else
	echo "WARNING: Cache ${AZURE_TAR_NAME} already exists, Skipping ..."
	exit 0
fi



