#!/usr/bin/env bash

checksum()
{
	md5sum "${1}" | cut -f 1 -d ' '
}

ORB_PATH="$(eval echo "${ORB_PATH}")"
ORB_KEY="$(eval echo "${ORB_KEY}")"
ORB_CONTAINER="$(circleci env subst "${ARTIFACT_CONTAINER_URL}")"
TEMP_FILE_NAME="compressed_cache.tar.gz"
AZURE_TAR_NAME="${ORB_KEY}/${ORB_PATH}.tar.gz"
#IFS=":" read -r -a array <<< "${ORB_PATH}" # Using : as field seperator
IFS=":"

echo "Saving to KEY: ${AZURE_TAR_NAME}"
azcopy list "${ORB_CONTAINER}"/"${AZURE_TAR_NAME}" | grep -iqa "Content Length" && FILE_PRESENT_CHECK="Present" || FILE_PRESENT_CHECK=""
echo "${FILE_PRESENT_CHECK}"
if [ -z "${FILE_PRESENT_CHECK}" ] ; then
	echo "Compressing"
	tar cPvzf "${TEMP_FILE_NAME}" "${ORB_PATH}" --ignore-failed-read
	echo "Copying"
	set +e
	azcopy copy "${TEMP_FILE_NAME}" "${ORB_CONTAINER}/${AZURE_TAR_NAME}" --recursive --overwrite=false || (echo "Save to cache could have filed please check logs" && true)
	rm "${TEMP_FILE_NAME}"
	set -e
else
	echo "WARNING: Cache ${AZURE_TAR_NAME} already exists, Skipping ..."
	azcopy list "${ORB_CONTAINER}"/"${AZURE_TAR_NAME}"
	exit 0

fi
