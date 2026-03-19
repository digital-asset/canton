#!/bin/bash

# NUM_PARTICIPANT_PODS=5
# NUM_PARTICIPANTS_PER_POD=3
# NUM_SYNCHRONIZERS=3

REMOTE_PARTICIPANTS=""
REMOTE_MEDIATORS=""
REMOTE_SEQUENCERS=""

TARGET=$1

if [ -z "$TARGET" ]; then
    echo "Missing parameter for the target config file. Usage: <script> remote.conf"
    exit 1
fi

if [ -z "$NUM_PARTICIPANT_PODS" ]; then
    echo "Missing environment variable NUM_PARTICIPANT_PODS"
    exit 1
fi

if [ -z "$NUM_PARTICIPANTS_PER_POD" ]; then
    echo "Missing environment variable NUM_PARTICIPANTS_PER_POD"
    exit 1
fi

if [ -z "$NUM_SYNCHRONIZERS" ]; then
    echo "Missing environment variable NUM_SYNCHRONIZERS"
    exit 1
fi

for (( POD=1; POD<=$NUM_PARTICIPANT_PODS; POD++ )); do
    for (( PAR=1; PAR<=$NUM_PARTICIPANTS_PER_POD; PAR++ )); do
        REMOTE_PARTICIPANTS+="canton.remote-participants.participant${POD}_${PAR}.ledger-api.port=$((10000 + PAR*10 + 1))"$'\n'
        REMOTE_PARTICIPANTS+="canton.remote-participants.participant${POD}_${PAR}.ledger-api.address=participants$POD"$'\n'
        REMOTE_PARTICIPANTS+="canton.remote-participants.participant${POD}_${PAR}.admin-api.port=$((10000 + PAR*10 + 2))"$'\n'
        REMOTE_PARTICIPANTS+="canton.remote-participants.participant${POD}_${PAR}.admin-api.address=participants$POD"$'\n'
    done
done

for (( SYNC=1; SYNC<=$NUM_SYNCHRONIZERS; SYNC++ )); do
    REMOTE_MEDIATORS+="canton.remote-mediators.mediator${SYNC}.admin-api.port=$((10001))"$'\n'
    REMOTE_MEDIATORS+="canton.remote-mediators.mediator${SYNC}.admin-api.address=synchronizer$SYNC"$'\n'
    REMOTE_SEQUENCERS+="canton.remote-sequencers.sequencer${SYNC}.admin-api.port=$((10002))"$'\n'
    REMOTE_SEQUENCERS+="canton.remote-sequencers.sequencer${SYNC}.admin-api.address=synchronizer$SYNC"$'\n'
    REMOTE_SEQUENCERS+="canton.remote-sequencers.sequencer${SYNC}.public-api.port=$((10003))"$'\n'
    REMOTE_SEQUENCERS+="canton.remote-sequencers.sequencer${SYNC}.public-api.address=synchronizer$SYNC"$'\n'
done



echo "$REMOTE_PARTICIPANTS" > $TARGET
echo >> $TARGET
echo "$REMOTE_MEDIATORS" >> $TARGET
echo >> $TARGET
echo "$REMOTE_SEQUENCERS" >> $TARGET
