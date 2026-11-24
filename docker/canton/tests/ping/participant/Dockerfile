ARG BASE_IMAGE=canton-participant:local
FROM ${BASE_IMAGE}

COPY participant-bootstrap.sc /app/bootstrap.sc

# Example DAR (Iou template). PQS aborts without a user Daml model on the ledger.
COPY CantonExamples.dar /app/CantonExamples.dar
