# syntax=docker/dockerfile:1.7
# JWKS server plus a pre-minted audience-based token for the smoke stack.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
RUN pip install --no-cache-dir "PyJWT==2.9.0" "cryptography==43.0.1"

COPY serve.py /app/serve.py

EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=3s --retries=10 \
  CMD python3 -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8080/health').status==200 else 1)"

CMD ["python3", "/app/serve.py"]
