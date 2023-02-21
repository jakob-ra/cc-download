FROM python:3.10.7

COPY cc-downloader .

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt


## to run cc-download.py locally:
# python cc-download.py --batch_size=100 --batches_per_partition=1 --output_bucket=cc-download-kieran --result_output_path=test

## to push the dockerfile to Amazon Elastic Container Registry:
# start docker daemon
# docker build -t public.ecr.aws/r9v1u7o6/cc-download-kieran:latest .
# aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws
# docker push public.ecr.aws/r9v1u7o6/cc-download-kieran:latest