FROM python:3.10.7

COPY cc-downloader .

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

## to run cc-download.py locally:
# python cc-download.py --batch_size=10 --batches_per_partition=1 --output_bucket=cc-download-test --result_output_path=test --page_processing_func_path=https://www.dropbox.com/s/7jaq0w5e8t8jxot/process_page.py?dl=1

## to push the dockerfile to Amazon Elastic Container Registry:
# start docker daemon
# docker build -t public.ecr.aws/r9v1u7o6/cc-download:latest .
# aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws
# docker push public.ecr.aws/r9v1u7o6/cc-download:latest