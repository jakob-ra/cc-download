<a name="readme-top"></a>

# CommonCrawl Downloader

<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#purpose">Purpose</a>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#aws">AWS Permissions & Authentication</a></li>
        <li><a href="#bucket">Creating a bucket for the project</a></li>
        <li><a href="#url-list">Uploading URL list</a></li>
        <li><a href="#supbage-selection">Choosing which subpages to download for each domain</a></li>
        <li><a href="#keywords">Providing keyword and URL keyword lists</a></li>
        <li><a href="#crawls">Choosing a time frame (which crawls are searched)</a></li>
        <li><a href="#run">How to run</a></li>
      </ul>
    </li>
    <li><a href="#costs">Estimated costs</a></li>
    <li><a href="#runtime">Estimated runtime</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## Purpose

[CommonCrawl](https://commoncrawl.org/the-data/get-started/) is a nonprofit organization that crawls the web and freely
provides its archives and datasets to the public. The Common Crawl corpus contains petabytes of data collected since 2013. It contains monthly updates of raw web page data that are hosted as WARC files on Amazon Web Services' (AWS) S3
storage servers located in the US-East-1 (Northern Virginia) AWS Region.

This script was written with the purpose of downloading and processing the raw web page data for a user-provided list of
domain names (e.g. apple.com, walmart.com, microsoft.com). We first get the byte range within the WARC file where a
specific subpage is stored by querying the CommonCrawl Index via Athena. Using the byte range, the raw html of a webpage
is downloaded and processed according to a user-provided processing function. The results, along with information about
their source are uploaded in batches of csv files to S3. The output files can then be downloaded or further processed.
The output files have the following format:

![image](https://user-images.githubusercontent.com/49194118/199245335-a00f27ad-01e4-470b-8a06-4f06a8efd4cb.png)

By using Fargate spot instances the processing is cheap (in case a task is interrupted, it is just re-attempted). Also
transfer speed is maximal because we access the CommonCrawl data from the same AWS region where it is hosted.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- Getting Started -->
## Getting Started

### AWS Permissions & Authentication

To run the script, you first need to make an [AWS account](https://aws.amazon.com/). You then need
to [create](https://us-east-1.console.aws.amazon.com/iamv2/home) an IAM-User in the US-East-1 region and add the
following permissions:

- AmazonAthenaFullAccess
- AWSBatchFullAccess
- IAMFullAccess
- AmazonS3FullAccess

After you created the user and added all permission, click on > Security credentials > create access key > Download .csv
file:

![image](https://user-images.githubusercontent.com/49194118/199265023-4df68721-41fd-49d2-bf91-9335330779c2.png)

Provide the path to the credential file that you just downloaded under config.yml > credentials_csv_filepath.

You also need to [create](https://us-east-1.console.aws.amazon.com/iamv2/home#/roles) a role (select trusted entity type = AWS account) and add the following
permissions:

- AmazonS3FullAccess
- CloudWatchFullAccess
- AmazonAthenaFullAccess
- AmazonElasticContainerRegistryPublicReadOnly

Then go to trust relationships and replace whatever is there with

{
    "Version": "2008-10-17",
    "Statement": [
        {
            "Sid": "",
            "Effect": "Allow",
            "Principal": {
                "Service": "ecs-tasks.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}

Once you created the role, added all permissions, and updated the trust policy, click on the role and click the copy symbol next to the role's ARN:

![image](https://user-images.githubusercontent.com/49194118/199257495-1abe5be3-ed21-45c9-bdd3-9566a0169838.png)

Provide the role's ARN that you just copied under config.yml > batch_role.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

### Creating a bucket for the project

Navigate to the [S3 console](https://s3.console.aws.amazon.com/s3/buckets?region=us-east-1) and create a bucket for your
project in the US-East-1 region. The bucket can be private or public.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

### Uploading URL list

In the bucket you created, create a folder. In it, upload the list of domain names that you are interested in as one or multiple csv files. *Warning:* URLs should not contain "https" or "www." upfront, just the domain name (e.g. apple.com). The URLs
should not be contained within quotes (apple.com, not "apple.com"). 

Provide the full S3 path of the *folder* (not the
file itself), e.g. 's3://cc-extract/url_list/', under config.yml >
s3path_url_list. There should be no other files in the folder except the URL list.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

### Choosing which subpages to download for each domain

There are often many hundred subpages per domain and downloading and processing all of them might be too costly. You
have two ways of selecting a subset of subpages per domain:

- Downloading the n_subpages subpages with the shortest URLs. For instance, if n_subpages is 2 and the crawled subpages
  have the URLs 
  
  > 'apple.com/store', 'apple.com/contact', 'apple.com/iphone-14/switch/
  
  then only 
  
  > 'apple.com/store', 'apple.com/contact' 
  
  will be downloaded and processed.
- Selecting subpages that contain at least one of a list of keywords in their website address. For example, if "covid"
  is in the url_keywords list, the subpage with the URL:'apple.com/covid-19' will be downloaded. However, you might
  still get too many subpages containing such keywords, in which case you can limit the number of such subpages to
  download per domain by specifying the config.yml > limit_pages_url_keywords parameter. You can use both ways of
  filtering, just one, or neither (by setting the n_subpages and url_keywords_path parameters to None).

<p align="right">(<a href="#readme-top">back to top</a>)</p>

### Providing a custom processing function

If you want to process the downloaded subpages in a custom way, you can alter process_page.py to meet your requirements.
The file is uploaded to s3 to have it available to the containers running each job.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

### Choosing a time frame (which crawls are searched)

[This](https://commoncrawl.org/the-data/get-started/) is a list of all available crawls on CommonCrawl. Using this
downloader you can acess all crawls in WARC format, i.e. anything after and including the CC-MAIN-2013-20 crawl (the
format is CRAWL-NAME-YYYY-WW – The name of the crawl and year + week it was initiated). You can specify one or multiple
crawls you want to search under config.yml > crawls. *Warning:* Searching many crawls can quickly lead to considerable
costs, see estimated costs.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

### How to run

- To clone the repo:
  ```sh
  git clone https://github.com/jakob-ra/cc-download.git
  ```
  
- Install requirements:
   ```sh
   pip install -r requirements.txt
   ```
   
- Configure all input parameters using config.yml

- Run:
   ```sh
   python main.py
   ```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Estimated costs
The costs of querying Athena for each crawl you want to search are relatively fixed because even when providing a small URL list, the entire CommonCrawl index file has to be scanned for the inner join operation. These costs will be around 0.30$-0.50$ per crawl, independent of the number of URLs you provide.

The costs of downloading and processing the raw webpages depend on the number of URLs you provide and the
number of subpages you want to download for each URL. The cost is around 0.33$ per million subpages.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Estimated runtime

Querying the CommonCrawl index usually takes around 2 minutes per Crawl. The download and processing of 10 million subpages takes around 10 minutes. For fewer subpages, you can speed up the process by reducing config.yml > batch_size.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Checking status

As soon as the batch job is submitted, you can check its status in the [AWS Batch console](console.aws.amazon.com/batch). By clicking on the job and selecting 'child jobs', you can see the status of each child jobs (the number of which is the number of subpages to download divided by your batch size). To debug, you can also check the logs of each child job in the [CloudWatch console](https://console.aws.amazon.com/cloudwatch) or by clicking on the child job and selecting 'logging'. A container could also fail because it ran out of memory. You would see this by selecting 'JSON' in the child job details, there should an exit code and a reason. In case of memory issues, you can increase the memory allocation to each container in config.yml > memory.

Each successfully ran job will create an output file in the S3 bucket you specified under config.yml > s3path_output. You can check and download your output files by navigating to the [S3 console](https://s3.console.aws.amazon.com) and selecting your output bucket and folder.

<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE.txt` for more information.

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- CONTACT -->
## Contact

Jakob Rauch - j.m.rauch@vu.nl

Project Link: [https://github.com/jakob-ra/cc-download](https://github.com/jakob-ra/cc-downloader)

<p align="right">(<a href="#readme-top">back to top</a>)</p>