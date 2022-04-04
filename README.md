# news_chrysalis_segmentation #

Segmenting users to BBC News app based on their preference for different article topics.

## Overview

- **Project title:** news_chrysalis_segmentation
- **Inception Date:** April 2022
- **Main Stakeholder department(s):** BBC News
- **Main Stakeholder(s):** holly_silk, jana_eggink
- **Author(s):** vicky_banks

## Details
  
#### How was the project initiated?

__..Please complete..__

#### What was the goal of the project

__..Please complete..__

#### Outputs

__..Add any more info here if required..__

See `vignettes/`

## Usage 

### Setup

The R ecosystem is slightly different to that of Python and the approach to virtual environments is different. On 2019-08-02 an introduction to [renv](https://rstudio.github.io/renv/articles/renv.html) was created by the folks at RStudio. For the purposes of this template, it's recommended that we also use `renv`.

To learn more about how `renv` works see [here.](https://rstudio.github.io/renv/articles/renv.html#workflow)

To install the latest `renv` install the following in your system-level version of R. 

Note: __check that R will use the proxy__ if on the BBC network. 

```R
file.edit('~/.Renviron') # in Rstudio, or in any text editor of your choice
```

then add to the file and save / write out:

```R
https_proxy=http://www-cache.reith.bbc.co.uk:80
http_proxy=http://www-cache.reith.bbc.co.uk:80
```

Now you should be able to do the following:

__..If anything more than R package install required, use docker..__ 

1. Install docker desktop - https://www.docker.com/products/docker-desktop
2. Run `make docker` - this will build and then run the container, will take a while the first time
3. Instructions to access via a terminal or web interface will be printed to the console 
Note: see thoughts / issues on the use of Docker to manage dependencies [here](https://rstudio.github.io/renv/articles/docker.html). For more general information on R environments and Docker see [here.](https://environments.rstudio.com/docker.html)

__..Otherwise..__

__Option A: Using `make` (the easy way):__

1. (Optional) initialise a renv for the project and activate: 

```
make create_environment
```

2. To make sure your environment is stored run `make package` before checking in


3. There are other things you can easily run using `make`:

Run `make help` to remind yourself:

```
> make help:

clean               Delete all temp files (including environment) 
create_environment  Set up R interpreter environment 
data                Make Dataset 
docker              Run docker container, will build it first if required 
docker_stop         Stop the running container 
make shiny          Build and run shiny app 
package             Package dependencies to lockfile 
sync_data_from_s3   Download Data from S3 
sync_data_to_s3     Upload Data to S3 
test                Run tests inside renv 
update_environment  Sync Dependencies with Lockfile 
```

### Tests

Run the unit tests using `make test`

### Data

__..If you are making use of s3 to back up data..__

Data is stored here:
s3://map-input-output/news_chrysalis_segmentation

To get data:
1. Set up AWS cli on your machine, download the cli from AWS and then run something like the below on an EC2 box to generate some credentials to use:
    ```
    instanceid=$( \
        curl -s 'http://169.254.169.254/latest/meta-data/iam/security-credentials/'
    )
    curl "http://169.254.169.254/latest/meta-data/iam/security-credentials/${instanceid}"
    ```
2. Once access is granted you can run `make sync_data_from_s3` to download data to use

### Running

__..Include instructions on how to run your program, or get up and running with your package.__ 

### Results

__..Any more info on visualisations or where results might be saved. Under the package structure currently in use, we suggest using vignettes.__
