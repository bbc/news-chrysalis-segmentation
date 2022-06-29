# g1
<p align="left">
    <a href="https://insights-jupyterhub.tools.bbc.co.uk/jh/hub/user-redirect/git-pull?repo=https%3A%2F%2Fgithub.com%2Fbbc%2Fg1&urlpath=lab%2Ftree%2Fg1%2FREADME.md">
    	<img src="https://img.shields.io/static/v1?label=&message=open%20on%20ccog%20platform&color=505050&colorA=505050&logo=jupyter&style=for-the-badge">
    </a>
</p>

A short description of the project.

## Overview

- **Project title:** g1
- **Inception Date:** Month + Year when project started (e.g. May 2019)
- **Main Stakeholder department(s):** A list of interested departments (e.g. sports/childrens/etc)
- **Main Stakeholder(s):** A list of interested parties
- **Author(s):** Your name (or your organization/company/team)

## Details
  
#### How was the project initiated?

__..Please complete..__

#### What was the goal of the project

__..Please complete..__

#### Outputs

__..Add any more info here if required..__

See `reports/`

## Usage 

### Setup 

__..If anything more than pip install required, use docker..__ 

Advised to use docker:
1. Install docker desktop - https://www.docker.com/products/docker-desktop
2. Run `./start.sh` - this will build and then run the container, will take a while the first time
3. Access via a terminal or jupyter 
    - Option A (recommended): Use kinematic (comes with docker) to see details about which docker containers are running, from here either launch a terminal or launch the browser to use jupyter
    - Option B: Run `docker ps` to view running containers, 
        - The port jupyter is running on will be shown. 
        - To open a termainal inside the container run `docker exec -it g1 /bin/bash`

Any additional dependencies outside of pip you need, install them in the DOCKERFILE.

__..Otherwise..__

1. Initialise and setup the virtualenv for the project and activate: 

	```
	make create_environment
	```
	Run the command printed to then activate for your os
	
	This will also install an ipykernel you can select from jupyter 
	
	**The name of the kernel will be the same as your repository name

2. If you need to install any additional dependencies simply add the to the `requirements.txt` file and run `make requirements`

	**If you are working in a jupyter notebook you will need to restart the kernel to see the changes

3. To make sure your environment is stored run `make package` before checking in (this will pick up anything you installed just using pip directly)

### Tests

Run the unit tests using `make test`

### Data

__..If you are making use of s3 to back up data..__

Data is stored here:
s3://map-input-output/g1

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

__..Include instructions on how to run your program, ideally just `python main.py` or otherwise steps..__

### Results

__..Any more info on visualisations or where results might be saved..__
