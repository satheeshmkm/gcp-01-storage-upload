# Approach 01:

* a REST end point to submit an XML file
* WHich will be converted to JSON (newline-delimited)
* Upload the JSOn directly to BigQueryTable

## Accessing application Swagger Document

http://localhost:8080/gcp-services/swagger-ui.html


# Steps for Creating and running a Docker application:
## 01) Create Executable jar
    java -jar gcp-01-storage-bigquery.jar
    http://localhost:8080/gcp-services/swagger-ui.html
    Running with port 8090
    java -jar gcp-01-storage-bigquery.jar --server.port=8090
## 02) Create Docker File
    FROM : Base Docker Image 
  in https://hub.docker.com
    
    ADD: Executable application to the Base docker image
    
    ENTRYPOINT: Command to be executed when you run the docker container
    
## 03) Build Docker Image
* Ensure docker is up :  
  *docker -v* / *docker --version*
* List current images :  
    *docker images*
* List running containers :  
  *docker container ls*
* Build the image :  
  *docker build -t dockergcppoc01 .*
  
    Build image with tag name dockersbootdemo from current folder .
  
    Note: Image name should be in lower case
* List the images :  
  *docker images*

## 04) Run Docker Container
* List running processes :  
  *docker ps*
* running a new process :  
  *docker run -p8081:8090 dockergcppoc01*
  
    System port 8081 is mapped to the port 8090 in docker container (Virtual machine)
    http://localhost:8081/gcp-services/swagger-ui.html

# Clean up Commands


- docker ps
- docker stop <ContainerID>
- docker rm <ContainerID>
- docker rmi -f <imageName>

# Steps for uploading the docker image to google cloud registry (GCR) 
## 01) Install Google Cloud SDK and in Environment, set the path to gcloud
## 02) Open a terminal and connect to your cloud account using following command
  *gcloud auth login*
	Google login screen to enter google account and password will be displayed. Login with your cloud credentials.
	You will get logged in to your default project
## 03) Tag your docker image (tag name dockergcppoc01) with GCP projectID (macysproject)
  *docker tag dockergcppoc01 gcr.io/macysproject/dockergcppoc01*
## 04) Enable Container Registry API in the project ( CI/CD ->Container Registry)
## 05) Configure docker for saving credentials in %USERPROFILE%/.docker/config.json diectory
  *gcloud auth configure-docker*
## 05) Push the image tag to Container Registry
  *docker push gcr.io/macysproject/dockergcppoc01*
  

Continue : https://www.youtube.com/watch?v=V3jAGbTwS-M
  
