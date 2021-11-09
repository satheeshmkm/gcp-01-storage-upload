FROM openjdk:11
# building from base image openjdk:11
ADD target/docker-spring-boot.jar gcp01.jar
# adding jar in the path target/docker-spring-boot.jar to docker image with name helloapp.jar
ENTRYPOINT ["java","-jar","/gcp01.jar","--server.port=8090"]
# Instruction to docker on how to run the docker file