FROM iflavoursbv/sbt-openjdk-8-alpine
COPY . /app/
ARG NEXUS_USERNAME=$USER
ARG NEXUS_PASSWORD=
ENV NEXUS_USERNAME=$NEXUS_USERNAME
ENV NEXUS_PASSWORD=$NEXUS_PASSWORD
CMD ["apt-get update" "-yqq"]
CMD ["apt-get install" "-yqq" "apt-transport-https" "ca-certificates"]
CMD ["apt-key" "adv" "--keyserver" "hkp://keyserver.ubuntu.com:80" "--recv" "2EE0EA64E40A89B84B2DF73499E82A75642AC823"]
CMD ["apt-get update -yqq"]
CMD ["apt-get" "install" "-yqq" "sbt"]
CMD ["sbt", "compile"]
CMD ["sbt", "assembly"]
WORKDIR /app
#ENTRYPOINT ["java","-jar","consumer-totsvconverter.jar"]
