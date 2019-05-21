FROM openjdk:11
COPY target/scala-2.12/consumer-totsvconverter.jar /
ENTRYPOINT ["java","-jar","consumer-totsvconverter.jar"]
