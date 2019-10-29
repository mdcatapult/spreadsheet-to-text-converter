FROM openjdk:11
COPY target/scala-2.12/consumer-spreadsheetconverter.jar /
ENTRYPOINT ["java", "-jar","consumer-spreadsheetconverter.jar"]
