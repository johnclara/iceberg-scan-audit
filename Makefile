
GRADLE=./gradlew

.PHONY: format publishSnapshot test

# Commands for humans
test:
	$(GRADLE) test --info --stacktrace

clean:
	$(GRADLE) clean
	rm -r s3mock/*

format:
	$(GRADLE) goJF --info --stacktrace

publishSnapshot:
	$(GRADLE) -DskipSign=true publishAllPublicationsToSnapshotRepository --info --stacktrace

# Commands for jenkins
build:
	$(GRADLE) build --info --stacktrace

checkstyle: build
	$(GRADLE) verGJF scalastyleCheck --info --stacktrace

report: build
	$(GRADLE) jacocoAggregateReport --info --stacktrace

sonarqube: report
	$(GRADLE) -Dsonar.host.url=http://sonarqube.dev.box.net:80 sonarqube --info --stacktrace
