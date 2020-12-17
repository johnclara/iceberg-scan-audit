# Build Process

# WARNING: PLEASE DO NOT COPY PASTE ANY build.gradle

# Add a new SubProject
# TODO the steps have changed

## Step 0 (Choose Name)
Choose a name for your subproject: `subprojectName`

## Step 1 (Create Source Directory)
* If your library only has java dependencies or will have at least one scala 2.11 version:
    * Create a directory with your subproject name
* If it is a scala 2.12 subproject:
    * Create a directory with `subprojectName_2.12`
* If it is a different type of subproject:
    * you gotta figure that out on your own.

## Step 2 (Set up SubProject's build.gradle)

Add a `build.gradle` file within this directory with one of the following:

*If your library only has java dependencies:
```
apply from: "${project.rootDir}/gradle/customBuildscripts/java.gradle"


dependencies {
}
```
*If your library also has scala dependencies:
```
apply from: "${project.rootDir}/gradle/customBuildscripts/scala.gradle"

dependencies {
}
```

## Step 3 (Add SubProject to Root)

* If you would like to cross build your 2.11 subproject to 2.12, add `subprojectName` to subprojectsToCrossBuild.
* Otherwise, add your `subprojectName` to the `sourceSubProjects` in `settings.gradle`.
* Finally, run `./gradlew` and fix any missed steps.

# Add a Dependency to a SubProject

## Step 0 (Choose Dependency/SubProject)
Identify which dependency and version you would like to add to which subproject.

## Step 1 (Check/Add Platform Wide Constraints)
The `platform` subproject adds version constraints to all subprojects. Check the rules there and do the following:

* If the dependency you wish to add is already constrained by the rules in the platform:
    * and you're fine with the version:
        * skip to the `Step 2`
    * but the version you want is higher:
        * update the constraint here and verify if that's safe.
    * but you want a lower version:
        * figure that our on your own. not sure how to get around that.
* Otherwise: 
    * add your dependency to the constraints list.

## Step 2 (Add the dependency to your subproject)
Add the dependency to the build.gradle within your subproject's directory. **Do not include the version**, as it will
get set by the platform level constraints.

# Add Publishing to a SubProject

## Publish a java/scala subproject
* Add `apply from: 'apply from: "${project.rootDir}/gradle/customBuildscripts/publishing.gradle"'` to subproject directory's `build.gradle` at the top.
* Try `Publish a snapshot locally` for a specific subproject to make sure it works.

## Cross publish a scala subproject for 2.12

See Step 3 in `Add a new SubProject`

## Shade a SubProject (WARNING UNTESTED)

At a high level, the goal is to have a clean subproject which can publish nonshaded jars. Then there is a second subproject which depends on the first subproject in order to do the shaded publishing.

Attempt to copy the setup for `iceberg-catalog-shaded_2.12`. (Note that `iceberg-catalog` source code is cross built for 2.11 and for 2.12, but the shading is only for 2.12.)

## Step 0 (Identify/Create SubProject to Shade)
* Choose your subproject to shade: `baseSubProjectName`
* If it doesn't exist yet, fully follow the steps for `Add a new SubProject` with `baseSubProjectName` as `subprojectName`.

## Step 1 (Create Shading SubProject)
* Follow the steps in `Add a new SubProject` (again) but with `baseSubProjectName-shaded` as `subprojectName`.

## Step 2 (Set up Dependencies)
* Try to copy the setup from `iceberg-catalog-shaded_2.12`.

## Step 3 (Set up Shaded Publishing)
* Follow the steps in `Publish a java/scala subproject` This will use the `com.github.johnrengelman.shadow` plugin by default.

# Publish a snapshot locally
* For all subprojects:
    * `./gradlew publishMavenJavaPublicationToSnapshotRepository`
* For a specific subproject with name `subprojectName`:
    * `./gradlew :subprojectName:publishMavenJavaPublicationToSnapshotRepository`

# How it works:
* This is a multi-module gradle 6.7 build. It has a collection of java and scala subprojects with different cross building and publishing requirements.
    * multi-module build docs: https://docs.gradle.org/current/userguide/multi_project_builds.html
* `./gradle/customBuildScripts` holds buildscripts that hold logic for each type of subproject.
(We tried buildSrc convention plugins but intelliJ can't handle them well).
    * see: `./gradle/customBuildScripts/README.md`
* This repo uses a `java-platform` to governs dependencies across multiple subprojects. This the gradle equivalent to a mvn bom.xml. Currently, there is a single unpublished `platform` project which is a `java-platform`.
The `java` and `scala` buildscripts will automatically include the constraints defined by the `platform`.
    * see: `platform/README.md`
* The root project is responsible for setting up repo wide requirements. This includes adding `idea` plugins, repo level versions, and generating repo wide reports. Build logic that pertains to only certain subprojects should be moved to buildScripts.
