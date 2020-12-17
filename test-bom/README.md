# Internal Platform

This is a java platform to add constraints for all java based projects in this repo. It will also get published as a bom.

See data-platform-iceberg/BUILD_README.md for how it is added.

Source on gradle java-platforms: https://docs.gradle.org/current/userguide/java_platform_plugin.html

Source on recommendation to do this: https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/setup-project-gradle.html


### Note:
Iceberg uses the `nebula.dependency-recommender` plugin to do this. But it's no longer getting support and the owners recommend switching over to this method: https://github.com/nebula-plugins/nebula-dependency-recommender-plugin#maintenance-mode-support.
