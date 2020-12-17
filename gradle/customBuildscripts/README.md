# DataPlatform Iceberg Build

This defines common logic for java projects, scala projects (which are also java projects), and java projects which
need to be published.

We use buildscripts instead of buildSrc because buildSrc does not interact cleanly with intelliJ at all.

Docs: https://docs.gradle.org/current/userguide/sharing_build_logic_between_subprojects.html

More docs: https://docs.gradle.org/current/userguide/organizing_gradle_projects.html#sec:build_sources

Example Project: https://docs.gradle.org/current/samples/sample_convention_plugins.html
