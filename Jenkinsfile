// See https://git.dev.box.net/FATools/jenkins-pipeline-library
@Library("jenkins-pipeline-library") _
@Library('security-checks-library') scl

pipeline {
  agent { label 'docker-2xlarge' }
  options {
    ansiColor('xterm')
    timestamps()
    timeout(time: 3, unit: 'HOURS')
  }
  environment {
    SKYNET_APP = 'data-platform-iceberg'
    SLACK_CHANNEL = '#eng-compute-int'
    GHE_ORGANIZATION = "${thisBuild.gitInfo.owner}"
    PUBLISH_PATH = "${GE_ORGANIZATION}/${SKYNET_APP}".toLowerCase()
    // Set to true if you wish to get verbose logging from JPL:
    PIPELINE_DEBUG = false
    // Used by security-checks library.
    JiraProject = 'CFRAME'
    SlackChannel = '#eng-compute-int'
    TEST_RESULTS = "**/test-results/**/*.xml"
    CONTAINER_IMAGE = "box-registry.jfrog.io/jenkins/box-centos7-build-jdk-zulu:8.x"
    DEFAULT_BRANCH = "master"
    TRUSTSTORE_PATH = '/tmp/pki/trust/test.jts'
    KEYSTORE_PATH = '/tmp/pki/certs/test.p12'
  }
  stages {
    stage('Prepare') {
      steps {
        prepareBuild script: this, path: PUBLISH_PATH
      }
    }
    stage('[Main Build]') {
      stages {
        stage('[Containerized Pipeline]') {
          agent {
            docker {
              label thisBuild.agentConfig.label
              reuseNode thisBuild.agentConfig.reuseNode
              image CONTAINER_IMAGE
              args thisBuild.agentConfig.dockerArgs
            }
          }
          stages {
            stage('Build, Test, Coverage, And Sonar') {
              agent {
                docker {
                  label thisBuild.agentConfig.label
                  reuseNode thisBuild.agentConfig.reuseNode
                  image CONTAINER_IMAGE
                  args thisBuild.agentConfig.dockerArgs
                }
              }
              steps {
                containerInit gradleArtifactoryCreds: true
                initJavaKeysAndCerts('test')
                sh '''
                    make checkstyle
                    make report
                '''
              }
              post {
                always {
                  junit allowEmptyResults: true, testResults: TEST_RESULTS
                }
              }
            }
          }
        }
      }
    }
  }
  post {
    always {
      archiveBuildInfo()
      slackBuildNotify to: "@${thisBuild.gitInfo.authorLogin}"
      slackBuildNotify to: SLACK_CHANNEL, onlyWhenFailed: true, onlyWhenFixed: true, onlyWhenBranchIsMaster: true
    }
    cleanup {
      cleanWs()
    }
  }
}
