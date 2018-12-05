pipeline {
    agent any
    environment {
      HTTP_PROXY = 'http://one.proxy.att.com:8080'
	  HTTPS_PROXY="http://one.proxy.att.com:8080"
      http_proxy="http://one.proxy.att.com:8080"
      https_proxy="http://one.proxy.att.com:8080"
	/*
    Tool name: 'sbt1.1.1' must match name sbt settings under Jenkins global tool configuration.
	*/
      JAVA_HOME = tool '1.8.0_121'
      ANT_HOME = tool 'ant'
      SBT_HOME = tool name: 'sbt1.1.1', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'
      PATH = "/home/jenkins/jenkins/sdt-cara/sbt/sbt1.1.1/sbt/bin:${PATH}"
    }
	
	stages {
		stage('testing stuff') {
			steps {
				echo "Hello silvertail"
                // sh 'sbt test'
			}
		}
        stage('branch master') {
            when {
                changeRequest branch: 'master'
            }
            steps {
                echo "there was a change request"
                sh 'sbt test'
                echo "hello"
            }
        }
		
	}
	
}