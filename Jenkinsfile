pipeline {
    agent any
    environment {
	/*
    Tool name: 'sbt1.1.1' must match name sbt settings under Jenkins global tool configuration.
	*/
      SBT_HOME = tool name: 'sbt1.1.1', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'
      PATH = "/home/jenkins/jenkins/sdt-cara/sbt/sbt1.1.1/sbt/bin:${PATH}"
    }
	
	stages {
		stage('testing stuff') {
			steps {
				echo "Hello silvertail"
				sh 'sbt test'
			}
		}
		
	}
	
}