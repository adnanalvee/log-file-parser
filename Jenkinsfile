pipeline {
    agent any
    environment {
      SBT_HOME = tool name: 'sbt1.1.1', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'
      PATH = "${env.SBT_HOME}/bin:${env.PATH}"
    }
	
	stages {
		stage('testing stuff') {
			steps {
				echo "Hello silvertail"
				echo "{$SBT_HOME}"
			}
		}
		
	}
	
}