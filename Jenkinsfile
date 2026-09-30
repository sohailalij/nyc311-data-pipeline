pipeline {
    agent any

    options {
        timeout(time: 20, unit: 'MINUTES')
        disableConcurrentBuilds()
    }

    environment {
        PROJECT_DIR = 'D:\\nyc311-data-pipeline'
        PYTHON      = 'D:\\nyc311-data-pipeline\\.venv\\Scripts\\python.exe'
    }

    stages {
        stage('Environment Check') {
            steps {
                dir("${env.PROJECT_DIR}") {
                    bat '"%PYTHON%" --version'
                    bat '"%PYTHON%" -m pytest --version'
                    bat 'java -version'
                }
            }
        }

        stage('Unit Tests') {
            steps {
                dir("${env.PROJECT_DIR}") {
                    bat '"%PYTHON%" -m pytest tests/test_transform_functions.py tests/test_postgres_helpers.py -v --junitxml=reports/unit-tests.xml'
                }
            }
        }

        stage('Data Quality Tests') {
            steps {
                dir("${env.PROJECT_DIR}") {
                    bat '"%PYTHON%" -m pytest tests/test_data_quality.py -v --junitxml=reports/data-quality-tests.xml'
                }
            }
        }
    }

    post {
        always {
            dir("${env.PROJECT_DIR}") {
                junit allowEmptyResults: false, testResults: 'reports/*.xml'
            }
        }
        success {
            echo 'All 19 tests passed.'
        }
        failure {
            echo 'Pipeline failed. Check the stage logs above.'
        }
    }
}