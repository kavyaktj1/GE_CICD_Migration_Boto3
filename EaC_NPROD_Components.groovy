pipelineJob('ODP-NPROD-Assembly-Line') {
	parameters {
		stringParam('PR_ID', '-1', 'Pull Request ID to be migrated')
		stringParam('SSO_ID', '', 'SSO ID for Email Notification')
	}
	definition {
		cpsScm {
			scm {
				git {
					remote {
						url('git@github.build.ge.com:DnA-ODP/CICD_Migration_Boto3.git')
						credentials('git')
					}
					branch('*/master')
				}
			}
			scriptPath('Master_Jenkinsfile_NProd')
			lightweight()
		}
	}
}
pipelineJob('ODP-NPROD-Common-Assembly-Line') {
	parameters {
		stringParam('Repo_Name', '', '')
		stringParam('PR_ID', '-1', '')
		stringParam('SSO_ID', '', '')
	}
	definition {
		cpsScm {
			scm {
				git {
					remote {
						url('git@github.build.ge.com:DnA-ODP/CICD_Migration_Boto3.git')
						credentials('git')
					}
					branch('*/master')
				}
			}
			scriptPath('Scripts/Jenkinsfile_NProd/Master_Jenkinsfile_NProd')
			lightweight()
		}
	}
}
pipelineJob('ODP-NPROD-Master-Assembly-Line') {
	definition {
		cpsScm {
			scm {
				git {
					remote {
						url('git@github.build.ge.com:DnA-ODP/CICD_Migration_Boto3.git')
						credentials('git')
					}
					branch('*/master')
				}
			}
			scriptPath('Scripts/Jenkinsfile_NProd/Master_AssemblyLine_NProd')
			lightweight()
		}
	}
}
freeStyleJob('ODP-NPROD-Log_Creation') {
	scm {
		git {
			remote {
				url('git@github.build.ge.com:DnA-ODP/ODP_Product.git')
				credentials('git')
			}
		branch('*origin/ODP_NPROD')
		}
	}
	steps {
		shell('''pwd
		git log --stat --name-status --since=1.week > git_commit_info.txt''')
	}
}
pipelineJob('ODP-NPROD-CSV_File_Creation') {
	parameters {
		stringParam('PR_ID', '-1', '')
		stringParam('Workspace_Path', '', '')
	}
	definition {
		cpsScm {
			scm {
				git {
					remote {
						url('git@github.build.ge.com:DnA-ODP/CICD_Migration_Boto3.git')
						credentials('git')
					}
					branch('*/master')
				}
			}
			scriptPath('Scripts/Jenkinsfile_NProd/CSV_File_Jenkinsfile_NProd')
			lightweight()
		}
	}
}
pipelineJob('ODP-NPROD-Common-Log_Creation') {
	parameters {
		stringParam('Repo_Name', '', '')
		stringParam('PR_ID', '-1', '')
	}
	definition {
		cpsScm {
			scm {
				git {
					remote {
						url('git@github.build.ge.com:DnA-ODP/CICD_Migration_Boto3.git')
						credentials('git')
					}
					branch('*/master')
				}
			}
			scriptPath('Scripts/Jenkinsfile_NProd/Git_Log_info_Jenkinsfile_NProd')
			lightweight()
		}
	}
}
pipelineJob('ODP-NPROD-Deploy_Classifier') {
	parameters {
		stringParam('Workspace_Path', '', '')
		stringParam('PR_ID', '-1', '')
	}
	definition {
		cpsScm {
			scm {
				git {
					remote {
						url('git@github.build.ge.com:DnA-ODP/CICD_Migration_Boto3.git')
						credentials('git')
					}
					branch('*/master')
				}
			}
			scriptPath('Scripts/Jenkinsfile_NProd/Classifier_Jenkinsfile_NProd')
			lightweight()
		}
	}
}
pipelineJob('ODP-NPROD-Deploy_Lambda_Layer') {
	parameters {
		stringParam('Workspace_Path', '', '')
		stringParam('PR_ID', '-1', '')
	}
	definition {
		cpsScm {
			scm {
				git {
					remote {
						url('git@github.build.ge.com:DnA-ODP/CICD_Migration_Boto3.git')
						credentials('git')
					}
					branch('*/master')
				}
			}
			scriptPath('Scripts/Jenkinsfile_NProd/Lambda_Layer_Jenkinsfile_NProd')
			lightweight()
		}
	}
}
pipelineJob('ODP-NPROD-Deploy_CloudWatch') {
	parameters {
		stringParam('Workspace_Path', '', '')
		stringParam('PR_ID', '-1', '')
	}
	definition {
		cpsScm {
			scm {
				git {
					remote {
						url('git@github.build.ge.com:DnA-ODP/CICD_Migration_Boto3.git')
						credentials('git')
					}
					branch('*/master')
				}
			}
			scriptPath('Scripts/Jenkinsfile_NProd/CloudWatch_Jenkinsfile_NProd')
			lightweight()
		}
	}
}
pipelineJob('ODP-NPROD-Deploy_Crawler') {
	parameters {
		stringParam('Workspace_Path', '', '')
		stringParam('PR_ID', '-1', '')
	}
	definition {
		cpsScm {
			scm {
				git {
					remote {
						url('git@github.build.ge.com:DnA-ODP/CICD_Migration_Boto3.git')
						credentials('git')
					}
					branch('*/master')
				}
			}
			scriptPath('Scripts/Jenkinsfile_NProd/Crawler_Jenkinsfile_NProd')
			lightweight()
		}
	}
}
pipelineJob('ODP-NPROD-Deploy_DynamoDB') {
	parameters {
		stringParam('Workspace_Path', '', '')
		stringParam('PR_ID', '-1', '')
	}
	definition {
		cpsScm {
			scm {
				git {
					remote {
						url('git@github.build.ge.com:DnA-ODP/CICD_Migration_Boto3.git')
						credentials('git')
					}
					branch('*/master')
				}
			}
			scriptPath('Scripts/Jenkinsfile_NProd/DynamoDB_Jenkinsfile_NProd')
			lightweight()
		}
	}
}
pipelineJob('ODP-NPROD-Deploy_Glue') {
	parameters {
		stringParam('Workspace_Path', '', '')
		stringParam('PR_ID', '-1', '')
	}
	definition {
		cpsScm {
			scm {
				git {
					remote {
						url('git@github.build.ge.com:DnA-ODP/CICD_Migration_Boto3.git')
						credentials('git')
					}
					branch('*/master')
				}
			}
			scriptPath('Scripts/Jenkinsfile_NProd/Glue_Jenkinsfile_NProd')
			lightweight()
		}
	}
}
pipelineJob('ODP-NPROD-Deploy_Lambda') {
	parameters {
		stringParam('Workspace_Path', '', '')
		stringParam('PR_ID', '-1', '')
	}
	definition {
		cpsScm {
			scm {
				git {
					remote {
						url('git@github.build.ge.com:DnA-ODP/CICD_Migration_Boto3.git')
						credentials('git')
					}
					branch('*/master')
				}
			}
			scriptPath('Scripts/Jenkinsfile_NProd/Lambda_Jenkinsfile_NProd')
			lightweight()
		}
	}
}
pipelineJob('ODP-NPROD-Deploy_Redshift_DDL') {
	parameters {
		stringParam('PR_ID', '', '')
	}
	definition {
		cpsScm {
			scm {
				git {
					remote {
						url('git@github.build.ge.com:DnA-ODP/CICD_Migration_Boto3.git')
						credentials('git')
					}
					branch('*/master')
				}
			}
			scriptPath('Scripts/Jenkinsfile_NProd/Redshift_DDL_Jenkinsfile_NProd')
			lightweight()
		}
	}
}
pipelineJob('ODP-NPROD-Deploy_Redshift_SP') {
	parameters {
		stringParam('PR_ID', '', '')
	}
	definition {
		cpsScm {
			scm {
				git {
					remote {
						url('git@github.build.ge.com:DnA-ODP/CICD_Migration_Boto3.git')
						credentials('git')
					}
					branch('*/master')
				}
			}
			scriptPath('Scripts/Jenkinsfile_NProd/Redshift_SP_Jenkinsfile_NProd')
			lightweight()
		}
	}
}
pipelineJob('ODP-NPROD-Deploy_SNS') {
	parameters {
		stringParam('Workspace_Path', '', '')
		stringParam('PR_ID', '-1', '')
	}
	definition {
		cpsScm {
			scm {
				git {
					remote {
						url('git@github.build.ge.com:DnA-ODP/CICD_Migration_Boto3.git')
						credentials('git')
					}
					branch('*/master')
				}
			}
			scriptPath('Scripts/Jenkinsfile_NProd/SNS_Jenkinsfile_NProd')
			lightweight()
		}
	}
}
pipelineJob('ODP-NPROD-Deploy_StepFunction') {
	parameters {
		stringParam('Workspace_Path', '', '')
		stringParam('PR_ID', '-1', '')
	}
	definition {
		cpsScm {
			scm {
				git {
					remote {
						url('git@github.build.ge.com:DnA-ODP/CICD_Migration_Boto3.git')
						credentials('git')
					}
					branch('*/master')
				}
			}
			scriptPath('Scripts/Jenkinsfile_NProd/StepFunction_Jenkinsfile_NProd')
			lightweight()
		}
	}
}