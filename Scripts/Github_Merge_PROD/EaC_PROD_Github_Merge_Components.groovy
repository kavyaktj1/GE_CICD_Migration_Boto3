pipelineJob('ODP-PROD_Github_Merge_Asset') {
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
			scriptPath('Scripts/Github_Merge_PROD/ODP_Asset/MergeAutomaticallyPrFromFeatureToDevPipelineJenkinsfile')
			lightweight()
		}
	}
}
pipelineJob('ODP-PROD_Github_Merge_CI') {
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
			scriptPath('Scripts/Github_Merge_PROD/ODP_Commercial_Intensity/MergeAutomaticallyPrFromFeatureToDevPipelineJenkinsfile')
			lightweight()
		}
	}
}
pipelineJob('ODP-PROD_Github_Merge_Common') {
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
			scriptPath('Scripts/Github_Merge_PROD/ODP_Common/MergeAutomaticallyPrFromFeatureToDevPipelineJenkinsfile')
			lightweight()
		}
	}
}
pipelineJob('ODP-PROD_Github_Merge_CD') {
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
			scriptPath('Scripts/Github_Merge_PROD/ODP_Common_Dimensions/MergeAutomaticallyPrFromFeatureToDevPipelineJenkinsfile')
			lightweight()
		}
	}
}
pipelineJob('ODP-PROD_Github_Merge_IB_Intel') {
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
			scriptPath('Scripts/Github_Merge_PROD/ODP_IB_Intel/MergeAutomaticallyPrFromFeatureToDevPipelineJenkinsfile')
			lightweight()
		}
	}
}
pipelineJob('ODP-PROD_Github_Merge_IB_Upgrade') {
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
			scriptPath('Scripts/Github_Merge_PROD/ODP_IB_Upgrade/MergeAutomaticallyPrFromFeatureToDevPipelineJenkinsfile')
			lightweight()
		}
	}
}
pipelineJob('ODP-PROD_Github_Merge_MAPP') {
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
			scriptPath('Scripts/Github_Merge_PROD/ODP_MAPP/MergeAutomaticallyPrFromFeatureToDevPipelineJenkinsfile')
			lightweight()
		}
	}
}
pipelineJob('ODP-PROD_Github_Merge_SR') {
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
			scriptPath('Scripts/Github_Merge_PROD/ODP_ServiceRequest/MergeAutomaticallyPrFromFeatureToDevPipelineJenkinsfile')
			lightweight()
		}
	}
}
pipelineJob('ODP-PROD_Github_Merge_Outliers') {
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
			scriptPath('Scripts/Github_Merge_PROD/ODP_System_Outliers/MergeAutomaticallyPrFromFeatureToDevPipelineJenkinsfile')
			lightweight()
		}
	}
}
pipelineJob('ODP-PROD-Master_Assembly_Line') {
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
			scriptPath('Scripts/Jenkinsfile_Prod/Master_AssemblyLine_Prod')
			lightweight()
		}
	}
}
pipelineJob('ODP-PROD-Common-Assembly-Line') {
	parameters {
		stringParam('Repo_Name', '', '')
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
			scriptPath('Scripts/Jenkinsfile_Prod/Master_Jenkinsfile_Prod')
			lightweight()
		}
	}
}
pipelineJob('ODP-PROD-Common-Log_Creation') {
	parameters {
		stringParam('Repo_Name', '', '')
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
			scriptPath('Scripts/Jenkinsfile_Prod/Git_Log_info_Jenkinsfile_Prod')
			lightweight()
		}
	}
}
pipelineJob('ODP-PROD-Common_CSV_File_Creation') {
	parameters {
		stringParam('PR_ID', '', '')
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
			scriptPath('Scripts/Jenkinsfile_Prod/CSV_File_Common_Jenkinsfile_Prod')
			lightweight()
		}
	}
}