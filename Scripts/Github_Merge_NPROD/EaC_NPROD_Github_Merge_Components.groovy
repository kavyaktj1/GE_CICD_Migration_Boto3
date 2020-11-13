pipelineJob('ODP-NPROD_Github_Merge_Asset') {
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
			scriptPath('Scripts/Github_Merge_NPROD/ODP_Asset/MergeAutomaticallyPrFromFeatureToDevPipelineJenkinsfile')
			lightweight()
		}
	}
}
pipelineJob('ODP-NPROD_Github_Merge_CI') {
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
			scriptPath('Scripts/Github_Merge_NPROD/ODP_Commercial_Intensity/MergeAutomaticallyPrFromFeatureToDevPipelineJenkinsfile')
			lightweight()
		}
	}
}
pipelineJob('ODP-NPROD_Github_Merge_Common') {
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
			scriptPath('Scripts/Github_Merge_NPROD/ODP_Common/MergeAutomaticallyPrFromFeatureToDevPipelineJenkinsfile')
			lightweight()
		}
	}
}
pipelineJob('ODP-NPROD_Github_Merge_CD') {
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
			scriptPath('Scripts/Github_Merge_NPROD/ODP_Common_Dimensions/MergeAutomaticallyPrFromFeatureToDevPipelineJenkinsfile')
			lightweight()
		}
	}
}
pipelineJob('ODP-NPROD_Github_Merge_IB_Intel') {
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
			scriptPath('Scripts/Github_Merge_NPROD/ODP_IB_Intel/MergeAutomaticallyPrFromFeatureToDevPipelineJenkinsfile')
			lightweight()
		}
	}
}
pipelineJob('ODP-NPROD_Github_Merge_IB_Upgrade') {
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
			scriptPath('Scripts/Github_Merge_NPROD/ODP_IB_Upgrade/MergeAutomaticallyPrFromFeatureToDevPipelineJenkinsfile')
			lightweight()
		}
	}
}
pipelineJob('ODP-NPROD_Github_Merge_MAPP') {
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
			scriptPath('Scripts/Github_Merge_NPROD/ODP_MAPP/MergeAutomaticallyPrFromFeatureToDevPipelineJenkinsfile')
			lightweight()
		}
	}
}
pipelineJob('ODP-NPROD_Github_Merge_SR') {
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
			scriptPath('Scripts/Github_Merge_NPROD/ODP_ServiceRequest/MergeAutomaticallyPrFromFeatureToDevPipelineJenkinsfile')
			lightweight()
		}
	}
}
pipelineJob('ODP-NPROD_Github_Merge_Outliers') {
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
			scriptPath('Scripts/Github_Merge_NPROD/ODP_System_Outliers/MergeAutomaticallyPrFromFeatureToDevPipelineJenkinsfile')
			lightweight()
		}
	}
}