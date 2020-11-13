pipelineJob('ODP-PROD_Github_Merge_To_PROD_Asset') {
	parameters {
		stringParam('PR_ID', '', '')
	}
	definition {
		cpsScm {
			scm {
				git {
					remote {
						url('git@github.build.ge.com:DnA-ODP/ODP_Asset.git')
						credentials('git')
					}
					branch('*origin/ODP_NPROD')
				}
			}
			scriptPath('ODP/JobConfig/Jenkinsfile_Merge_ODP_DEV')
			lightweight()
		}
	}
}
pipelineJob('ODP-NPROD_Github_Merge_To_NPROD_Outliers') {
	parameters {
		stringParam('PR_ID', '', '')
	}
	definition {
		cpsScm {
			scm {
				git {
					remote {
						url('git@github.build.ge.com:DnA-ODP/ODP_System_Outliers.git')
						credentials('git')
					}
					branch('*origin/ODP_NPROD')
				}
			}
			scriptPath('ODP/JobConfig/Jenkinsfile_Merge_ODP_DEV')
			lightweight()
		}
	}
}
pipelineJob('ODP-NPROD_Github_Merge_To_NPROD_SR') {
	parameters {
		stringParam('PR_ID', '', '')
	}
	definition {
		cpsScm {
			scm {
				git {
					remote {
						url('git@github.build.ge.com:DnA-ODP/ODP_ServiceRequest.git')
						credentials('git')
					}
					branch('*origin/ODP_NPROD')
				}
			}
			scriptPath('ODP/JobConfig/Jenkinsfile_Merge_ODP_DEV')
			lightweight()
		}
	}
}
pipelineJob('ODP-NPROD_Github_Merge_To_NPROD_MAPP') {
	parameters {
		stringParam('PR_ID', '', '')
	}
	definition {
		cpsScm {
			scm {
				git {
					remote {
						url('git@github.build.ge.com:DnA-ODP/ODP_MAPP.git')
						credentials('git')
					}
					branch('*origin/ODP_NPROD')
				}
			}
			scriptPath('ODP/JobConfig/Jenkinsfile_Merge_ODP_DEV')
			lightweight()
		}
	}
}
pipelineJob('ODP-NPROD_Github_Merge_To_NPROD_IB_Upgrade') {
	parameters {
		stringParam('PR_ID', '', '')
	}
	definition {
		cpsScm {
			scm {
				git {
					remote {
						url('git@github.build.ge.com:DnA-ODP/ODP_IB_Upgrade.git')
						credentials('git')
					}
					branch('*origin/ODP_NPROD')
				}
			}
			scriptPath('ODP/JobConfig/Jenkinsfile_Merge_ODP_DEV')
			lightweight()
		}
	}
}
pipelineJob('ODP-NPROD_Github_Merge_To_NPROD_IB_Intel') {
	parameters {
		stringParam('PR_ID', '', '')
	}
	definition {
		cpsScm {
			scm {
				git {
					remote {
						url('git@github.build.ge.com:DnA-ODP/ODP_IB_Intel.git')
						credentials('git')
					}
					branch('*origin/ODP_NPROD')
				}
			}
			scriptPath('ODP/JobConfig/Jenkinsfile_Merge_ODP_DEV')
			lightweight()
		}
	}
}
pipelineJob('ODP-NPROD_Github_Merge_To_NPROD_CD') {
	parameters {
		stringParam('PR_ID', '', '')
	}
	definition {
		cpsScm {
			scm {
				git {
					remote {
						url('git@github.build.ge.com:DnA-ODP/ODP_Common_Dimensions.git')
						credentials('git')
					}
					branch('*origin/ODP_NPROD')
				}
			}
			scriptPath('ODP/JobConfig/Jenkinsfile_Merge_ODP_DEV')
			lightweight()
		}
	}
}
pipelineJob('ODP-NPROD_Github_Merge_To_NPROD_Common') {
	parameters {
		stringParam('PR_ID', '', '')
	}
	definition {
		cpsScm {
			scm {
				git {
					remote {
						url('git@github.build.ge.com:DnA-ODP/ODP_Common.git')
						credentials('git')
					}
					branch('*origin/ODP_NPROD')
				}
			}
			scriptPath('ODP/JobConfig/Jenkinsfile_Merge_ODP_DEV')
			lightweight()
		}
	}
}
pipelineJob('ODP-NPROD_Github_Merge_To_NPROD_CI') {
	parameters {
		stringParam('PR_ID', '', '')
	}
	definition {
		cpsScm {
			scm {
				git {
					remote {
						url('git@github.build.ge.com:DnA-ODP/ODP_Commercial_Intensity.git')
						credentials('git')
					}
					branch('*origin/ODP_NPROD')
				}
			}
			scriptPath('ODP/JobConfig/Jenkinsfile_Merge_ODP_DEV')
			lightweight()
		}
	}
}