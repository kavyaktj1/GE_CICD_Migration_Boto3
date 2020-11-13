#!/usr/bin/python3
from github import Github
import os
import sys
import boto3
import subprocess 
#####VARIABLE SECTION###########
merge_to_dev=''
approver_matched=''
approver_length=''
reviewer_str =''
success_merge_release_cnt=0
##################################


#Begining of main block

g = Github(base_url="https://github.build.ge.com/api/v3", login_or_token="b566b23e39d304804cb71c5e2b543f593b28dba5")

repo = g.get_repo("DnA-ODP/ODP_Spotfire")

pulls = repo.get_pulls(state='open', sort='created', base='master')
for pr in pulls :
    
    if 'backward' in str(pr.head.label):
        migration='backward'
        email=pr.user.login
        print(str(pr.number)+"|"+str(email)+"|"+str(migration))
