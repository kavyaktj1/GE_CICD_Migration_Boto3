#!/usr/bin/python3
from github import Github
import os
import sys
import boto3

reviewer_str =''

def get_users_from_pr(pr):

    try:
        email=pr.user
        email=pr.user.login
        global reviewer_str
        pr_number=pr.number
        review_requests = pr.get_review_requests()
        reviewer_str = str(email)
        for reviewer in review_requests[0]:
            reviewer_str = reviewer_str+','+reviewer.login
        test_review = pr.get_reviews()
        for review_1 in test_review:
            reviewer_str = reviewer_str+','+str(review_1.user.login)
        return reviewer_str
    except Exception as e:
        print('ERROR :',e)

g = Github(base_url="https://github.build.ge.com/api/v3", login_or_token="b566b23e39d304804cb71c5e2b543f593b28dba5")

repo = g.get_repo("DnA-ODP/ODP_Product")

pr_str = sys.argv[1]
pr_list_str = pr_str.split(',')
pr_list = []
for pr in pr_list_str:
    pr_list.append(int(pr))

pulls = repo.get_pulls(state='closed', sort='created', base='ODP_DEV')
rev_str = ''
if pulls.totalCount >0 :
    for pr in pulls:
      if pr.number in pr_list:
        rev_str = get_users_from_pr(pr)+','+rev_str
       
setforpr = set(rev_str.split(','))
listsetforpr = list(setforpr)
finalpr_str = ''
for pr in listsetforpr:
    if pr!='':
        finalpr_str = pr+','+finalpr_str
        
print(finalpr_str)