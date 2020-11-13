#!/usr/bin/python3
from github import Github
import os
import sys
import boto3
#####VARIABLE SECTION###########
merge_to_dev=''
approver_matched=''
approver_length=''
reviewer_str =''
success_merge_release_cnt=0

backlog_feature_br_approvers = ['503194400','503193288','503184414']
##################################


##print ("Total Number of PRs To Process are :" + str(pulls.totalCount))
def merge_feature_to_dev(pr,backlog_flag):

    try:
        #print ("Line : 17 inside merge_feature_to_dev : {}".format(pr))
        #print (pr)
        email=pr.user
        email=pr.user.login
        #print (email)
        global pr_number
        global reviewer_str
        pr_number=pr.number
        backlog_review_flag = False
        #print(pr.commits)
        
        #print("Going to merge PR_ID :"+str(pr_number))
        #v_commit=repo.merge("cicd_branch_2_for_testing","cicd_branch_3_testing","commit 3")
        #print('The Employee Ids who can Approve and there Name are:') 
        review_requests = pr.get_review_requests()
        #To get the employee id of the reviewrs
        reviewer_list=[]
        reviewer_name=[]
        reviewer_str = '|'+str(email)
        for reviewer in review_requests[0]:
            reviewer_list.append(reviewer.login)
            reviewer_name.append(reviewer.name)
            reviewer_str = reviewer_str+','+reviewer.login
    
        #print(reviewer_list)
        #print(reviewer_name)
        count_of_reviewer=len(reviewer_list)
        #check wethere the developer has added reviewer for that PR
        
        
        #count of how many reviewed
        test_review = pr.get_reviews()
        print(test_review)
        for review_1 in test_review:
            reviewer_str = reviewer_str+','+str(review_1.user.login)
            print(review_1)
        get_count_review=pr.get_reviews().totalCount
        
        for review_1 in test_review:
            review_det = str(review_1.user.login).strip()
            if backlog_flag==True:
                if review_det in backlog_feature_br_approvers:
                    backlog_review_flag = True
        # The below condition is called when PR is from 'Backlog' feature branch and not approved by the proper approvers.            
        if backlog_flag==True and backlog_review_flag==False:
            print(str(pr_number)+"|zeroapprove"+str(reviewer_str))
            return
        #print(type(get_count_review))
        #print_stmt="The count of review done is :" +str(get_count_review)
        #print(print_stmt)
        #print ("Line : 47 before get_count_review : {}".format(get_count_review))
        get_reviews=pr.get_reviews()
        #print(type(get_reviews))
        #print ("get_reviews:" + str(get_reviews))
        #print ("Line : 50 inside get_reviews : {}".format(get_reviews))
        #Getting the state i.e. "APPROVED"
        for reviews in get_reviews:
            #print("inside get_reviews")
            review_state=''
            review_state=reviews.state
            review_state=review_state.strip()
            #print ("Line : 57 inside merge_feature_to_dev : {}".format(review_state))
            #print (type(review_state))
            #print("Approved or not:"+ review_state)

        
        
             
        #print ("Line : 60 printing get_state : {}".format(review_state))        
        #review_state='APPROVED'
        if review_state=='APPROVED' :
            #print ("Line : 62 inside After approve : {}".format(pr))
            #os.system('pwd')
            #matched_approver=[]
            #approver_file = open("track_lead_list_of_approver", "r")
            
            length_reviewer_list=len(reviewer_list)
            print_stmt="INFO:Number of reviewers are: "+str(length_reviewer_list)
            #print(print_stmt)
            feature_to_dev_merge_msg="Merge to ODP_DEV Branch (#"+str(pr_number)+")"
            print_stmt="INFO:"+feature_to_dev_merge_msg
            #print(print_stmt)
            feature_to_dev_commit_title="PR:"+str(pr_number)+" commit title"
            print_stmt=feature_to_dev_commit_title
            #print (print_stmt)
            pull_done=pr.merge(feature_to_dev_merge_msg,feature_to_dev_commit_title,"squash")
            #print ("Line : 78 After merge to dev  : {}".format(pr_number))
            return "y"
            
    
    except NameError as e:
        print(str(pr_number)+"|zeroapprove"+str(reviewer_str))
        #print ("ERROR 1:Not Approved the PR :"+str(pr_number)+" .Please get it approved.")
        
    except:
        print(str(pr_number)+"|mergeconflictdev"+str(reviewer_str))
        #print ("ERROR 2:There is a error while merging"+ str(pr_number)+" from feature branch to Dev Branch.Hence Closing it")
        title="ERROR "+str(pr_number)+" while Merging to ODP_DEV"
        pr_closed=pr.edit(title='Close title', body='close body', state='closed', base='ODP_DEV')
        return 3
        
       
def merge_dev_to_release() :
    try:
        global pr_number
        global success_merge_release_cnt
        title="PR:"+str(pr_number)+" Create Pull to Release Branch"
        body="PR:"+str(pr_number)+" Body"
        pr_for_release_branch= repo.create_pull(title=title, body=body, head="ODP_DEV", base="CDRelease1.0")
        #print ("pr_for_release_branch: " + str(pr_for_release_branch.number))
        pr_to_be_merged_to_release=pr_for_release_branch.number
        #print("merge to Release branch")
        dev_to_release_merge_msg="Merge to ODP_DEV Branch (#"+str(pr_to_be_merged_to_release)+")"
        pull_done_release=pr_for_release_branch.merge(dev_to_release_merge_msg,"Merging to Release title","squash")
        #get_pr = repo.get_pulls(state='open', sort='created', base='release_branch_for_test')
        #print ("Total Number of PRs To Process are :" + str(pulls.totalCount))
        #print("pull_done:"+ str(pull_done_release))
        
        #To empty the file once the merge successfully done
        #print ("Line : 121 value success_merge_release_cnt  : {}".format(success_merge_release_cnt))
        success_merge_release_cnt=1
        #print ("Line : 123 value success_merge_release_cnt  : {}".format(success_merge_release_cnt))
        return 9
        #os.system('cat > Scripts/ListOfMergeConflictsReleaseBranch') 
    except:
        print(str(pr_number)+"|mergeconflictrelease"+str(reviewer_str))
        #print ("ERROR 2:There is a error while merging"+ str(pr_number)+" from Dev branch to Dev Branch.Hence Closing it")
        title="ERROR "+str(pr_number)+" while Merging to CDRelease1"
        pr_closed=pr_for_release_branch.edit(title=title, body='close body', state='closed', base='CDRelease1.0')
        #return 10
        
        
#Begining of main block
#This is Nigam's SSO ID Personal Access Token
#g = Github(base_url="https://github.build.ge.com/api/v3", login_or_token="b566b23e39d304804cb71c5e2b543f593b28dba5")

#This is CICD FSSO ID Personal Access Token
g = Github(base_url="https://github.build.ge.com/api/v3", login_or_token="9410aeef823911c5aa2814e7394dfe7334b53eb3")

repo = g.get_repo("DnA-ODP/ODP_ServiceRequest")


#cnt_line=os.system(wc -l 
pulls = repo.get_pulls(state='open', sort='created', base='ODP_DEV')

#print (pulls)
if pulls.totalCount >0 :
    for pr in pulls:
      #print ("Line : 146 calling pr id : {}".format(pr))
      #print ("Line : 152 value success_merge_release_cnt  : {}".format(success_merge_release_cnt))
      if success_merge_release_cnt == 0 :
        if 'Backlog' in str(pr.head.label):
            merge_to_release=merge_feature_to_dev(pr,True)
        else:
            merge_to_release=merge_feature_to_dev(pr,False)
        #to move out of script in case of mergeconflict in dev branch
        if merge_to_release == 3:
            sys.exit(0)
        #to move out of script in case of mergeconflict in release branch
        if merge_to_release == "y" :
            print(str(pr_number)+"|success"+str(reviewer_str))
            sys.exit(0) 
            #return_from_release=merge_dev_to_release()
            #if return_from_release == 9 :
                #print ("Going to exit the python script")
                 
            #print ("Line : 157 value success_merge_release_cnt  after function : {}".format(success_merge_release_cnt))
else:
    print("0|nopr"+str(reviewer_str))
    #print ( "ERROR 3:No PR's to process.Hence,ODP_Assmbely_Pipeline is not triggered")
    sys.stdout.flush()
    sys.exit(0)
