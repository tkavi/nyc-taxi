Step 1 : Git & Visual Studio Code
1.	Download and install Git
2.	Create a repo for the project
3.	Connect the git account on visual studio code terminal using ‘git config –global’
4.	Clone the git repo in visual studio
5.	Add sample file or folder in vscode workspace and do git push to repo
   
Step 2: CloudFormation & Git
1.	Create a cloudformation yaml template for 4 S3 buckets (Raw, Processed, Curated, Master) and an IAM Service Role for AWS Glue. This role is granted "least privilege" access to read/write only to these specific buckets, as well as the standard permissions required to run Glue jobs.
2.	Create “cloud_formation” folder and add this template as nyc-taxi-infra.yaml inside the created folder.
3.	Push it to git
   
Step 3 : AWS Console - Cloud Formation
1.	Navigate to Cloud Formation Service and ‘Create a Stack’
2.	Select accordingly and give relevant names.
3.	Sync from Git -> add a new connection -> give a name -> click install
4.	It navigates to new browser for git connection with aws -> select your git account and select your repo -> okay
5.	Now, we can select further repo details -> new iam role
6.	On the final screen, check the box that says "I acknowledge that AWS CloudFormation might create IAM resources" before clicking Submit.
7.	Create the stack
8.	After stack created successfully, go to github and check for ‘Pull Requests’.
9.	If you see a PR from AWS, you must merge it. The stack will not create any resources until this PR is merged into your tracked branch which is ‘main’ in our case.

Possible Issues & Solutions
1.	While creating Cloud Formation stack, in Permissions – Optional, if it errors and doesn’t allow to move forward
Sol : For Permissions IAM role name -> follow the instructions for root and target accounts as below -
https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/stacksets-prereqs-self-managed.html

2.	Below error while trying to push to git
error: failed to push some refs to 'https://github.com/repo.git'
Sol : git pull -> opens new screen for merge mgs -> type msg or do nothing -> click ‘esc : wq’ in that order to come out of the screen -> git push
Code will be pushed to git. 

3.	The CloudFormationExecutionRole has full AdminstratorAccess, still Glue role given in the nyc-taxi-infra.yaml file failed during the stack creation.
Sol : Even if the role has AdministratorAccess, some cross-service operations (like handing a role to AWS Glue) require an explicit iam:PassRole to ensure you aren't escalating privileges.
IAM -> ExecutionRole -> Add Permissions -> Create inline policy -> JSON -> Paste below policy.
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowCloudFormationToPassGlueRole",
            "Effect": "Allow",
            "Action": "iam:PassRole",
            "Resource": "arn:aws:iam::YOUR_ACCOUNT_ID:role/nyc-taxi-*-glue-role"
        }
    ]
}

If this happened after first time creating a stack, its better to delete the stack and create a new stack after adding this policy.


