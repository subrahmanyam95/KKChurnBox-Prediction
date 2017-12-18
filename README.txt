For this project we have stored data in Amazon S3 and used Amazon EMR to run the clusters.

Following are the steps to follow:
1)Create/login to AWS(Amazon Web Services)
2)In the AWS menu select S3. Download the required files from the URL https://www.kaggle.com/c/kkbox-churn-prediction-challenge/data.
3)Create a new bucket with the name “s3kkbox” with versioning enabled and permissions set to public .
4)Since our dataset is very large we have uploaded the data into Amazon S3 using AWS CLI.
5)To Install AWS CLI following are the commands:

	$ curl "https://s3.amazonaws.com/aws-cli/awscli-bundle.zip" -o "awscli-bundle.zip"
	$ unzip awscli-bundle.zip
	$ sudo ./awscli-bundle/install -i /usr/local/aws -b /usr/local/bin/aws

6)check if aws cli is installed by giving the command
	$aws

If the output is as follows then aws is succesfully installed.
	Output:
	usage: aws [options] <command> <subcommand> [<subcommand> ...] [parameters]
	To see help text, you can run:

  	aws help
  	aws <command> help
  	aws <command> <subcommand> help
	aws: error: too few arguments

7)Configure aws tool access with the help of following commands:
Replace with your AWS Access Key ID and AWS Secret Access Key which can be found in Your Security Credentials on the AWS Console.
$ aws configure

AWS Access Key ID: <Your-Access-KeyID>
AWS Secret Access Key: <Your-secret-access-key>
Default region name [us-west-2]: us-west-2
Default output format [None]: json

8)To upload the large files we have to use the following command:
Traverse to the location where your files are present run these commands in the terminal accordingly.

To upload single file at a time:
$ aws s3 cp localfilename s3location
e.g:$ aws s3 cp transcations.csv s3://s3kkbox

To sync a folder to s3 bucket
$ aws s3 sync . s3://my-bucket/my-dir/
e.g:$ aws s3 sync . s3://s3kkbox

With the help of above commands we have uploaded data to the Amazon S3 bucket.

9)Generate key value pair by following the instructions in the link  http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html

10)In the aws menu select EMR. In the clusters menu ,select 'Create Cluster'.

11)In the Quick Cluster Configuration set the following :

Cluster name :My Cluster
Logging :Enabled
S3 Folder : select the bucket s3kkbox which is created in the previous step.
Launchmode: Cluster
Release : emr-5.10.0
Applications:Spark: Spark 2.2.0 on Hadoop 2.7.3 YARN with Ganglia 3.7.2 and Zeppelin 0.7.3
Instance Type:m4.xlarge
Number of Instances: 4
EC2key pair: Select the EC2 key pair which is created in the above step.
Then select 'Create cluster'

This step creates the cluster with required master and slave nodes.

12)When the cluster is started in the web connections Zeppelin is enabled.

13)Open Zeppelin in a new tab and then using import notebook option, link the .json files (which are submitted) to the cluster 
 
   

