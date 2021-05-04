# Canton AWS CloudFormation Deployment

## Purpose and Limitations

This presents an example AWS CloudFormation deployment template for deploying a multi-node Canton topology to AWS.



# How to deploy

To build your Canton network on AWS, follow the instructions in the deployment guide. The deployment process includes these steps: (This tutorial is for v0.4.2)

1. If you don't already have an AWS account, sign up at https://aws.amazon.com.

2.  create an Amazon S3 bucket in one of the AWS Regions. When you create a bucket, you must choose a bucket name and Region. After you create a bucket, you cannot change the bucket name or Region
> An Amazon S3 bucket name is globally unique, and the namespace is shared by all AWS accounts. This means that after a bucket is created, the name of that bucket cannot be used by another AWS account in any AWS Region until the bucket is deleted.

The following rules apply for naming buckets in Amazon S3:
- Bucket names must be between 3 and 63 characters long.
- Bucket names can consist only of lowercase letters, numbers, dots (.), and hyphens (-).
- Bucket names must begin and end with a letter or number.

3. Then download the project and decompress it. In the first step, there is a folder with the name of the `lambdaFunction`, which contains two zip files with the following names:
- createDbLambdaFunction.zip
- createConfigonEfs.zip
  
Just upload these two zip files to the root of bucket that we created in the previous step.
 > Note that no changes to the two zip files are required and upload them without any changes. Also, these two zip files must be exactly inside the root of bucket we created. We will give the name of this bucket to the system during installation



4. Deploy required AWS services and Canton network via Cloudformation template
   
   4.1. Open the AWS CloudFormation console at https://console.aws.amazon.com/cloudformation 
   
   4.2. Create Stack.
   
   4.3. In the Template section, select **Upload a template file** then upload `canton.yaml` , and then choose Next:

   4.4. In the `Specify stack` details section, enter a `Stack name` ,which will serve as a name of a Cloudformation stack. For example:  MyCantonTestNetwork. The stack name can't contain spaces and must be at least 3 characters. 
   
   In the `S3bucketName` section ,  enter a S3 bucket name that we created in step `2`. 
   
   Then choose a name for the `DBPrefix` (must begin with a letter and contain only alphanumeric characters and a minimum of three (3) characters in length). 

   4.5. Next you need to choose a password for the PostgreSQL Master user and another passwords for the Canton Domains and Participants.

   Password requirments:
   - At least eight (8) characters;
   - must contain only alphanumeric characters
  
   and then choose `Next`.

   4.6. Review the information for the stack. When you're satisfied with the settings, choose `Create`.

> you must explicitly acknowledge that your stack template contains certain capabilities in order for AWS CloudFormation to create the stack.

  

5. After stack is provisioned, in the _Outputs_ section of the Cloudformation console you can find the AWS NLB DNS name together with Participant Navigator ports. You can use them to access the Navigator UI. 

For example : publicloadbalancercanton-a2c2e098cc581fa2.elb.eu-central-1.amazonaws.com:`4100` 


Where : 

`publicloadbalancercanton-a2c2e098cc581fa2.elb.eu-central-1.amazonaws.com` is a `AWS NLB DNS name`

`4100` is a Navigator port of Participant 1

# How to Test Your Canton Network

## 1. DAML Smart Contracts

### 1.1. Deploying the Model

Upload the desired dar file from any project with:

  

`daml ledger upload-dar --host <ledger_api_host> --port <ledger_api_port> <path_to_dar_file>`

  

This step will have to be repeated for any nodes that should be in communication with one another.

  

### 1.2. Interacting with the ledger

### 1.2.1. JSON API

In order to interact with the models in an easy and reliable fashion, we can use DAMLs JSON API, like so:

  

    daml json-api --ledger-host <ledger_api_host> \
    --ledger-port <ledger_api_port> \  
    --http-port <port_for_json_api> \   
    --package-reload-interval 5s \
    --allow-insecure-tokens

  
  

In this case we are allowing insecure tokens to facilitate testing and swapping between acting parties in the ledger. For this we can use [JWT](https://jwt.io/) to generate tokens based on this JSON argument:

  
  

    {
	    "https://daml.com/ledger-api": {   
		    "ledgerId": "<ledger_id>",
		    "applicationId": <application_id>,
		    "actAs": [
		    <party_identifier>
		    ]
	    }
    }

  
  

Where:

-  __ledger_id__ - Name of the service defined on node configuration for canton

-  __application_id__ - Identifier to be used for commands submitted using the token. Configuration defaults to "HTTP-JSON-API-Gateway"

-  __party_identifier__ - Identifier of the party to act in the ledger, as present in "party" field for DAML Assistant command `daml ledger list-parties --host <ledger_api_host> --port <ledger_api_port>`

  
  

### 1.2.2. Postman

To test all interactions using the [JSON API](https://docs.daml.com/json-api/), we can resort to postman to make the necessary requests to the ledger, which should follow a similar flow as, but not limited to:

1. Allocate all parties the are considered necessary for the different nodes

2. Perform an [Initiate and Accept](https://docs.daml.com/daml/patterns/initaccept.html#initiate-and-accept) pattern between parties in different nodes.
