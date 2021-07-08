# s3-and-elasticsearch-file-uploader

This utility script is written in Python, making it cross-OS compatible. It will accept a story file, parse it for metadata, then upload the story file to S3 and the metadata to AWS Elastic Search. It ensures consistency between both, including rolling back in case of any issue.

1. Set up your local environment to be able to run this script.
    - Configure the AWS CLI on your local machine.
        - For Windows: https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2-windows.html
    - Download Python on your local machine.
        - For Windows: https://www.python.org/downloads/
        - IMPORTANT: DO check the box to add Python to PATH.
                ![Alt text](check-add-to-path.jpg?raw=true "Optional Title")
    - Rename 'rename_me_to_upload_parameters.json' to 'upload_parameters.json'
        - This file will be read by the story uploader script to get important information about your AWS Account, and is excluded by the .gitignore file intentionally. After all, if you do save this to your own repo or push anywhere, you don't want to push your AWS account information.

2. Configure the necessary resources on your AWS Account.
    - AWS S3 Bucket
        - Provide the bucket name in the 'upload_parameters.json'
    - AWS Elastic Search Cluster
        - Create it using Master User/Master User password to authenticate
        - Provide the Master User in the 'upload_parameters.json'
        - Provide the Master User in the 'upload_parameters.json'
        - Provide the Region in which you created this cluster in the 'upload_parameters.json'
        - Once the cluster is done provisioning, provide its Endpoint in the 'upload_parameters.json'


3. Run the provided script using your terminal and pointing to a story file which you want uploaded.
    - You may need to run `pip install -r requirements` to get the dependencies that this script requires, such as the elastic search client and boto3 for uploading data to AWS services.
    - Example - To upload the sample file in this repository:
    `python story_uploader.py sample-file\totally wacky_sample-fileNAME.txt`




