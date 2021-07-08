# s3-and-elasticsearch-file-uploader

This utility script is written in Python, making it cross-OS compatible. It will accept a story file (.txt or .epub) OR path to a folder (ending in a trailing slash) to recursively process all story files inside.

Processing a story file entails collecting the metadata, then uploading the story file to S3 and the metadata to AWS Elastic Search. It ensures consistency between both, including rolling back in case of any issue.

In the case of multi-file processing (such as pointing to a folder), any failed processing of a single story will not stop further processing of other stories. The result for any file(s) processed (including failure reasons) will be saved to a csv file for further inspection.

1. Set up your local environment to be able to run this script.
    - Configure the AWS CLI on your local machine.
        - For Windows: https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2-windows.html
    - Download Python on your local machine.
        - For Windows: https://www.python.org/downloads/
        - IMPORTANT: DO check the box to add Python to PATH.
                ![Alt text](check-add-to-path.jpg?raw=true "Check 'Add Python to PATH' during Python Installation.")
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
    - There are no Required Arguments that need to be provided when running the script.
    - Optional Script Arguments (Positional Arguments):
        - 1: Source path
            - Default Value: Current Folder (where the script is located)
            - Description: For the stories which should be processed (accepts files and folders- for folders, does all files inside)
        - 2: Desired CSV Results File Name
            - Default Value: story_upload_results
            - Description: For the results csv that will be generated.
    - Example Script Usages:
        - To upload the sample file in this repository, with a default name for the results csv file:
        `python story_uploader.py sample_file_folder\totally wacky_sample-fileNAME.txt`
        - To upload all files in the sample folder in this repository, with a desired name for the results csv file:
        `python story_uploader.py sample_file_folder all_stories_results`
        - To upload all files found in the same folder where the script is located (including nested folders), with a default name for the results csv file:
        `python story_uploader.py`



