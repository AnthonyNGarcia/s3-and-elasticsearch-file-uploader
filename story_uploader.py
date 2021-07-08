from elasticsearch import Elasticsearch, RequestsHttpConnection
import json
import boto3
import sys
import os
import glob

def tally_provided_uploaded_parameters(provided_parameters, all_expected_parameters, parent_parameter):
    for provided_parameter in provided_parameters:
        if provided_parameter not in all_expected_parameters:
            if parent_parameter:
                print("Warning: Found an unrecognized upload parameter: '" + provided_parameter + "' as a nested parameter to '" + parent_parameter + "'. This script ignores unrecognized upload parameters.")
            else:
                print("Warning: Found an unrecognized upload parameter: '" + provided_parameter + "'. This script ignores unrecognized upload parameters.")
        else:
            parameter_data = provided_parameters[provided_parameter]
            if not parameter_data and type(parameter_data) is not bool:
                if parent_parameter:
                    print("ERROR: An empty value was provided for the parameter: '" + provided_parameter + "'! This parameter is nested in '" + parent_parameter + "'. Please provide a valid value to this parameter, and then try running this script again.")
                else:
                    print("ERROR: An empty value was provided for the parameter: '" + provided_parameter + "'! Please provide a valid value to this parameter, and then try running this script again.")
                quit()
            all_expected_parameters[provided_parameter]["was_found"] = True
            expected_parameter_data = all_expected_parameters[provided_parameter]
            if "nested_parameters" in expected_parameter_data:
                nested_provided_parameters = provided_parameters[provided_parameter]
                nested_expected_parameters = all_expected_parameters[provided_parameter]["nested_parameters"]
                tally_provided_uploaded_parameters(nested_provided_parameters, nested_expected_parameters, provided_parameter)

def validate_expected_upload_parameters_were_provided(all_expected_parameters, parent_parameter):
    for expected_parameter in all_expected_parameters:
        expected_parameter_data = all_expected_parameters[expected_parameter]
        if expected_parameter_data["is_required"]:
            if not expected_parameter_data["was_found"]:
                if parent_parameter:
                    print("ERROR: The Required Parameter '" + expected_parameter + "', which is supposed to be nested in '" + parent_parameter + "' was missing from the upload_parameters.json. Please provide that parameter with a valid value, and then try running this script again.")
                else:
                    print("ERROR: The Required Parameter '" + expected_parameter + "' was missing from the upload_parameters.json. Please provide that parameter with a valid value, and then try running this script again.")
                quit()
        else:
            if not expected_parameter_data["was_found"]:
                if parent_parameter:
                    print("Warning: The Optional Parameter '" + expected_parameter + "', which is supposed to be nested in '" + parent_parameter + "' was missing from the upload_parameters.json...")
                else:
                    print("Warning: The Optional Parameter '" + expected_parameter + "' was missing from the upload_parameters.json...")
        if "nested_parameters" in expected_parameter_data:
            nested_expected_parameter_data = expected_parameter_data["nested_parameters"]
            validate_expected_upload_parameters_were_provided(nested_expected_parameter_data, expected_parameter)

expected_upload_parameters = {
    "script_logging": {
        "is_required": False,
        "was_found": False,
        "nested_parameters": {
            "silence_script_progression_logs": {"is_required": False, "was_found": False},
            "silence_missing_optional_metadata_warning": {"is_required": False, "was_found": False}
        }
    },
    "s3": {
        "is_required": True,
        "was_found": False,
        "nested_parameters": {"bucket_name": {"is_required": True, "was_found": False}}
    },
    "elastic_search": {
        "is_required": True,
        "was_found": False,
        "nested_parameters": {
            "endpoint": {"is_required": True, "was_found": False},
            "region": {"is_required": True, "was_found": False},
            "master_user": {"is_required": True, "was_found": False},
            "master_user_password": {"is_required": True, "was_found": False},
            "index": {"is_required": True, "was_found": False}}
    }
}

upload_parameters = {}

with open('upload_parameters.json') as upload_parameters_file:
    upload_parameters = json.load(upload_parameters_file)

tally_provided_uploaded_parameters(upload_parameters, expected_upload_parameters, '')
validate_expected_upload_parameters_were_provided(expected_upload_parameters, '')

def transform_array_value(raw_value):
    value = f'["{raw_value}"]'
    if "," in raw_value:
        value = '['
        value_array = raw_value.split(",")
        for inner_value in value_array:
            value += f'"{inner_value.lstrip()}", '
        value = value.rstrip(", ")
        value += ']'
    return value

def transform_string_value(raw_value):
    return f'"{raw_value}"'

def transform_number_value(raw_value):
    value = raw_value
    if "," in raw_value:
        value = ""
        value_array = raw_value.split(",")
        for inner_value in value_array:
            value += inner_value
    return value

def transform_title(line):
    return f'"Title": "{line}"'

def transform_author(line):
    return f', "Author": "{line.lstrip("by ")}"'

def transform_field(raw_field):
    return raw_field.replace(" ", "")

transformers_for_expected_story_metadata = {
    "Title": {
        "transformer": transform_title,
        "is_required": True,
    },
    "Author": {
        "transformer": transform_author,
        "is_required": True,
    },
    "Category": {
        "transformer": transform_array_value,
        "is_required": False,
    },
    "Genre": {
        "transformer": transform_array_value,
        "is_required": False,
    },
    "Language": {
        "transformer": transform_string_value,
        "is_required": False,
    },
    "Status": {
        "transformer": transform_string_value,
        "is_required": False,
    },
    "Published":{
        "transformer": transform_string_value,
        "is_required": False,
    },
    "Updated": {
        "transformer": transform_string_value,
        "is_required": False,
    },
    "Packaged": {
        "transformer": transform_string_value,
        "is_required": False,
    },
    "Rating": {
        "transformer": transform_string_value,
        "is_required": False,
    },
    "Chapters": {
        "transformer": transform_number_value,
        "is_required": False,
    },
    "Words": {
        "transformer": transform_number_value,
        "is_required": False,
    },
    "Publisher": {
        "transformer": transform_string_value,
        "is_required": False,
    },
    "StoryURL": {
        "transformer": transform_string_value,
        "is_required": False,
    },
    "AuthorURL": {
        "transformer": transform_string_value,
        "is_required": False,
    },
    "Summary": {
        "transformer": transform_string_value,
        "is_required": False,
    },
    "Warnings": {
        "transformer": transform_string_value,
        "is_required": False,
    },
    "Relationships": {
        "transformer": transform_string_value,
        "is_required": False,
    },
    "Comments": {
        "transformer": transform_number_value,
        "is_required": False,
    },
    "Kudos": {
        "transformer": transform_number_value,
        "is_required": False,
    }
}

silence_script_progression_logs = False
silence_missing_optional_metadata_warning = False
if "script_logging" in upload_parameters and upload_parameters["script_logging"] and "silence_script_progression_logs" in upload_parameters["script_logging"]:
    silence_script_progression_logs = upload_parameters["script_logging"]["silence_script_progression_logs"]
if "script_logging" in upload_parameters and upload_parameters["script_logging"] and "silence_missing_optional_metadata_warning" in upload_parameters["script_logging"]:
    silence_missing_optional_metadata_warning = upload_parameters["script_logging"]["silence_missing_optional_metadata_warning"]

s3 = boto3.client('s3')
s3_upload_parameters = upload_parameters["s3"]
bucket_name = s3_upload_parameters["bucket_name"]
s3_object_version_id = ""

elastic_search_parameters = upload_parameters["elastic_search"]
host = elastic_search_parameters["endpoint"]
region = elastic_search_parameters["region"]
master_user = elastic_search_parameters["master_user"]
master_user_password = elastic_search_parameters["master_user_password"]
index = elastic_search_parameters["index"]

es = Elasticsearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = (master_user, master_user_password),
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
)
successful_elastic_search_upload = False

def required_metadata_was_collected(metadata_dictionary):
    for metadata_field in metadata_dictionary:
        if metadata_dictionary[metadata_field]["is_required"]:
            return False
    return True

def process_txt_story_metadata(story_file_name):
    if not silence_script_progression_logs:
        print("COLLECTING TXT STORY METADATA...")
    
    json_metadata_file_name = story_file_name.rstrip(".txt") + ".json"
    with open(story_file_name, "r") as file_to_transform, open(json_metadata_file_name, "w") as json_metadata_file:
        json_metadata_file.write('{')
        remaining_data_to_collect = transformers_for_expected_story_metadata.copy()
        while remaining_data_to_collect:
            raw_line = file_to_transform.readline()
            if raw_line == "\n":
                continue
            else:
                line = raw_line.rstrip()
                if "Title" in remaining_data_to_collect:
                    transformer = remaining_data_to_collect["Title"]["transformer"]
                    transformed_line = transformer(line)
                    json_metadata_file.write(transformed_line)
                    remaining_data_to_collect.pop("Title")
                elif "Author" in remaining_data_to_collect:
                    transformer = remaining_data_to_collect["Author"]["transformer"]
                    transformed_line = transformer(line)
                    json_metadata_file.write(transformed_line)
                    remaining_data_to_collect.pop("Author")
                    continue
                elif len(line.split(":")) >= 2:
                    raw_field = line.split(":")[0]
                    transformed_field = transform_field(raw_field)
                    raw_value = line.lstrip(raw_field + ": ")
                    if (transformed_field in remaining_data_to_collect):
                        transformer = remaining_data_to_collect[transformed_field]["transformer"]
                        transformed_value = transformer(raw_value)
                        json_metadata_file.write(f', "{transformed_field}": {transformed_value}')
                        remaining_data_to_collect.pop(transformed_field)
                    if not remaining_data_to_collect:
                        if not silence_script_progression_logs:
                            print("Awesome Possum! Found all the required AND optional story metadata! :)")
                        break
                else:
                    if required_metadata_was_collected(remaining_data_to_collect):
                        if not silence_missing_optional_metadata_warning:
                            print("Warning: Although all the Required metadata was found, some Optional metadata was missing, indicated below.")
                            for missing_field in remaining_data_to_collect:
                                print("\t" + missing_field + " (Optional)")
                        break
                    else:
                        print("ERROR: We just finishing reading the metadata section of the provided file, but we didn't find all the metadata which was specifically requested to be Required! :(")
                        for missing_field in remaining_data_to_collect:
                            required_or_optional = "Required" if remaining_data_to_collect[missing_field]["is_required"] else "Optional"
                            print("\t" + missing_field + " (" + required_or_optional + ")")
                        print("Please update either the file to include the missing Required metadata, or update which metadata should be Required, and then try running this script again.")
                        quit()
        json_metadata_file.write("}")

    if not silence_script_progression_logs:
        print("SUCCESSFULLY COLLECTED TXT STORY METADATA!")
    return json_metadata_file_name

def upload_story_file_to_s3(story_file_name):
    try:
        with open(story_file_name, "rb") as story_file:
            if not silence_script_progression_logs:
                print("S3 UPLOAD OF STORY FILE INITIATED...")
            s3_upload_response = s3.put_object(Body=story_file, Bucket=bucket_name, Key=story_file_name)
            if s3_upload_response and "VersionId" in s3_upload_response and s3_upload_response["VersionId"]:
                if not silence_script_progression_logs:
                    print("S3 UPLOAD OF STORY FILE SUCCESSFUL!")
                return s3_upload_response["VersionId"]
            else:
                print("ERROR: Failed processing as we did not get a successful response when trying to upload the story file to S3. Double-check S3 parameters provided to this script as well as the S3 bucket configurations on AWS. Also verify whether your terminal is configured with the AWS CLI.")
                quit()
    except Exception as e:
        print(e)
        print("ERROR: Failed processing as there was an exception thrown when trying to upload the story file to S3. Double-check S3 parameters provided to this script as well as the S3 bucket configurations on AWS. Also verify whether your terminal is configured with the AWS CLI.")
        quit()

def rollback_s3_upload_from_failed_elastic_search_upload(reason, story_file_name, s3_object_version_id):
    print("ERROR: " + reason)
    print("S3 UPLOAD ROLLBACK INITIATED: Even though there was not an issue with uploading the story file to S3, because there was an issue at a later step in this process, the file that was just uploaded to S3 is being deleted, to prevent unnecessary orphan/unreferenced/unsearchable S3 objects. Other S3 objects are NOT being deleted, nor any other versions of files by the same name as this one. Only one file- the specific file that was uploaded just now with this script right before the error issue above came up, is being deleted.")
    s3.delete_object(Bucket=bucket_name, Key=story_file_name, VersionId=s3_object_version_id)
    print("S3 UPLOAD ROLLBACK COMPLETED.")
    quit()

def upload_metadata_file_to_elastic_search(story_file_name, json_metadata_file_name, s3_object_version_id):
    elastic_search_upload_response = {}
    try:
        with open(json_metadata_file_name) as json_metadata_file:
            story_metadata = json.load(json_metadata_file)
            story_metadata["s3_object_key"] = story_file_name
            story_metadata["s3_object_version_id"] = s3_object_version_id
            if not silence_script_progression_logs:
                print("ELASTIC SEARCH UPLOAD OF STORY METADATA INITIATED...")
            elastic_search_upload_response = es.index(index=index, doc_type="_doc", body=story_metadata)
            successful_elastic_search_upload = True if elastic_search_upload_response["result"] == "created" else False
    except Exception as e:
        print(e)
        try:
            if elastic_search_upload_response and "id" in elastic_search_upload_response and elastic_search_upload_response["id"]:
                es.delete(index=index, id=elastic_search_upload_response["_id"])
        except Exception as e:
            print(e)
        finally:
            rollback_s3_upload_from_failed_elastic_search_upload("Failed processing as there was an exception thrown issue when trying to upload the file metadata to AWS ElasticSearch. Double-check elastic_search parameters provided to this script as well as the Elastic Search configurations on AWS.", story_file_name, s3_object_version_id)
    if successful_elastic_search_upload:
        if not silence_script_progression_logs:
            print("ELASTIC SEARCH UPLOAD OF STORY METADATA SUCCESSFUL!")
    else:
        try:
            if elastic_search_upload_response and "id" in elastic_search_upload_response and elastic_search_upload_response["id"]:
                es.delete(index=index, id=elastic_search_upload_response["_id"])
        except Exception as e:
            print(e)
        finally:
            rollback_s3_upload_from_failed_elastic_search_upload("While there was no exception thrown during the Elastic Search upload, the response received by them indicates that the metadata may not have been saved properly. Thus, this is considered to have been a Failed file processing. This may just be a minor glitch on AWS side- try rerunninng this script to see if this error persists. If it does, then double-check the elastic search configurations provided in the upload_parameters.json.", story_file_name, s3_object_version_id)
    

def process_and_upload_txt_story(story_file_name):
    if not silence_script_progression_logs:
        print(">>> STARTED PROCESSING TXT FILE: " + story_file_name)
    json_metadata_file_name = process_txt_story_metadata(story_file_name)
    s3_object_version_id = upload_story_file_to_s3(story_file_name)
    upload_metadata_file_to_elastic_search(story_file_name, json_metadata_file_name, s3_object_version_id)
    if not silence_script_progression_logs:
        print(">>> SUCCESSFULLY PROCESSED TXT FILE: " + story_file_name)
   
provided_file_path = sys.argv[1]
provided_file_path_is_folder = os.path.isdir(provided_file_path)
if provided_file_path_is_folder:
    for file_name in glob.iglob(provided_file_path + '**/*.txt', recursive=True):
        process_and_upload_txt_story(file_name)
else:
    process_and_upload_txt_story(provided_file_path)
