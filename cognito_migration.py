import json
import boto3
import os
import time

REGION = "eu-central-1"
S3_CLIENT = boto3.client("s3", REGION)
COGNITO_CLIENT = boto3.client("cognito-idp",REGION)
USER_POOL_ID = "eu-central-1_QmmAGW6ro"
EXPORTED_RECORDS_COUNT = 0
BUCKET_NAME = "migration-cognito-test"
PAGINATION_COUNTER = 0
CSV_FILE_NAME = "CognitoUsers.csv"
REQUIRED_ATTRIBUTES = ["name",
        "given_name",
        "family_name",
        "middle_name",
        "nickname",
        "preferred_username",
        "profile",
        "picture",
        "website",
        "email",
        "email_verified",
        "gender",
        "birthdate",
        "zoneinfo",
        "locale",
        "phone_number",
        "phone_number_verified",
        "address",
        "updated_at",
        "custom:joinedOn",
        "cognito:mfa_enabled",
        "cognito:username"]
MAX_NUMBER_RECORDS = 0
LIMIT = 60
FILE_PATH = os.path.join(os.path.dirname(__file__), f"../../tmp/{CSV_FILE_NAME}")
def get_list_cognito_users(cognito_idp_client, next_pagination_token ="", Limit = LIMIT):  

    return cognito_idp_client.list_users(
        UserPoolId = USER_POOL_ID,
        Limit = Limit,
        PaginationToken = next_pagination_token
    ) if next_pagination_token else cognito_idp_client.list_users(
        UserPoolId = USER_POOL_ID,
        Limit = Limit
    ) 
    
def check_next_pagination_token_existence(user_records):
    if set(["PaginationToken","NextToken"]).intersection(set(user_records)):
        pagination_token = user_records["PaginationToken"] if "PaginationToken" in user_records else user_records["NextToken"]
        return pagination_token
    else:
        pagination_token = None
        return pagination_token

def write_to_csv_file(user_records, csv_file, csv_new_line):
    csv_lines = []
    for user in user_records["Users"]:
        csv_line = csv_new_line.copy()
        for required_attribute in REQUIRED_ATTRIBUTES:
            csv_line[required_attribute] = ""
            if required_attribute in user.keys():
                csv_line[required_attribute] = str(user[required_attribute])
                continue
            if required_attribute == "phone_number_verified" or required_attribute == "cognito:mfa_enabled" :
                csv_line[required_attribute] = "false"
            if required_attribute == "email_verified" and required_attribute not in user.keys():
                csv_line[required_attribute] = "false"
            for user_attribute in user["Attributes"]:
                if user_attribute["Name"] == required_attribute:
                    csv_line[required_attribute] = str(user_attribute["Value"])
        csv_line["cognito:username"] = user["Username"]
        if not csv_line["email_verified"] == "false":
            csv_lines.append(",".join(csv_line.values()) + "\n")       
    print(csv_lines)
    csv_file.writelines(csv_lines)
    global EXPORTED_RECORDS_COUNT
    EXPORTED_RECORDS_COUNT += len(csv_lines)
    global PAGINATION_COUNTER
    print("Page: #{} \n Total Exported Records: #{} \n".format(str(PAGINATION_COUNTER), str(EXPORTED_RECORDS_COUNT)))

def open_csv_file(csv_new_line):
    try:
        csv_file = open(FILE_PATH, "w")
        csv_file.write(",".join(csv_new_line.keys()) + "\n")
    except Exception as err:
        error_message = repr(err)
        print("\nERROR: Can not create file: " + file_path)
        print("\tError Reason: " + error_message)
        exit()    
    return csv_file

def cooldown_before_next_batch():
    time.sleep(0.15)

def save_file():
    with open(FILE_PATH, "rb") as f:
        S3_CLIENT.upload_fileobj(f, BUCKET_NAME, CSV_FILE_NAME)

def lambda_handler(event, context):
    
    pagination_token = ""
    csv_new_line = {REQUIRED_ATTRIBUTES[i]: "" for i in range(len(REQUIRED_ATTRIBUTES))}
    csv_file = open_csv_file(csv_new_line)
    global PAGINATION_COUNTER
    while pagination_token is not None:
        try:
            user_records = get_list_cognito_users(
            cognito_idp_client = COGNITO_CLIENT,
            next_pagination_token = pagination_token,
            Limit = LIMIT if LIMIT < MAX_NUMBER_RECORDS else MAX_NUMBER_RECORDS
            )
        except COGNITO_CLIENT.exceptions.ClientError as client_error:
            error_message = client_error.response["Error"]["Message"]
            print( "PLEASE CHECK YOUR COGNITO CONFIGS")
            print("Error Reason: " + error_message)
            csv_file.close()
            exit()
        except Exception as unknown_exception:
            print("Error Reason: " + str(unknown_exception))
            csv_file.close()
            exit()
        pagination_token = check_next_pagination_token_existence(user_records)
        write_to_csv_file(user_records,csv_file, csv_new_line)
        PAGINATION_COUNTER += 1
        cooldown_before_next_batch()
    csv_file.close() 
    save_file()
