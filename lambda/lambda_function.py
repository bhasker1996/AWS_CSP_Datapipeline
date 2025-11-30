import json
import uuid
import boto3
import os
import botocore
import botocore.session as bc
from botocore.client import Config
import time
# import pandas as pd
import io
import csv



GET_ALL_TOOLS_PATH = "/csp-tooling-lambda1/getTools"
CREATE_RAW_PATH = "/csp-tooling-lambda1/createTool"
UPDATE_RAW_PATH = "/csp-tooling-lambda1/updateTool"
DELETE_RAW_PATH = "/csp-tooling-lambda1/deleteTool"


print('Loading function')
secret_name = os.environ['SecretId']


def retrieve_data(redshift_client, cluster_id, database, schema_name,table_name, secret_arn):
    try:

        print("Inside retrieve data method. ")
        # SQL query
        # query = f"SELECT *, is_display FROM {schema_name}.{table_name};"
        query = f"SELECT * FROM {schema_name}.{table_name} WHERE is_display = TRUE;"
        
        # Execute the query
        response = redshift_client.execute_statement(
            ClusterIdentifier=cluster_id,
            Database=database,
            SecretArn=secret_arn,
            Sql=query
        )
        
        # Get query result
        statement_id = response['Id']
        all_records = []
        
        # Wait for query completion
        while True:
            status_response = redshift_client.describe_statement(Id=statement_id)
            status = status_response['Status']
            print(f"Query Status: {status}")
            
            if status == 'FINISHED':
                # Get initial results
                result = redshift_client.get_statement_result(Id=statement_id)
                
                # Get column names once
                columns = [meta['name'] for meta in result['ColumnMetadata']]
                print(f"Columns: {columns}")

                # Add this debug information
                print("Column Metadata details:")
                for meta in result['ColumnMetadata']:
                    print(f"Column: {meta['name']}, Type: {meta.get('typeName')}")
                
                # Process all pages of results
                while True:
                    # Process current page
                    for row in result['Records']:
                        record = {}
                        for i, value in enumerate(row):
                            # Handle different data types
                            if 'stringValue' in value:
                                record[columns[i]] = value['stringValue']
                            elif 'longValue' in value:
                                record[columns[i]] = value['longValue']
                            elif 'doubleValue' in value:
                                record[columns[i]] = value['doubleValue']
                            elif 'booleanValue' in value:  # Add this case
                                record[columns[i]] = value['booleanValue']
                            elif 'isNull' in value:
                                record[columns[i]] = None
                        all_records.append(record)
                    
                    # Check if there are more pages
                    if 'NextToken' in result:
                        # Get next page
                        result = redshift_client.get_statement_result(
                            Id=statement_id,
                            NextToken=result['NextToken']
                        )
                    else:
                        break
                
                print(f"Total records retrieved: {len(all_records)}")
                
                # Convert to JSON and format it nicely
                formatted_json = json.dumps(
                    {
                        'total_count': len(all_records),
                        'records': all_records
                    },
                    indent=2
                )
                
                print("Full JSON output:")
                print(formatted_json)
                
                return {
                    'statusCode': 200,
                    'body': formatted_json,
                    'headers': {
                        'Content-Type': 'application/json'
                    }
                }
                
            elif status in ['FAILED', 'ABORTED']:
                error_message = f"Query failed: {status_response.get('Error', 'Unknown error')}"
                print(error_message)
                raise Exception(error_message)
            
            time.sleep(1)  # Wait before checking again

    except Exception as e:
        error_message = f"Error: {str(e)}"
        print(error_message)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': error_message
            }),
            'headers': {
                'Content-Type': 'application/json'
            }
        }



def create_redshift_client(access_key, secret_key, session_token, region):
    return boto3.client(
        'redshift-data',
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key,
        aws_session_token = session_token,
        region_name= region
    )


def assume_role(role_arn, session_name):
    sts_client = boto3.client('sts')
    response = sts_client.assume_role(
        RoleArn=role_arn,
        RoleSessionName=session_name
    )
    return response['Credentials']



def get_secret(secret_name, access_key, secret_key, session_token, region):
    secrets_client = boto3.client(
        'secretsmanager',
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key,
        aws_session_token = session_token,
        region_name = region
    )
    response = secrets_client.get_secret_value(SecretId=secret_name)
    return response


def check_tool_exists(redshift_client, cluster_id, database, schema_name, table_name, secret_arn, tool_name):
    try:
        # SQL query to check if tool_name exists
        query = f"""
            SELECT EXISTS (
                SELECT 1 
                FROM {schema_name}.{table_name} 
                WHERE tool_name = '{tool_name}'
            );
        """
        
        # Execute the query
        response = redshift_client.execute_statement(
            ClusterIdentifier=cluster_id,
            Database=database,
            SecretArn=secret_arn,
            Sql=query
        )
        
        # Get query result
        statement_id = response['Id']
        
        # Wait for query completion
        while True:
            status_response = redshift_client.describe_statement(Id=statement_id)
            status = status_response['Status']
            
            if status == 'FINISHED':
                result = redshift_client.get_statement_result(Id=statement_id)
                # The result will be a boolean value
                exists = result['Records'][0][0]['booleanValue']
                return exists
                
            elif status in ['FAILED', 'ABORTED']:
                error_message = f"Query failed: {status_response.get('Error', 'Unknown error')}"
                print(error_message)
                raise Exception(error_message)
            
            time.sleep(1)

    except Exception as e:
        print(f"Error checking tool existence: {str(e)}")
        raise



def wait_for_query(redshift_client, statement_id, query_name):
    """Helper function with detailed waiting logic"""
    start_time = time.time()
    timeout = 30  # Reduced timeout for debugging
    last_status = None
    
    while time.time() - start_time < timeout:
        try:
            status_response = redshift_client.describe_statement(Id=statement_id)
            current_status = status_response['Status']
            
            if current_status != last_status:
                print(f"{query_name} status changed to: {current_status}")
                last_status = current_status
                
            if current_status == 'FINISHED':
                result = redshift_client.get_statement_result(Id=statement_id)
                return True, result
            elif current_status in ['FAILED', 'ABORTED']:
                error = status_response.get('Error', 'No error details')
                return False, error
                
            time.sleep(1)
        except Exception as e:
            print(f"Error checking {query_name} status: {str(e)}")
            return False, str(e)
    
    return False, f"Timeout waiting for {query_name}"



def escape_sql_value(value):
    if value == "NA" or value == "" or value is None:
        return 'NULL'
    elif isinstance(value, bool):
        return 'TRUE' if value else 'FALSE'
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    else:
        return str(value)  # fallback for other types

def insert_tool_data(redshift_client, cluster_id, database, schema_name, table_name, secret_arn, request_body):
    try:
        # Prepare the column names and properly escaped values
        columns = list(request_body.keys())
        values = [escape_sql_value(value) for value in request_body.values()]
        values_str = ", ".join(values)

        # Transaction with separate INSERT and SELECT queries
        insert_query = f"""
BEGIN;
LOCK TABLE {schema_name}.{table_name} IN EXCLUSIVE MODE;
INSERT INTO {schema_name}.{table_name} (s_no, {", ".join(columns)})
SELECT COALESCE(MAX(s_no), 0) + 1, {values_str}
FROM {schema_name}.{table_name};
COMMIT;
"""

        # First execute the INSERT transaction
        print(f"Insert Query: {insert_query}")
        insert_response = redshift_client.execute_statement(
            ClusterIdentifier=cluster_id,
            Database=database,
            SecretArn=secret_arn,
            Sql=insert_query
        )
        
        # Wait for INSERT to complete
        insert_statement_id = insert_response["Id"]
        while True:
            status_response = redshift_client.describe_statement(Id=insert_statement_id)
            status = status_response["Status"]
            
            if status == "FINISHED":
                break
            elif status in ["FAILED", "ABORTED"]:
                error_message = f"Insert query failed: {status_response.get('Error', 'Unknown error')}"
                print(error_message)
                return False, None, error_message
            time.sleep(1)

        # Now execute a separate SELECT to get the max s_no
        select_query = f"SELECT MAX(s_no) FROM {schema_name}.{table_name};"
        print(f"Select Query: {select_query}")
        select_response = redshift_client.execute_statement(
            ClusterIdentifier=cluster_id,
            Database=database,
            SecretArn=secret_arn,
            Sql=select_query
        )
        
        # Wait for SELECT to complete
        select_statement_id = select_response["Id"]
        while True:
            status_response = redshift_client.describe_statement(Id=select_statement_id)
            status = status_response["Status"]
            
            if status == "FINISHED":
                try:
                    result = redshift_client.get_statement_result(Id=select_statement_id)
                    if result.get('Records') and len(result['Records']) > 0:
                        new_s_no = int(result['Records'][0][0]['longValue'])
                        return True, new_s_no, None
                    else:
                        return False, None, "Table is empty after insert"
                except Exception as e:
                    return False, None, f"Failed to get result: {str(e)}"
            elif status in ["FAILED", "ABORTED"]:
                error_message = f"Select query failed: {status_response.get('Error', 'Unknown error')}"
                print(error_message)
                return False, None, error_message
            time.sleep(1)
            
    except Exception as e:
        error_message = f"Unexpected error: {str(e)}"
        print(f"Error in insert_tool_data: {error_message}")
        return False, None, error_message




def check_And_Insert(redshift_client, cluster_id, database, schema_name,table_name, secret_arn, tool_exists, tool_name, request_body):

    try:

        if tool_exists:
            return {
                "statusCode": 400,
                "body": json.dumps(
                    {
                        "message": f'Tool name "{tool_name}" already exists in database',
                        "exists": True,
                    }
                ),
                "headers": {"Content-Type": "application/json"},
            }
        else:
            # If tool doesn't exist, proceed with insertion
            # redshift_client, cluster_id, database, schema_name, table_name, secret_arn, tool_data
            insert_success = insert_tool_data(
                redshift_client,
                cluster_id,
                database,
                schema_name,
                table_name,
                secret_arn,
                request_body,  # Passing the entire request body as tool data
            )
        
            if insert_success:
                return {
                    "statusCode": 201,
                    "body": json.dumps(
                        {
                            "message": f'Tool "{tool_name}" successfully inserted',
                            "tool_data": request_body,
                        }
                    ),
                    "headers": {"Content-Type": "application/json"},
                }
            else:
                return {
                    "statusCode": 500,
                    "body": json.dumps({"error": "Failed to insert tool data"}),
                    "headers": {"Content-Type": "application/json"},
                }

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            }),
            'headers': {
                'Content-Type': 'application/json'
            }
        }


def update_tool_data(redshift_client, cluster_id, database, schema_name,table_name, secret_arn, tool_data):
    try:

        # tool_name = tool_data.get("tool_name")
        s_no = tool_data.get("s_no")

        # Remove tool_name from the data to be updated (since it's our primary key)
        update_data = {k: v for k, v in tool_data.items() if k != "s_no"}

        if not update_data:
            raise ValueError("No fields provided for update")

        # Construct the SET clause for UPDATE statement
        set_clause = ", ".join(
            [
                (
                    f"{key} = '{value}'"
                    if isinstance(value, str)
                    else f"{key} = {value}"
                )
                for key, value in update_data.items()
            ]
        )

        # Construct and execute UPDATE query
        query = f"""
                UPDATE {schema_name}.{table_name}
                SET {set_clause}
                WHERE s_no = {s_no};
            """

        print(f"Update Query: {query}")  # For debugging

        # Execute the query
        response = redshift_client.execute_statement(
            ClusterIdentifier=cluster_id,
            Database=database,
            SecretArn=secret_arn,
            Sql=query,
        )

        # Wait for query completion
        statement_id = response["Id"]
        while True:
            status_response = redshift_client.describe_statement(Id=statement_id)
            status = status_response["Status"]

            if status == "FINISHED":
                # Check if any rows were updated
                # result = redshift_client.get_statement_result(Id=statement_id)
                # rows_updated = result.get("UpdatedRows", 0)
                # return rows_updated > 0
                return True

            elif status in ["FAILED", "ABORTED"]:
                error_message = f"Update query failed: {status_response.get('Error', 'Unknown error')}"
                print(error_message)
                raise Exception(error_message)

            time.sleep(1)

    except Exception as e:
        print(f"Error updating tool data: {str(e)}")
        raise



def check_And_Update(redshift_client, cluster_id, database, schema_name,table_name, secret_arn, tool_exists, s_no, request_body):
    try:
        if not tool_exists:
            return {
                "statusCode": 404,
                "body": json.dumps(
                    {"message": f'Record with s_no "{s_no}" does not exist in database'}
                ),
                "headers": {"Content-Type": "application/json"},
            }

        # If tool exists, proceed with update
        update_success = update_tool_data(
            redshift_client,
            cluster_id,
            database,
            schema_name,
            table_name,
            secret_arn,
            request_body,
        )

        # if update_success:
        #     return {
        #         "statusCode": 200,
        #         "body": json.dumps(
        #             {
        #                 "message": f'Tool "{tool_name}" successfully updated',
        #                 "updated_data": request_body,
        #             }
        #         ),
        #         "headers": {"Content-Type": "application/json"},
        #     }
        # else:
        #     return {
        #         "statusCode": 500,
        #         "body": json.dumps({"error": "Failed to update tool data"}),
        #         "headers": {"Content-Type": "application/json"},
        #     }

        if update_success:
            return {
                "statusCode": 200,
                "body": json.dumps(
                    {
                        "message": f'Record with s_no "{s_no}" successfully updated',
                        "updated_data": request_body,
                    }
                ),
                "headers": {"Content-Type": "application/json"},
            }
        else:
            return {
                "statusCode": 500,
                "body": json.dumps({"error": "Failed to update record"}),
                "headers": {"Content-Type": "application/json"},
            }

    except ValueError as ve:
        return {
            'statusCode': 400,
            'body': json.dumps({
                'error': str(ve)
            }),
            'headers': {
                'Content-Type': 'application/json'
            }
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            }),
            'headers': {
                'Content-Type': 'application/json'
            }
        }


def soft_delete_tool(redshift_client, cluster_id, database, schema_name, table_name, secret_arn, s_no):
    # print(" Inside soft_delete_tool ")
    try:
        # Construct and execute UPDATE query for soft delete
        query = f"""
            UPDATE {schema_name}.{table_name}
            SET is_display = FALSE
            WHERE s_no = {s_no};
        """
        
        print(f"Soft Delete Query: {query}")  # For debugging
        
        # Execute the query
        response = redshift_client.execute_statement(
            ClusterIdentifier=cluster_id,
            Database=database,
            SecretArn=secret_arn,
            Sql=query
        )
        
        # Wait for query completion
        statement_id = response['Id']
        while True:
            status_response = redshift_client.describe_statement(Id=statement_id)
            status = status_response['Status']
            # print(" Inside while True for delete")
            # print("Status : ", status)
            if status == 'FINISHED':
                # Check if any rows were updated
                # result = redshift_client.get_statement_result(Id=statement_id)
                # rows_updated = result.get('UpdatedRows', 0)
                # return rows_updated > 0
                return True
                
            elif status in ['FAILED', 'ABORTED']:
                error_message = f"Soft delete query failed: {status_response.get('Error', 'Unknown error')}"
                print(error_message)
                raise Exception(error_message)
            
            time.sleep(1)

    except Exception as e:
        print(f"Error soft deleting tool: {str(e)}")
        raise


def check_And_Delete(redshift_client, cluster_id, database, schema_name,table_name, secret_arn, tool_exists, s_no, request_body):
    # print(" Inside Check and delete function ")
    try:
        if not tool_exists:
            return {
                "statusCode": 404,
                "body": json.dumps(
                    {
                        "message": f'Record with s_no "{s_no}" does not exist in database'
                    }
                ),
                "headers": {"Content-Type": "application/json"},
            }

        # If tool exists, proceed with soft delete
        delete_success = soft_delete_tool(
            redshift_client,
            cluster_id,
            database,
            schema_name,
            table_name,
            secret_arn,
            s_no,
        )

        # print(" Delete_Success printing : ", delete_success)

        if delete_success:
            return {
                "statusCode": 200,
                "body": json.dumps(
                    {
                        "message": f'Record with s_no "{s_no}" successfully marked as deleted'
                    }
                ),
                "headers": {"Content-Type": "application/json"},
            }
        else:
            return {
                "statusCode": 500,
                "body": json.dumps({"error": "Failed to delete tool"}),
                "headers": {"Content-Type": "application/json"},
            }

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback

        print(f"Traceback: {traceback.format_exc()}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
            "headers": {"Content-Type": "application/json"},
        }

def check_s_no_exists(redshift_client, cluster_id, database, schema_name, table_name, secret_arn, s_no):
    try:
        query = f"""
            SELECT EXISTS (
                SELECT 1 
                FROM {schema_name}.{table_name} 
                WHERE s_no = {s_no}
            );
        """
        
        response = redshift_client.execute_statement(
            ClusterIdentifier=cluster_id,
            Database=database,
            SecretArn=secret_arn,
            Sql=query
        )
        
        statement_id = response['Id']
        while True:
            status_response = redshift_client.describe_statement(Id=statement_id)
            status = status_response['Status']
            
            if status == 'FINISHED':
                result = redshift_client.get_statement_result(Id=statement_id)
                exists = result['Records'][0][0]['booleanValue']
                return exists
                
            elif status in ['FAILED', 'ABORTED']:
                raise Exception(f"Query failed: {status_response.get('Error', 'Unknown error')}")
            
            time.sleep(1)

    except Exception as e:
        print(f"Error checking s_no existence: {str(e)}")
        raise


def get_tool_by_s_no(redshift_client, cluster_id, database, schema_name, table_name, secret_arn, s_no):
    try:
        # SQL query to get specific tool
        query = f"""
            SELECT * 
            FROM {schema_name}.{table_name} 
            WHERE s_no = {s_no} AND is_display = TRUE;
        """
        
        # Execute the query
        response = redshift_client.execute_statement(
            ClusterIdentifier=cluster_id,
            Database=database,
            SecretArn=secret_arn,
            Sql=query
        )
        
        # Get query result
        statement_id = response['Id']
        
        while True:
            status_response = redshift_client.describe_statement(Id=statement_id)
            status = status_response['Status']
            
            if status == 'FINISHED':
                result = redshift_client.get_statement_result(Id=statement_id)
                
                # Get column names
                columns = [meta['name'] for meta in result['ColumnMetadata']]
                
                if result['Records']:
                    # Convert the first record to a dictionary
                    record = {}
                    for i, value in enumerate(result['Records'][0]):
                        if 'stringValue' in value:
                            record[columns[i]] = value['stringValue']
                        elif 'longValue' in value:
                            record[columns[i]] = value['longValue']
                        elif 'doubleValue' in value:
                            record[columns[i]] = value['doubleValue']
                        elif 'booleanValue' in value:
                            record[columns[i]] = value['booleanValue']
                        elif 'isNull' in value:
                            record[columns[i]] = None
                    
                    return {
                        'statusCode': 200,
                        'body': json.dumps(record, default=str),
                        'headers': {
                            'Content-Type': 'application/json'
                        }
                    }
                else:
                    return {
                        'statusCode': 404,
                        'body': json.dumps({
                            'message': f'No record found with s_no: {s_no}'
                        }),
                        'headers': {
                            'Content-Type': 'application/json'
                        }
                    }
                
            elif status in ['FAILED', 'ABORTED']:
                raise Exception(f"Query failed: {status_response.get('Error', 'Unknown error')}")
            
            time.sleep(1)

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            }),
            'headers': {
                'Content-Type': 'application/json'
            }
        }


def get_tools_by_login(redshift_client, cluster_id, database, schema_name, table_name, secret_arn, login):
    try:
        # SQL query to get tools for a specific login
        query = f"""
            SELECT * 
            FROM {schema_name}.{table_name} 
            WHERE login = '{login}' AND is_display = TRUE;
        """
        
        # Execute the query
        response = redshift_client.execute_statement(
            ClusterIdentifier=cluster_id,
            Database=database,
            SecretArn=secret_arn,
            Sql=query
        )
        
        # Get query result
        statement_id = response['Id']
        
        while True:
            status_response = redshift_client.describe_statement(Id=statement_id)
            status = status_response['Status']
            
            if status == 'FINISHED':
                result = redshift_client.get_statement_result(Id=statement_id)
                
                # Get column names
                columns = [meta['name'] for meta in result['ColumnMetadata']]
                
                # Convert all records to a list of dictionaries
                records = []
                for row in result['Records']:
                    record = {}
                    for i, value in enumerate(row):
                        if 'stringValue' in value:
                            record[columns[i]] = value['stringValue']
                        elif 'longValue' in value:
                            record[columns[i]] = value['longValue']
                        elif 'doubleValue' in value:
                            record[columns[i]] = value['doubleValue']
                        elif 'booleanValue' in value:
                            record[columns[i]] = value['booleanValue']
                        elif 'isNull' in value:
                            record[columns[i]] = None
                    records.append(record)
                
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'total_count': len(records),
                        'records': records
                    }, default=str),
                    'headers': {
                        'Content-Type': 'application/json'
                    }
                }
                
            elif status in ['FAILED', 'ABORTED']:
                raise Exception(f"Query failed: {status_response.get('Error', 'Unknown error')}")
            
            time.sleep(1)

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            }),
            'headers': {
                'Content-Type': 'application/json'
            }
        }



def lambda_handler(event, context):
    # TODO implement

    print("Event ====>>>>> ", event)

    # print(event)
    # if event['rawPath'] ==  GET_RAW_PATH:
    #     #GetPerson Path
    #     print("Start request for GetPerson")
    #     personId = event['queryStringParameters']['personid']
    #     print("Received request with personid = "+ personId)
    #     return {"firstName":"Daniel"}

    # elif event['rawPath'] == CREATE_RAW_PATH:
    #     #createPerson Path - write to database
    #     print("Start request for CreatePerson")
    #     decodedEvent = json.loads(event['body'])
    #     firstName = decodedEvent['firstName']
    #     print("Received request with firstName = "+ firstName)
    #     #call database
    #     return {
    #         "personId" : str(uuid.uuid1())
    #     }

    # return {
    #     'statusCode': 200,
    #     'body': json.dumps('Hello from Lambda!')
    # }

    
    try:
        
        # Assume role to get credentials for accessing Redshift
        redshift_region = os.environ.get('REDSHIFT_REGION', 'us-east-1')
        Role_Arn = os.environ['Role_Arn']
        session_name = "cross_acct_lambda"

        print("redshift_region  ==> ", redshift_region)
        print("Role_Arn  ==> ", Role_Arn)
        

        Credentials = assume_role(Role_Arn, session_name)
        access_key = Credentials['AccessKeyId']
        secret_key = Credentials['SecretAccessKey']
        session_token = Credentials['SessionToken']

        print("access_key ==> ", access_key)
        print("secret_key ==> ", secret_key)
        print("session_token ==> ", session_token)


        # Fetch RedShift credentials from Secrets Manager
        secret_name = os.environ["SecretId"]
        print("secret_name ==> ", secret_name)


        response = get_secret(secret_name, access_key, secret_key, session_token, redshift_region)
        secret_arn = response['ARN']
        print("secret_arn ==> ", secret_arn)

        secret = response['SecretString']
        print("secret ==> ", secret)

        secret_json = json.loads(secret)
        print("secret_json  ==> ", secret_json)


        # Initialize Redshift client
        cluster_id = secret_json['dbClusterIdentifier']
        print("Cluster_id ==> ", cluster_id)
        database = secret_json['dbname']
        print("database name => ", database)
        table = os.environ["REDSHIFT_TABLE_NAME"]
        print("table ==> ", table)


        redshift_client= create_redshift_client(access_key, secret_key, session_token, redshift_region)
        print(redshift_client)

        # Create and execute INSERT statement
        schema_name = os.environ['SCHEMA_NAME']
        table_name = os.environ['REDSHIFT_TABLE_NAME']

        # print("Schema name ===> ", schema_name)
        # print("Table name ===> ", table_name)


        # Ended redshift logic, checking for the APIs ==========>
        print("raw path : ", event['rawPath'])
        if event['rawPath'] == GET_ALL_TOOLS_PATH:
            query_parameters = event.get('queryStringParameters', {})
            
            if 's_no' in query_parameters:
                s_no = query_parameters['s_no']
                print(f"Request type: Get specific tool with s_no {s_no}")
                return get_tool_by_s_no(
                    redshift_client,
                    cluster_id,
                    database,
                    schema_name,
                    table_name,
                    secret_arn,
                    s_no
                )
            elif 'login' in query_parameters:
                login = query_parameters['login']
                print(f"Request type: Get tools for login {login}")
                return get_tools_by_login(
                    redshift_client,
                    cluster_id,
                    database,
                    schema_name,
                    table_name,
                    secret_arn,
                    login
                )
            else:
                print("Request type: Get all tools")
                return retrieve_data(
                    redshift_client,
                    cluster_id,
                    database,
                    schema_name,
                    table_name,
                    secret_arn
                )

            # return retrieve_data(redshift_client, cluster_id, database, schema_name,table_name, secret_arn)

        request_body = json.loads(event['body'])
        # tool_name = request_body.get('tool_name')

        # print("request body",  request_body)
        # print("tool_name", tool_name)

        # # Validate tool_name presence
        # if not tool_name:
        #     return {
        #         'statusCode': 400,
        #         'body': json.dumps({
        #             'error': 'tool_name is required in the request'
        #         }),
        #         'headers': {
        #             'Content-Type': 'application/json'
        #         }
        #     }
            
               
        # # Check if tool already exists
        # tool_exists = check_tool_exists(
        #     redshift_client, 
        #     cluster_id, 
        #     database, 
        #     schema_name,
        #     table_name, 
        #     secret_arn,
        #     tool_name
        # )

        # print("tool_exists result from the lambda handler : ", tool_exists)

        if event['rawPath'] == CREATE_RAW_PATH:
            success, new_s_no, error_message = insert_tool_data(redshift_client, cluster_id, database, schema_name,table_name, secret_arn, request_body) # tool_exists, tool_name, request_body)

            if success:
                return {
                    "statusCode": 201,
                    "body": json.dumps(
                        {
                            "message": "Tool successfully created",
                            "s_no": new_s_no,
                            "data": request_body,
                        }
                    ),
                    "headers": {"Content-Type": "application/json"},
                }
            else:
                return {
                    "statusCode": 400,
                    "body": json.dumps(
                        {"message": "Failed to create tool", "error": error_message}
                    ),
                    "headers": {"Content-Type": "application/json"},
                }


        s_no = request_body.get('s_no')
        if not s_no and event['rawPath'] in [UPDATE_RAW_PATH, DELETE_RAW_PATH]:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 's_no is required in the request'}),
                'headers': {'Content-Type': 'application/json'}
            }

        tool_exists = check_s_no_exists(redshift_client, cluster_id, database, schema_name, table_name, secret_arn, s_no)

        
        if event['rawPath'] == UPDATE_RAW_PATH:
            return check_And_Update(redshift_client, cluster_id, database, schema_name,table_name, secret_arn, tool_exists, s_no, request_body)
        if event['rawPath'] == DELETE_RAW_PATH:
            print(" Delete Request ")
            return check_And_Delete(redshift_client, cluster_id, database, schema_name,table_name, secret_arn, tool_exists, s_no, request_body)


    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        raise



    return None
