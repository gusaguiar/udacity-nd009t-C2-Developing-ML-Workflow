''' Function is responsible for data generation. ''' 

import json
import boto3
import base64

s3 = boto3.client('s3')
s3_bucket = boto3.resource('s3')

import botocore # to raise errors

def lambda_handler(event, context):
    """A function to serialize target data from S3"""
    
    # Get the s3 address from the Step Function event input
    key = event['s3_key']
    bucket = event['s3_bucket']
    
    # Download the data from s3 to /tmp/image.png
    try:
        s3_bucket.Bucket(bucket).download_file(key, '/tmp/image.png')
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise
    
    # We read the data from a file
    with open("/tmp/image.png", "rb") as f:
        image_data = base64.b64encode(f.read())

    # Pass the data back to the Step Function
    print("Event:", event.keys())
    return {
        'statusCode': 200,
        'body': {
            "image_data": image_data,
            "s3_bucket": bucket,
            "s3_key": key,
            "inferences": []
        }
    }

''' Function is responsible for image classification. And the third '''

import json
import base64
import boto3

# Fill this in with the name of your deployed model
ENDPOINT = "image-classification-2023-01-01-21-28-45-483"
runtime = boto3.Session().client('sagemaker-runtime')

def lambda_handler(event, context):

    # Decode the image data
    image = base64.b64decode(event['image_data'])
 
    # Make a prediction:
    response = runtime.invoke_endpoint(EndpointName=ENDPOINT,
                                       ContentType='image/png',
                                       Body=image)
    
    # We return the data back to the Step Function    
    event["inferences"] = json.loads(response['Body'].read().decode('utf-8'))
    return {
        'statusCode': 200,
        'body': event
    }

''' Function is responsible for filtering out low-confidence inferences. '''

import json


THRESHOLD = .80


def lambda_handler(event, context):
    
    # Get the inferences from the event
    inferences = event["inferences"]
    
    # Check if any values in any inferences are above THRESHOLD
    meets_threshold = (max(inferences) > THRESHOLD)
    
    # If our threshold is met, pass our data back out of the
    # Step Function, else, end the Step Function with an error
    if meets_threshold:
        pass
    else:
        raise("THRESHOLD_CONFIDENCE_NOT_MET")

    return {
        'statusCode': 200,
        'body': json.dumps(event)
    }