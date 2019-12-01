from __future__ import print_function
import boto3
import base64
from json import loads

client = boto3.client('sns')
# Include your SNS topic ARN here.
topic_arn = 'Add your topic SRN here'


def lambda_handler(event, context):
    output = []
    success = 0
    failure = 0
    for record in event['records']:
        try:
            
            payload = base64.b64decode(record['data'])
            data_item = loads(payload)
            #print (data_item)
            
            if (data_item['ANOMALY_SCORE']) > 2:
                client.publish(TopicArn=topic_arn, Message=payload, Subject='Anomaly Detected!')
                print ('Anomaly Detected')
            output.append({'recordId': record['recordId'], 'result': 'Ok'})
            success += 1
        except Exception:
            output.append({'recordId': record['recordId'], 'result': 'DeliveryFailed'})
            failure += 1

    print('Successfully delivered {0} records, failed to deliver {1} records'.format(success, failure))
    return {'records': output}
