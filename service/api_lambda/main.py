import json

def handler(event, context):
     s_event = str(event)
     s_context = str(context)

     params = {}
     required_params = ["master_instance_type"]

     missing_args_response = {
                                'statusCode': 400,
                                'body': 'Missing arguments.',
                                'headers': {'Content-Type': 'application/json'}
                             }

     if 'queryStringParameters' in event:
          qstring = event['queryStringParameters']

          for rq in required_params:
               if rq not in qstring:
                    return missing_args_response

          for k in event['queryStringParameters']:
               params[k] = event['queryStringParameters'][k]
     else:
          return missing_args_response

     return {
          'statusCode': 200,
          'body': json.dumps({"Response": [f"Event -> {s_event}", f"Context -> {s_context}"]}),
          'headers': {'Content-Type': 'application/json'},
     }