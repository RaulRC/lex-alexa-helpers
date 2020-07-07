import json
import os
import time
import logging
import boto3

DATABASE = 'FILLME'
TABLE = 'FILLME'
BUCKET = 'FILLME'


def checkEvent(event):
    intent = dict()
    intent['name'] = None
    intent['slots'] = dict()

    if 'currentIntent' in event.keys():
        # LEX
        source = 'lex'
        intent['name'] = event['currentIntent']['name']
        intent['slots'] = event['currentIntent']['slots']
        print(intent)
    else:
        # ALEXA
        source = 'alexa'
        intent['name'] = event['request']['type'] if event['request']['type'] == 'LaunchRequest' else \
        event['request']['intent']['name']
        slots = dict()
        all_slots = event['request']['intent']['slots']
        for slot in all_slots:
            print(all_slots[slot])
            if 'value' in all_slots[slot].keys():
                slots[slot] = all_slots[slot]['value']
    intent['slots'] = slots
    return intent, source


def getLexResponse(event, message, end=False):
    response = {
        'sessionAttributes': event['sessionAttributes'],
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': "Fulfilled",
            'message': {'contentType': 'PlainText',
                        'content': message}
        }
    }
    return response


def getAlexaResponse(event, message, end=False):
    return {
        "version": "string",
        "sessionAttributes": {
            "key": "value"
        },
        "response": {
            "outputSpeech": {
                "type": "PlainText",
                "text": message,
                "playBehavior": "REPLACE_ENQUEUED"
            },
            "shouldEndSession": end
        },
        "card": {
            "type": "Standard",
            "title": "Title of the card",
            "text": "Text content for a standard card",
            "image": {
                "smallImageUrl": "https://url-to-small-card-image...",
                "largeImageUrl": "https://url-to-large-card-image..."
            }
        },
        "reprompt": {
            "outputSpeech": {
                "type": "PlainText",
                "text": message,
                "playBehavior": "REPLACE_ENQUEUED"
            }
        },
    }


def queryAthenaCount(intent):
    #TODO COmplete
    # HERE IS WHERE THE MAGIC IS ----------------------------------------------
    COLUMNS_TO_GET = 'COUNT(*)'
    query = 'SELECT {} FROM "{}"."{}" WHERE '.format(COLUMNS_TO_GET, DATABASE, TABLE)
    for slot in intent['slots'].items():
        if slot[1] != None:
            query += "LOWER({}) LIKE '%{}%' AND ".format(slot[0].lower(), slot[1].lower())
    query = query[:-4] + ";"
    print(query)
    # -------------------------------------------------------------------------
    client = boto3.client('athena')
    # Execution
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': DATABASE
        },
        ResultConfiguration={
            'OutputLocation': "s3://{}_queries".format(BUCKET),
        }
    )
    query_execution_id = response['QueryExecutionId']
    print(query_execution_id)

    for i in range(1, 1 + 10):
        # get query execution
        query_status = client.get_query_execution(QueryExecutionId=query_execution_id)
        query_execution_status = query_status['QueryExecution']['Status']['State']
        if query_execution_status == 'SUCCEEDED':
            print("STATUS:" + query_execution_status)
            break
        if query_execution_status == 'FAILED':
            raise Exception("STATUS:" + query_execution_status)
        else:
            print("STATUS:" + query_execution_status)
            time.sleep(i)
    else:
        client.stop_query_execution(QueryExecutionId=query_execution_id)
        raise Exception('TIME OVER')

    # get query results
    result = client.get_query_results(QueryExecutionId=query_execution_id)
    print(result)

    return result['ResultSet']['Rows'][1]['Data'][0]['VarCharValue']


def queryAthena(intent):
    #TODO Complete
    # HERE IS WHERE THE MAGIC IS ----------------------------------------------
    COLUMNS_TO_GET = 'customer_name, customer_website'
    query = 'SELECT {} FROM "{}"."{}" WHERE '.format(COLUMNS_TO_GET, DATABASE, TABLE)
    for slot in intent['slots'].items():
        if slot[1] != None:
            query += "LOWER({}) LIKE '%{}%' AND ".format(slot[0].lower(), slot[1].lower())
    query = query[:-4] + ";"
    # -------------------------------------------------------------------------
    print(query)
    client = boto3.client('athena')
    # Execution
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': DATABASE
        },
        ResultConfiguration={
            'OutputLocation': "s3://{}_queries".format(BUCKET),
        }
    )
    query_execution_id = response['QueryExecutionId']
    print(query_execution_id)

    for i in range(1, 1 + 10):
        # get query execution
        query_status = client.get_query_execution(QueryExecutionId=query_execution_id)
        query_execution_status = query_status['QueryExecution']['Status']['State']
        if query_execution_status == 'SUCCEEDED':
            print("STATUS:" + query_execution_status)
            break
        if query_execution_status == 'FAILED':
            raise Exception("STATUS:" + query_execution_status)
        else:
            print("STATUS:" + query_execution_status)
            time.sleep(i)
    else:
        client.stop_query_execution(QueryExecutionId=query_execution_id)
        raise Exception('TIME OVER')

    # get query results
    result = client.get_query_results(QueryExecutionId=query_execution_id)
    # print(result)
    message = ""
    ## BUILD THE SOLUTION
    # print(result)
    for item in result['ResultSet']['Rows'][1:]:
        message += item['Data'][0]['VarCharValue'] + " at " + item['Data'][1]['VarCharValue'] + ".\n"
    if not message:
        message = "No values found."
    # print("Final message: {}".format(message))
    return message


def handleIntent(intent, event, source='lex'):
    if intent['name'] == 'CountIntent':
        resp = "I've found {} customers".format(queryAthenaCount(intent))
    elif intent['name'] == 'SearchIntent':
        resp = queryAthena(intent)

    # ALEXA
    elif intent['name'] == 'LaunchRequest':
        resp = "Welcome to your customer bot. How can I help?"
    elif intent['name'] == 'AMAZON.StopIntent':
        resp = 'See you!'

    else:
        resp = "Sorry, I did not understand."

    # Build final response:
    if source == 'lex':
        response = getLexResponse(event, resp)
    else:
        response = getAlexaResponse(event, resp)
    return response


def lambda_handler(event, context):
    intent, source = checkEvent(event)
    response = handleIntent(intent, event, source)
    # TODO implement
    return response
