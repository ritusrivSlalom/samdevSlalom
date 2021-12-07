import logging
import jwt
import os

def authorizer(event, context):
    token = event['authorizationToken']
    methodArn = event['methodArn']

    if (token == 'allow'):
        return generateAuthResponse('user', 'Allow', methodArn)
    else:
        return generateAuthResponse('user', 'Deny', methodArn)


def generateAuthResponse(principalId, effect, methodArn):
    policyDocument = generatePolicyDocument(effect, methodArn)

    return (principalId,policyDocument)


def generatePolicyDocument(effect, methodArn):
    policyDocument = '''{
        Version: '2012-10-17',
        Statement: [{
            Action: 'execute-api:Invoke',
            Effect: '''+ effect+''',
            Resource: '''+ methodArn+'''
        }]
    };
    '''
    return policyDocument
