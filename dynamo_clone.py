#!/usr/bin/env python

"""
dynamo_clone.py: Clones a DynamoDB table
Copyright 2016, Mark Wetter
This code is distributed under the MIT License
See LICENSE in the project root for details
"""

import argparse
import sys
import boto3
from botocore.exceptions import ClientError


def clone_throughput(source_throughput):
    new_throughput = {}
    new_throughput['ReadCapacityUnits'] = source_throughput['ReadCapacityUnits']
    new_throughput['WriteCapacityUnits'] = source_throughput['WriteCapacityUnits']

    return new_throughput


def clone_indexes(source_index_list):
    new_index_list = []
    for source_index in source_index_list:
        new_index = {}
        new_index['IndexName'] = source_index['IndexName']
        new_index['KeySchema'] = source_index['KeySchema']
        new_index['Projection'] = source_index['Projection']
        new_index['ProvisionedThroughput'] = clone_throughput(source_index['ProvisionedThroughput'])
        new_index_list.append(new_index)

    return(new_index_list)


def main(source_table_name, destination_table_name, region, profile):
    session = boto3.session.Session(region_name=region, profile_name=profile)
    dynamo = session.client('dynamodb')

    try:
        source = dynamo.describe_table(TableName=source_table_name)['Table']
    except ClientError:
        print('Source table "{}" does not exist'.format(source_table_name))
        sys.exit(1)

    try:
        dynamo.describe_table(TableName=destination_table_name)['Table']
        print('Destination table "{}" already exists'.format(destination_table_name))
        sys.exit(1)
    except ClientError:
        pass

    destination = {}

    destination['TableName'] = destination_table_name
    destination['KeySchema'] = source['KeySchema']
    destination['AttributeDefinitions'] = source['AttributeDefinitions']
    destination['ProvisionedThroughput'] = clone_throughput(source['ProvisionedThroughput'])

    if 'LocalSecondaryIndexes' in source:
        destination['LocalSecondaryIndexes'] = clone_indexes(source['LocalSecondaryIndexes'])

    if 'GlobalSecondaryIndexes' in source:
        destination['GlobalSecondaryIndexes'] = clone_indexes(source['GlobalSecondaryIndexes'])

    waiter = dynamo.get_waiter('table_exists')
    print('Provisioning new table {}'.format(destination_table_name))
    response = dynamo.create_table(**destination)
    waiter.wait(TableName=destination_table_name)
    print('Table creation complete')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Clone a DynamoDB Table')
    parser.add_argument('source', help='source table')
    parser.add_argument('dest', help='destination table')
    parser.add_argument('--region', default='us-east-1', help='AWS region to target')
    parser.add_argument('--profile', default='default', help='AWS credential profile to use')
    args = parser.parse_args()

    main(args.source, args.dest, args.region, args.profile)
