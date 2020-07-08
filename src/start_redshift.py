"""start readshit cluster
- create I am role  and attach S3 read access policy
- configure and create redshift cluster
"""

import json
import boto3
import time
from config import config
from utils import redshift_status

def create_iam(config):
    ''''
    create IAM and atache S3 read policy
    return iam_role
    '''

    iam = boto3.client(
        'iam',
        aws_access_key_id=config['AWS_ACCESS']['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=config['AWS_ACCESS']['AWS_SECRET_ACCESS_KEY'],
        region_name=config['AWS_ACCESS']['AWS_REGION']
    )
    iam_role_name = config['IAM_ROLE']['NAME']
    try:
        role = iam.create_role(
            RoleName=iam_role_name,
            Description='Allows Redshift to Access Other AWS Services',
            AssumeRolePolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "redshift.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            })
        )
        print('IAM Role Arn:', role['Role']['Arn'])
    except iam.EntityAlreadyExistsException:
        print('iam role already exists')
        role = iam.get_role(RoleName=iam_role_name)
        print('IAM Role Arn:', role['Role']['Arn'])

    try:
        iam.attach_role_policy(
            RoleName=iam_role_name,
            PolicyArn=config.get('IAM_ROLE', 'ARN')
        )
    except Exception as e:
        raise e
    print('role created, S3 Read-Only policy Attached.')
    return role

def create_redshift_cluster(config, iam_role):
    """
    create readshift cluster
    :return:
    """
    redshift = boto3.client(
        'redshift',
        aws_access_key_id=config['AWS_ACCESS']['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=config['AWS_ACCESS']['AWS_SECRET_ACCESS_KEY'],
        region_name=config['AWS_ACCESS']['AWS_REGION']
    )
    print('creating redshift cluster ...')

    try:
        response = redshift.create_cluster(
            DBName=config['CLUSTER']['DB_NAME'],
            ClusterIdentifier=config['CLUSTER']['CLUSTER_IDENTIFIER'],
            MasterUsername=config['CLUSTER']['DB_USER'],
            MasterUserPassword=config['CLUSTER']['DB_PASSWORD'],
            NodeType=config['CLUSTER']['NODE_TYPE'],
            Port=int(config['CLUSTER']['DB_PORT']),
            IamRoles=[
                iam_role['Role']['Arn']
            ],
            NumberOfNodes=int(config['CLUSTER']['NODE_COUNT'])
        )
        print('Create Cluster Call Made.')
    except Exception as e:
        print('Could not create cluster', e)

    cluster_initiated = time.time()
    status_checked = 0
    while True:
        print('Getting Cluster Status..')
        cluster_status = redshift_status(config, redshift)
        status_checked += 1
        if cluster_status['ClusterStatus'] == 'available':
            break
        print('Cluster Status', cluster_status)
        print('Status Checked', status_checked)
        print('Time Since Initiated', time.time() - cluster_initiated)
        time.sleep(5)
    print('Cluster is created and available.')




def main():
    """
    - create iam role
    - create readshift cluster
    - display cluster details
    :return:
    """
    iam_role = create_iam(config)
    create_redshift_cluster(config, iam_role)

if __name__ == '__main__':
    main()

