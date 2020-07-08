"""
clean up created resources
"""

import json
import boto3
import time
from config import config
from utils import redshift_status

def remove_iam(config):
    '''
    remove iam and policies attached
    :param config:
    :return:
    '''
    iam = boto3.client(
        'iam',
        aws_access_key_id=config['AWS_ACCESS']['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=config['AWS_ACCESS']['AWS_SECRET_ACCESS_KEY'],
        region_name=config['AWS_ACCESS'][ 'AWS_REGION']
    )
    iam_role_name = config['IAM_ROLE']['NAME']
    iam_policy_arn = config['IAM_ROLE']['ARN']

    try:
        iam.detach_role_policy(
            RoleName=iam_role_name,
            PolicyArn=config['IAM_ROLE']['ARN']
        )
        print('etached role policy')
    except Exception as e:
        print('could not remove role policy: ', e)

    try:
        iam.delete_role(
            RoleName=iam_role_name
        )
        print('removed iam role')
    except Exception as e:
        print('could not remove IAM role: ', e)

def delete_redshift_cluster(config):
    '''
    delete cluster
    :param config:
    :return:
    '''
    redshift = boto3.client(
        'redshift',
        aws_access_key_id=config['AWS_ACCESS']['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=config['AWS_ACCESS']['AWS_SECRET_ACCESS_KEY'],
        region_name=config['AWS_ACCESS']['AWS_REGION'],
    )
    try:
        redshift.delete_cluster(
            ClusterIdentifier=config['CLUSTER']['CLUSTER_IDENTIFIER'],
            SkipFinalClusterSnapshot=True
        )
        print('deleted redshift cluster.')
    except Exception as e:
        print('could not delete redshift cluster', e)

    cluster_delete_actioned = time.time()
    while True:
        cluster_status = redshift_status(config, redshift)
        if cluster_status is None:
            print('Cluster is deleted.')
            break
        print('Cluster is', cluster_status['ClusterStatus'])
        time.sleep(5)
        print('delete time', time.time() - cluster_delete_actioned)

def main():
    remove_iam(config)
    delete_redshift_cluster(config)
if __name__ == '__main__':
    main()