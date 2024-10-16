import logging
import requests
import json
from sys import version_info
from ipyparallel import Client


logger = logging.getLogger(__name__)

def cluster_list(gateway='172.19.0.1:8011'):
    r = requests.get(f'http://{gateway}/clusters')
    return (r.json())


def cluster_launch(
    tag,
    instance_count=10,
    spot_price="1.50",
    instance_type="c4.2xlarge",
    ami_id=None,
    key_name="cluster-key",
    security_groups=["ipyparallel-cluster"],
    region="us-west-1",
    availability_zone="us-west-1b",
    num_nodes_per_instance=None,
    soterapy_branch="master",
    nodb=False,
    env=None,
    gateway='172.19.0.1:8011'
):
    r = requests.post(
            f'http://{gateway}/clusters', 
            json={
                'tag':tag,
                'nodb':nodb,
                'env': f"prodpy{version_info[0]}{version_info[1]}" if env is None else env,
                'instance_count': instance_count,
                'spot_price':spot_price,
                'instance_type':instance_type,
                'ami_id': ami_id,
                'key_name': key_name,
                'security_groups': security_groups,
                'region': region,
                'availability_zone': availability_zone,
                'num_nodes_per_instance': num_nodes_per_instance,
                'soterapy_branch': soterapy_branch
            }
        )
    user_data = json.loads(r.json())
    return user_data['name'], user_data


def cluster_get_client(cluster_name, gateway='172.19.0.1:8011'):
    r = requests.get(f'http://{gateway}/clusters/{cluster_name}/profile/client')
    connection_info = r.json()
    return Client(connection_info)


def cluster_terminate(cluster_name, gateway='172.19.0.1:8011'):
    r = requests.delete(f'http://{gateway}/clusters/{cluster_name}')
    return r.json()
