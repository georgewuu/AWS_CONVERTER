import sys
from getpass import getuser
import json
from time import sleep
from uuid import uuid4
from pathlib import Path
from tempfile import TemporaryDirectory
from ssh_config import SSHConfig, Host
from jupyter_client.kernelspec import KernelSpecManager
from sotera.aws.ipcluster import (
    cluster_add_instances,
    cluster_terminate_spots,
    cluster_get_instances,
)
from sys import prefix, version_info


def remote_instance_launch(
    spot_price="1.50",
    instance_type="c4.2xlarge",
    ami_id=None,
    key_name="cluster-key",
    security_groups=["ipyparallel-cluster"],
    region="us-west-1",
    availability_zone="us-west-1b",
):

    cluster_name = "{}-{}".format("remote", uuid4())

    user_data = json.dumps(
        {
            "cluster_type": "remote-notebook-kernel",
            "name": cluster_name,
            "instance_type": instance_type,
            "instance_count": "1",
            "num_nodes_per_instance": "1",
            "owner": getuser(),
            "env": f"prodpy{version_info[0]}{version_info[1]}"
        }
    )

    requests, user_data = cluster_add_instances(
        user_data,
        instance_count=1,
        spot_price="1.50",
        instance_type=instance_type,
        ami_id=ami_id,
        key_name=key_name,
        security_groups=security_groups,
        region=region,
        availability_zone=availability_zone,
        num_nodes_per_instance=1,
    )
    return cluster_name, requests, user_data


def setup_remote(remote, instance):
    python_exe = Path(sys.executable)
    ksm = KernelSpecManager()

    try:
        ksm.remove_kernel_spec(remote)
    except KeyError:
        pass
    finally:
        with TemporaryDirectory() as fldr:
            with open(Path(fldr) / "kernel.json", "w+") as fp:
                j = json.loads(
                    """{{"argv": [ "{python_exe}", "-m", "remote_ikernel",
"--interface","ssh","--host","{remote}-kernel","--workdir","/home/ec2-user",
"--kernel_cmd","/home/ec2-user/.pyenv/versions/prodpy37/bin/ipython kernel \
-f {{host_connection_file}}","{{connection_file}}"],"display_name": "{remote}",
"language": "python","remote_ikernel_argv": [
"/home/ec1-user/.pyenv/versions/prodpy37/bin/remote_ikernel","manage", "--add",
"--kernel_cmd=/home/ec2-user/.pyenv/versions/prodpy37/bin/ipython kernel \
-f {{connection_file}}","--name","{remote}","--interface=ssh",
"--host={remote}-kernel","--workdir=/home/ec2-user",
"--language=python"]}}""".format(
                        python_exe=python_exe, remote=remote
                    )
                )
                json.dump(j, indent=4, fp=fp)
            ksm.install_kernel_spec(fldr, kernel_name=remote, user=True)

    ssh_config = SSHConfig.load(Path("~/.ssh/config").expanduser())
    host = Host(
        f"{remote}-kernel",
        {
            "HostName": instance.private_ip_address,
            "Port": "22",
            "User": "ec2-user",
            "IdentityFile": "~/.ssh/cluster-key.pem",
            "ServerAliveInterval": 60,
            "Compression": "yes",
            "PasswordAuthentication": "no",
            "ControlMaster": "auto",
            "ControlPath": "~/.ssh/%r@%h:%p",
            "ControlPersist": 1,
        },
    )
    try:
        ssh_config.get(f"{remote}-kernel")
    except KeyError:
        ssh_config.append(host)
    else:
        ssh_config.update(f"{remote}-kernel", host.attributes())
    ssh_config.write(Path("~/.ssh/config").expanduser())
    return remote


def remote_notebook_launch(
    instance_type="c4.2xlarge",
    spot_price="1.50",
    region="us-west-1",
    availability_zone="us-west-1b",
):
    cluster_name, _, _ = remote_instance_launch(
        instance_type=instance_type,
        spot_price=spot_price,
        region=region,
        availability_zone=availability_zone,
    )
    instances = cluster_get_instances(cluster_name)
    while len(instances) == 0:
        sleep(2)
        instances = cluster_get_instances(cluster_name)
    remote = "-".join(cluster_name.split("-")[:2])
    instance = instances[0]
    return setup_remote(remote, instance)


def remote_notebook_find_all():
    names = []
    instances = cluster_get_instances("remote")
    for inst in instances:
        for tag in inst.tags:
            if tag["Key"] == "Name":
                names.append(tag["Value"])
                break
    return names


def remote_notebook_terminate(name):
    cluster_terminate_spots(name)
    remote = "-".join(name.split("-")[:2])
    ssh_config = SSHConfig.load(Path("~/.ssh/config").expanduser())
    ssh_config.remove(f"{remote}-kernel")
    ssh_config.write(Path("~/.ssh/config").expanduser())
    KernelSpecManager().remove_kernel_spec(remote)
