#!/usr/bin/env python

import kubernetes
import os
import requests
import signal
import subprocess
import yaml

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

NAMESPACE = None
SUBNETS = []
KUBEAPI = None

STUNNEL_SUBPROCESS = None
STUNNEL_PATH = '/usr/local/bin/stunnel'
STUNNEL_CONF_PATH = '/tmp/stunnel.conf'
STUNNEL_GLOBAL_OPT = '''
foreground = yes
fips = no
socket = l:SO_REUSEADDR=yes
'''

STUNNEL_EFS_TARGET_OPT = '''
[{filesystem_id}]
CAfile = /etc/amazon/efs/efs-utils.crt
OCSPaia = no
TIMEOUTbusy = 20
TIMEOUTclose = 0
accept = 127.0.0.1:{port}
checkHost = {target_ip}
client = yes
connect = {target_ip}:2049
delay = yes
libwrap = no
renegotiation = no
sslVersion = TLSv1.2
verifyChain = yes
'''

def set_globals():
    global KUBEAPI, NAMESPACE, SUBNETS

    with open('/run/secrets/kubernetes.io/serviceaccount/namespace') as f:
        NAMESPACE = f.read()

    with open('/run/secrets/kubernetes.io/serviceaccount/token') as f:
        serviceaccount_token = f.read()
        kubeconfig = kubernetes.client.Configuration()
        kubeconfig.api_key['authorization'] = serviceaccount_token
        kubeconfig.api_key_prefix['authorization'] = 'Bearer'
        kubeconfig.host = os.environ['KUBERNETES_PORT'].replace('tcp://', 'https://', 1)
        kubeconfig.ssl_ca_cert = '/run/secrets/kubernetes.io/serviceaccount/ca.crt'
        KUBEAPI = kubernetes.client.CoreV1Api(
            kubernetes.client.ApiClient(kubeconfig)
        )

    r = requests.get("http://169.254.169.254/latest/meta-data/network/interfaces/macs/")
    for mac in r.text.split():
        r = requests.get("http://169.254.169.254/latest/meta-data/network/interfaces/macs/{}subnet-id".format(mac))
        SUBNETS.append(r.text)

def get_target_ip_from_efs_target(efs_target):
    target_ip = None
    for subnet, ip in efs_target['mount_target_ip_by_subnet'].items():
        if subnet in SUBNETS:
            return ip
    raise Exception("No target ip found for any subnet available")

def write_stunnel_conf(conf):
    f = open(STUNNEL_CONF_PATH, 'w')
    f.write(STUNNEL_GLOBAL_OPT)
    for filesystem_id, efs_target in conf['efs_stunnel_targets'].items():
        try:
            f.write(STUNNEL_EFS_TARGET_OPT.format(
                filesystem_id = filesystem_id,
                port = efs_target['stunnel_port'],
                target_ip = get_target_ip_from_efs_target(efs_target)
            ))
        except Exception as e:
            print("Failure handling stunnel for {}".format(filesystem_id))
            print(e)

def start_stunnel():
    global STUNNEL_SUBPROCESS
    STUNNEL_SUBPROCESS = subprocess.Popen([
        STUNNEL_PATH,
        STUNNEL_CONF_PATH
    ])
 
def manage_stunnel():
    w = kubernetes.watch.Watch()
    for event in w.stream(
        KUBEAPI.list_namespaced_config_map,
        NAMESPACE
    ):
        if event['object'].metadata.name == 'efs-stunnel':
            efs_stunnel_conf = yaml.load(event['object'].data['efs-stunnel.yaml'])
            write_stunnel_conf(efs_stunnel_conf)
            if STUNNEL_SUBPROCESS:
                print("Reloading efs-stunnel configuration")
                STUNNEL_SUBPROCESS.send_signal(signal.SIGHUP)
            else:
                print("Starting stunnel")
                start_stunnel()

def main():
    set_globals()
    manage_stunnel()

if __name__ == '__main__':
   main()
