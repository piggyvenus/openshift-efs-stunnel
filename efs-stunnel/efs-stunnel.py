#!/usr/bin/env python

import kubernetes
import logging
import os
import os.path
import requests
import signal
import subprocess
import yaml
import time
import urllib3
# Disable warnings when connecting to the kubernetes API
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging_level = os.environ.get('LOGGING_LEVEL', 'INFO')
stunnel_subprocess = None
stunnel_conf_path = os.environ.get('STUNNEL_CONF_PATH', '/tmp/stunnel.conf')

stunnel_global_opt = '''
foreground = yes
fips = no
socket = l:SO_REUSEADDR=yes
'''

stunnel_efs_target_opt = '''
[{filesystem_id}]
CAfile = /etc/amazon/efs/efs-utils.crt
OCSPaia = no
TIMEOUTbusy = 20
TIMEOUTclose = 0
accept = 127.0.0.1:{port}
checkHost = {filesystem_id}.efs.{region}.amazonaws.com
client = yes
connect = {target_ip}:2049
delay = yes
libwrap = no
renegotiation = no
sslVersion = TLSv1.2
verifyChain = yes
'''

def init():
    init_logging()
    init_kube_api()
    init_namespace()
    init_region()
    init_stunnel_path()
    init_subnets()

def init_logging():
    global logger
    logging.basicConfig(
        format='%(asctime)-15s %(levelname)s %(message)s',
    )
    logger = logging.getLogger('efs-stunnel')
    logger.setLevel(logging_level)

def init_kube_api():
    global kube_api
    with open('/run/secrets/kubernetes.io/serviceaccount/token') as f:
        serviceaccount_token = f.read()
        kubeconfig = kubernetes.client.Configuration()
        kubeconfig.api_key['authorization'] = serviceaccount_token
        kubeconfig.api_key_prefix['authorization'] = 'Bearer'
        kubeconfig.host = os.environ['KUBERNETES_PORT'].replace('tcp://', 'https://', 1)
        kubeconfig.ssl_ca_cert = '/run/secrets/kubernetes.io/serviceaccount/ca.crt'
        kube_api = kubernetes.client.CoreV1Api(
            kubernetes.client.ApiClient(kubeconfig)
        )
        kube_storage_api = kubernetes.client.StorageV1Api(
            kubernetes.client.ApiClient(kubeconfig)
        )

def init_namespace():
    global namespace
    with open('/run/secrets/kubernetes.io/serviceaccount/namespace') as f:
        namespace = f.read()

def init_region():
    global region
    r = requests.get("http://169.254.169.254/latest/dynamic/instance-identity/document")
    region = r.json().get('region')

def init_stunnel_path():
    global stunnel_path
    stunnel_path = os.environ.get('STUNNEL_PATH', None)
    if stunnel_path:
        return
    for path in [
        '/usr/local/bin/stunnel',
        '/usr/bin/stunnel'
    ]:
        if os.path.exists(path):
            stunnel_path = path
            return
    raise Exception('Unable to find stunnel. Set STUNNEL_PATH environment variable?')

def init_subnets():
    global subnets
    subnets = []

    r = requests.get("http://169.254.169.254/latest/meta-data/network/interfaces/macs/")
    for mac in r.text.split():
        r = requests.get("http://169.254.169.254/latest/meta-data/network/interfaces/macs/{}subnet-id".format(mac))
        subnets.append(r.text)

def get_target_ip_from_efs_target(efs_target):
    target_ip = None
    for subnet, ip in efs_target['mount_target_ip_by_subnet'].items():
        if subnet in subnets:
            return ip
    raise Exception("No target ip found for any subnet available")

def write_stunnel_conf(conf):
    f = open(stunnel_conf_path, 'w')
    f.write(stunnel_global_opt)
    for filesystem_id, efs_target in conf['efs_stunnel_targets'].items():
        try:
            f.write(stunnel_efs_target_opt.format(
                filesystem_id = filesystem_id,
                port = efs_target['stunnel_port'],
                region = region,
                target_ip = get_target_ip_from_efs_target(efs_target)
            ))
        except Exception as e:
            logger.exception("Failure handling stunnel for {}".format(filesystem_id))

def start_or_reload_stunnel():
    if stunnel_subprocess:
        reload_stunnel()
    else:
        start_stunnel()

def start_stunnel():
    global stunnel_subprocess
    logger.info("Starting stunnel")
    stunnel_subprocess = subprocess.Popen([
        stunnel_path,
        stunnel_conf_path
    ])

def reload_stunnel():
    logger.info("Reloading stunnel configuration")
    stunnel_subprocess.send_signal(signal.SIGHUP)
 
def manage_stunnel():
    w = kubernetes.watch.Watch()
    for event in w.stream(
        kube_api.list_namespaced_config_map,
        namespace
    ):
        config_map = event['object']
        if event['type'] in ['ADDED','MODIFIED'] \
        and config_map.metadata.name == 'efs-stunnel':
            efs_stunnel_conf = yaml.load(config_map.data['efs-stunnel.yaml'])
            write_stunnel_conf(efs_stunnel_conf)
            start_or_reload_stunnel()

def main_loop():
    while True:
        try:
            manage_stunnel()
        except Exception as e:
            logger.exception("Error in main_loop: " + str(e))
            time.sleep(60)

def main():
    init()
    main_loop()

if __name__ == '__main__':
   main()
