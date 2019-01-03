#!/usr/bin/env python

import boto3
import datetime
import kubernetes
import kubernetes.client.rest
import os
import random
import re
import requests
import string
import threading
import time
import yaml

import logging
logging.basicConfig(
    format='%(asctime)-15s %(levelname)s %(message)s',
    level=logging.INFO
)

BASE_STUNNEL_PORT = 20490
STORAGE_CLASS_NAME = 'efs-stunnel'

EFSAPI = None
KUBEAPI = None
NAMESPACE = None

EFS_STUNNEL_TARGETS = None

PROVISION_BACKOFF = {}
DEPROVISION_BACKOFF = {}
PROVISION_BACKOFF_INTERVAL = 60

def set_globals():
    global EFSAPI, KUBEAPI, NAMESPACE

    r = requests.get("http://169.254.169.254/latest/dynamic/instance-identity/document")
    response_json = r.json()
    EFSAPI = boto3.client('efs', region_name=response_json.get('region'))

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

    with open('/run/secrets/kubernetes.io/serviceaccount/namespace') as f:
        NAMESPACE = f.read()

def manage_stunnel_conf():
    global EFS_STUNNEL_TARGETS
    conf_changed = False
    configmap = None
    efs_stunnel_targets = {}
    try:
        configmap = KUBEAPI.read_namespaced_config_map(
            'efs-stunnel',
            NAMESPACE
        )
        stunnel_conf = yaml.load(configmap.data['efs-stunnel.yaml'])
        efs_stunnel_targets = stunnel_conf.get('efs_stunnel_targets', {})
    except kubernetes.client.rest.ApiException as e:
        if e.status != 404:
            logging.error("Error getting efs-stunnel configmap" + str(e))
            return

    next_stunnel_port = BASE_STUNNEL_PORT
    used_stunnel_ports = {}
    for efs_stunnel_target in efs_stunnel_targets.values():
        efs_stunnel_target['not_found'] = True
        used_stunnel_ports[efs_stunnel_target['stunnel_port']] = True

    file_systems = EFSAPI.describe_file_systems()
    for file_system in file_systems['FileSystems']:
        file_system_id = file_system['FileSystemId']
        mount_target_ip_by_subnet = {}
        if file_system_id in efs_stunnel_targets:
            efs_stunnel_target = efs_stunnel_targets[file_system_id]
            del efs_stunnel_target['not_found']
        else:
            logging.info("New filesystem {}".format(file_system_id))
            # FIXME - Add new root dir persistent volume and persistent volume claim
            while next_stunnel_port in used_stunnel_ports:
                next_stunnel_port += 1
            efs_stunnel_target = {
                "name": file_system['Name'],
                "stunnel_port": next_stunnel_port
            }
            next_stunnel_port += 1
            efs_stunnel_targets[file_system_id] = efs_stunnel_target
            conf_changed = True

        mount_target_ip_by_subnet = efs_stunnel_target.get('mount_target_ip_by_subnet', None)
        if not mount_target_ip_by_subnet:
            efs_stunnel_target['mount_target_ip_by_subnet'] = mount_target_ip_by_subnet = {}

        mount_targets = EFSAPI.describe_mount_targets(FileSystemId=file_system['FileSystemId'])
        for mount_target in mount_targets['MountTargets']:
            if mount_target_ip_by_subnet.get(mount_target['SubnetId'],'') != mount_target['IpAddress']:
                logging.info("Set mount target ip for {} to {} on {}".format(
                    file_system_id,
                    mount_target['IpAddress'] ,
                    mount_target['SubnetId']
                ))
                mount_target_ip_by_subnet[mount_target['SubnetId']] = mount_target['IpAddress']
                conf_changed = True

    for file_system_id, efs_stunnel_target in efs_stunnel_targets.items():
        if 'not_found' in efs_stunnel_target:
            # FIXME - Remove root dir persistent volume and persistent volume claim
            del efs_stunnel_targets[file_system_id]
            logging.info("Removing EFS {}".format(file_system-id))
            conf_changed = True

    EFS_STUNNEL_TARGETS = efs_stunnel_targets
    if not conf_changed:
        return

    efs_stunnel_conf = {
        "efs_stunnel_targets": efs_stunnel_targets,
        "last_update": datetime.datetime.utcnow().strftime('%FT%TZ')
    }
    configmap_data = {
        "efs-stunnel.yaml": yaml.safe_dump(efs_stunnel_conf)
    }
    if configmap:
        logging.info("Updating efs-stunnel configmap")
        KUBEAPI.patch_namespaced_config_map(
            'efs-stunnel',
            NAMESPACE,
            {"data": configmap_data}
        )
    else:
        logging.info("Creating efs-stunnel configmap")
        configmap = kubernetes.client.V1ConfigMap()
        configmap.data = configmap_data
        configmap.metadata = kubernetes.client.V1ObjectMeta()
        configmap.metadata.name = 'efs-stunnel'
        configmap.metadata.labels = {
            'component': 'efs-stunnel'
        }
        KUBEAPI.create_namespaced_config_map(
            NAMESPACE,
            configmap
        )

def manage_stunnel_loop():
    while True:
        try:
            manage_stunnel_conf()
        except Exception as e:
            logging.error("Error in manage_stunnel_loop: " + str(e))
        time.sleep(300)

def provision_backoff(pvc):
    for pvc_uid, last_attempt in PROVISION_BACKOFF.items():
        if last_attempt < time.time() - PROVISION_BACKOFF_INTERVAL:
            del PROVISION_BACKOFF[pvc_uid]
    ret = pvc.metadata.uid in PROVISION_BACKOFF
    PROVISION_BACKOFF[pvc.metadata.uid] = time.time()
    return ret

def deprovision_backoff(pv):
    for pv_uid, last_attempt in DEPROVISION_BACKOFF.items():
        if last_attempt < time.time() - PROVISION_BACKOFF_INTERVAL:
            del DEPROVISION_BACKOFF[pv_uid]
    ret = pv.metadata.uid in DEPROVISION_BACKOFF
    DEPROVISION_BACKOFF[pv.metadata.uid] = time.time()
    return ret

def pvc_reject_reason(pvc):
    if not pvc.spec.selector:
        return "Missing spec.selector"
    if not pvc.spec.selector.match_labels['file_system_id']:
        return "Missing spec.selector.file_system_id"
    if not pvc.spec.selector.match_labels['file_system_id'] in EFS_STUNNEL_TARGETS:
        return "Unknown file_system_id {}".format(pvc.spec.selector.match_labels['file_system_id'])
    if not pvc.spec.selector.match_labels['subdir']:
        return "Missing spec.selector.subdir"
    if not re.match(r'^[a-z0-9]+$', pvc.spec.selector.match_labels['subdir']):
        return "Invalid value for pvc.spec.selector.match_labels.subdir"

    return

def reject_invalid_pvc(pvc):
    reject_reason = pvc_reject_reason(pvc)
    if not reject_reason:
        return

    logging.info("Rejecting pvc {} in {}: {}".format(
        pvc.metadata.name,
        pvc.metadata.namespace,
        reject_reason
    ))

    KUBEAPI.patch_namespaced_persistent_volume_claim(
        pvc.metadata.name,
        pvc.metadata.namespace,
        {
            "metadata": {
                "annotations": {
                    "efs-stunnel.gnuthought.com/rejected": "true",
                    "efs-stunnel.gnuthought.com/reject-reason": reject_reason
                }
            }
        }
    )
    return True

def create_pv_for_pvc(pvc):
    if provision_backoff(pvc) \
    or reject_invalid_pvc(pvc):
        return

    file_system_id = pvc.spec.selector.match_labels['file_system_id']
    namespace = pvc.metadata.namespace
    subdir = pvc.spec.selector.match_labels['subdir']

    pv_name = "efs-stunnel-{}-{}-{}".format(
        file_system_id,
        subdir,
        ''.join(random.sample(string.lowercase+string.digits, 5))
    )

    KUBEAPI.create_persistent_volume( kubernetes.client.V1PersistentVolume(
        metadata = kubernetes.client.V1ObjectMeta(
            name = pv_name,
            labels = pvc.spec.selector.match_labels,
            annotations = {
                "pv.kubernetes.io/provisioned-by": "gnuthought.com/efs-stunnel"
            }
        ),
        spec = kubernetes.client.V1PersistentVolumeSpec(
            access_modes = pvc.spec.access_modes,
            capacity = pvc.spec.resources.requests,
            claim_ref = kubernetes.client.V1ObjectReference(
                api_version = pvc.api_version,
                kind = pvc.kind,
                name = pvc.metadata.name,
                namespace = pvc.metadata.namespace,
                resource_version = pvc.metadata.resource_version,
                uid = pvc.metadata.uid
            ),
            mount_options = [
                'port={}'.format(
                    EFS_STUNNEL_TARGETS[file_system_id]['stunnel_port']
                )
            ],
            nfs = kubernetes.client.V1NFSVolumeSource(
                path = '/{}/{}'.format(pvc.metadata.namespace, subdir),
                server = '127.0.0.1'
            ),
            persistent_volume_reclaim_policy = 'Delete',
            storage_class_name = pvc.spec.storage_class_name
        )
    ))

    logging.info("Created persistent volume {}".format(pv_name))

    # FIXME - initialize_pv_mountpoint(...)

def pvc_has_been_rejected(pvc):
    annotations = pvc.metadata.annotations
    return (
        annotations and
        annotations.get('efs-stunnel.gnuthought.com/rejected','') == 'true'
    )

def manage_persistentvolumeclaims():
    # Wait for EFS_STUNNEL_TARGETS
    while not EFS_STUNNEL_TARGETS:
        time.sleep(10)

    logging.info("Starting to manage efs-stunnel persistent volumes")

    w = kubernetes.watch.Watch()
    for event in w.stream(
        KUBEAPI.list_persistent_volume_claim_for_all_namespaces
    ):
        pvc = event['object']
        if event['type'] in ['ADDED','MODIFIED'] \
        and pvc.spec.storage_class_name == STORAGE_CLASS_NAME \
        and pvc.status.phase == 'Pending' \
        and not pvc.spec.volume_name \
        and not pvc_has_been_rejected(pvc):
            create_pv_for_pvc(pvc)

def manage_persistentvolumeclaims_loop():
    while True:
        try:
            manage_persistentvolumeclaims()
        except Exception as e:
            logging.error("Error in manage_persistentvolumeclaims_loop: " + str(e))
            time.sleep(60)

def delete_persistentvolume(pv):
    logging.info("Deleting persistent volume {}".format(pv.metadata.name))
    # FIXME - remove_pv_mountpoint(...)
    KUBEAPI.delete_persistent_volume(pv.metadata.name, {})

def manage_persistentvolumes():
    w = kubernetes.watch.Watch()
    for event in w.stream(
        KUBEAPI.list_persistent_volume
    ):
        pv = event['object']
        if event['type'] in ['ADDED','MODIFIED'] \
        and pv.spec.storage_class_name == STORAGE_CLASS_NAME \
        and pv.spec.persistent_volume_reclaim_policy == 'Delete' \
        and pv.status.phase == 'Released' \
        and not deprovision_backoff(pv):
            delete_persistentvolume(pv)

def manage_persistentvolumes_loop():
    while True:
        try:
            manage_persistentvolumes()
        except Exception as e:
            logging.error("Error in manage_persistentvolumes_loop: " + str(e))
            time.sleep(60)

def main():
    set_globals()
    threading.Thread(target=manage_persistentvolumeclaims_loop).start()
    threading.Thread(target=manage_persistentvolumes_loop).start()
    manage_stunnel_loop()

if __name__ == '__main__':
    main()
