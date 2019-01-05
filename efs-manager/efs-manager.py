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
STORAGE_CLASSES = {}
STORAGE_PROVISIONER = 'gnuthought.com/efs-stunnel'

EFSAPI = None
KUBEAPI = None
STORAGEAPI = None
NAMESPACE = None

EFS_POLLING_INTERVAL = int(os.environ.get('EFS_POLLING_INTERVAL', 300))
WORKER_IMAGE = os.environ.get('WORKER_IMAGE', 'rhel7:latest')

EFS_STUNNEL_TARGETS = None

PROVISION_BACKOFF = {}
DEPROVISION_BACKOFF = {}
PROVISION_BACKOFF_INTERVAL = 60

def set_globals():
    global EFSAPI, KUBEAPI, STORAGEAPI, NAMESPACE

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
        STORAGEAPI = kubernetes.client.StorageV1Api(
            kubernetes.client.ApiClient(kubeconfig)
        )

    with open('/run/secrets/kubernetes.io/serviceaccount/namespace') as f:
        NAMESPACE = f.read()

def create_root_pv_and_pvc(file_system_id, stunnel_port):
    root_key = ''.join(random.sample(string.lowercase+string.digits, 10))
    
    pvc = KUBEAPI.create_namespaced_persistent_volume_claim(
        NAMESPACE,
        kubernetes.client.V1PersistentVolumeClaim(
            metadata = kubernetes.client.V1ObjectMeta(
                name = "efs-stunnel-{}".format(file_system_id),
                annotations = {
                    "volume.beta.kubernetes.io/storage-provisioner": STORAGE_PROVISIONER
                }
            ),
            spec = kubernetes.client.V1PersistentVolumeClaimSpec(
                access_modes = ['ReadWriteMany'],
                resources = kubernetes.client.V1ResourceRequirements(
                    requests = { "storage": "1Gi" },
                ),
                selector = kubernetes.client.V1LabelSelector(
                    match_labels = {
                        'file_system_id': file_system_id,
                        'root_key': root_key
                    }
                ),
                storage_class_name = "efs-stunnel-system"
            )
        )
    )

    KUBEAPI.create_persistent_volume( kubernetes.client.V1PersistentVolume(
        metadata = kubernetes.client.V1ObjectMeta(
            name = "efs-stunnel-{}".format(file_system_id),
            labels = {
                'file_system_id': file_system_id,
                'root_key': root_key
            },
            annotations = {
                'pv.kubernetes.io/provisioned-by': STORAGE_PROVISIONER,
                'efs-stunnel.gnuthought.com/file-system-id': file_system_id
            }
        ),
        spec = kubernetes.client.V1PersistentVolumeSpec(
            access_modes = ['ReadWriteMany'],
            capacity = { "storage": "1Gi" },
            claim_ref = kubernetes.client.V1ObjectReference(
                api_version = pvc.api_version,
                kind = pvc.kind,
                name = pvc.metadata.name,
                namespace = pvc.metadata.namespace,
                resource_version = pvc.metadata.resource_version,
                uid = pvc.metadata.uid
            ),
            mount_options = [ 'port={}'.format(stunnel_port) ],
            nfs = kubernetes.client.V1NFSVolumeSource(
                path = '/',
                server = '127.0.0.1'
            ),
            persistent_volume_reclaim_policy = 'Delete',
            storage_class_name = "efs-stunnel-system"
        )
    ))

def create_root_pvs_and_pvcs(efs_stunnel_targets):
    for file_system_id, efs_stunnel_target in efs_stunnel_targets.items():
        pvc = None
        pvc_name = "efs-stunnel-{}".format(file_system_id)
        try:
            pvc = KUBEAPI.read_namespaced_persistent_volume_claim(pvc_name, NAMESPACE)
        except kubernetes.client.rest.ApiException as e:
            if e.status != 404:
                logging.error("Error getting pvc {}:".format(pvc_name) + str(e))
        if not pvc:
            create_root_pv_and_pvc(
                file_system_id,
                efs_stunnel_target['stunnel_port']
            )

def remove_root_pvc(file_system_id):
    delete_namespaced_persistent_volume_claim(
        "efs-stunnel-{}".format(file_system_id),
        NAMESPACE,
        {}
    )

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
            logging.info("Removing EFS {}".format(file_system-id))
            del efs_stunnel_targets[file_system_id]
            remove_root_pvc(file_system_id)
            conf_changed = True

    create_root_pvs_and_pvcs(efs_stunnel_targets)
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
            logging.exception("Error in manage_stunnel_loop: " + str(e))
        time.sleep(EFS_POLLING_INTERVAL)

def provision_backoff(pvc):
    for pvc_uid, last_attempt in PROVISION_BACKOFF.items():
        if last_attempt < time.time() - PROVISION_BACKOFF_INTERVAL:
            del PROVISION_BACKOFF[pvc_uid]
    ret = pvc.metadata.uid in PROVISION_BACKOFF
    PROVISION_BACKOFF[pvc.metadata.uid] = time.time()
    return ret

def get_file_system_id_from_pvc(pvc):
    sc = STORAGE_CLASSES[pvc.spec.storage_class_name]

    file_system_id = pvc.spec.selector.match_labels.get('file_system_id', None)
    if not file_system_id:
        if sc['default_file_system_id']:
            file_system_id = sc['default_file_system_id']
        else:
            return None

    if sc['file_system_ids']:
        # Restrict access to efs to specific volumes
        if file_system_id == 'auto':
            return sc['file_system_ids'][0]
        elif file_system_id in sc['file_system_ids']:
            return file_system_id
        else:
            return None

    if file_system_id == 'auto' and EFS_STUNNEL_TARGETS:
        return EFS_STUNNEL_TARGETS.keys()[0]
    else:
        return file_system_id

def pvc_reject_reason(pvc):
    if not pvc.spec.selector:
        return "Missing spec.selector", True
    if not pvc.spec.selector.match_labels:
        return "Missing spec.selector.match_labels", True
    file_system_id = get_file_system_id_from_pvc(pvc)
    if not file_system_id:
        return "Missing spec.selector.match_labels.file_system_id", True
    if not file_system_id in EFS_STUNNEL_TARGETS:
        return "Unable to find file_system_id {}".format(file_system_id), False
    if not pvc.spec.selector.match_labels.get('volume_name', False):
        return "Missing spec.selector.match_labels.volume_name", True
    if not re.match(r'^[a-z0-9]+$', pvc.spec.selector.match_labels['volume_name']):
        return "Invalid value for pvc.spec.selector.match_labels.volume_name", True
    if not pvc.spec.selector.match_labels.get('reclaim_policy', 'Delete') in ['Delete','Retain']:
        return "Invalid value for pvc.spec.selector.match_labels.reclaim_policy", True

    return False, False

def record_pvc_reject(pvc, reject_reason):
    if not pvc.metadata.annotations:
        pvc.metadata.annotations = {}
    pvc.metadata.annotations['efs-stunnel.gnuthought.com/rejected'] = "true"
    pvc.metadata.annotations['efs-stunnel.gnuthought.com/reject-reason'] = reject_reason
    KUBEAPI.replace_namespaced_persistent_volume_claim(
        pvc.metadata.name,
        pvc.metadata.namespace,
        pvc
    )

def reject_invalid_pvc(pvc):
    reject_reason, record_rejection = pvc_reject_reason(pvc)
    # FIXME - Create event on reject
    if not reject_reason:
        return

    logging.warn("Rejecting pvc {} in {}: {}".format(
        pvc.metadata.name,
        pvc.metadata.namespace,
        reject_reason
    ))

    if record_rejection:
        record_pvc_reject(pvc, reject_reason)
    return True

def start_mountpoint_worker(worker_name, file_system_id, path, command):
    logging.info("Starting worker pod {}".format(worker_name))
    KUBEAPI.create_namespaced_pod(
        NAMESPACE,
        kubernetes.client.V1Pod(
            metadata = kubernetes.client.V1ObjectMeta(
                name = worker_name,
                labels = { "component": "efs-worker" }
            ),
            spec = kubernetes.client.V1PodSpec(
                containers = [ kubernetes.client.V1Container(
                    name = "worker",
                    image = WORKER_IMAGE,
                    image_pull_policy = "IfNotPresent",
                    command = [
                        "/bin/sh",
                        "-c",
                        command
                    ],
                    volume_mounts = [ kubernetes.client.V1VolumeMount(
                        mount_path = "/efs",
                        name = "efs"
                    )]
                )],
                restart_policy = "OnFailure",
                security_context = kubernetes.client.V1PodSecurityContext(
                    run_as_user = 0
                ),
                service_account_name = "efs-worker",
                volumes = [ kubernetes.client.V1Volume(
                    name = "efs",
                    persistent_volume_claim = kubernetes.client.V1PersistentVolumeClaimVolumeSource(
                        claim_name = "efs-stunnel-{}".format(file_system_id),
                    )
                )]
            )
        )
    )

def delete_worker_pod(worker_name):
    KUBEAPI.delete_namespaced_pod(
        worker_name,
        NAMESPACE,
        {}
    )

def wait_for_worker_completion(worker_name):
    w = kubernetes.watch.Watch()
    for event in w.stream(
        KUBEAPI.list_namespaced_pod,
        NAMESPACE,
        field_selector = "metadata.name={}".format(worker_name)
    ):
        pod = event['object']
        if event['type'] in ['ADDED','MODIFIED']:
            if pod.status.phase == 'Succeeded':
                logging.info("Worker pod {} completed".format(worker_name))
                delete_worker_pod(worker_name)
                return True
            elif pod.status.phase == 'Failed':
                logging.error("Worker pod {} failed".format(worker_name))
                return False

def run_mountpoint_worker(file_system_id, path, action, command):
    worker_name = "efs-{}-{}{}-{}".format(
        action,
        file_system_id,
        re.sub('[^0-9a-zA-Z]+', '-', path),
        ''.join(random.sample(string.lowercase+string.digits, 5))
    )
    start_mountpoint_worker(worker_name, file_system_id, path, command)
    wait_for_worker_completion(worker_name)

def initialize_pv_mountpoint(file_system_id, path):
    run_mountpoint_worker(
        file_system_id,
        path,
        'init',
        'mkdir -p /efs{0}; chmod 777 /efs{0}'.format(path)
    )

def remove_pv_mountpoint(file_system_id, path):
    run_mountpoint_worker(
        file_system_id,
        path,
        'clean',
        'rm -rf /efs{0}'.format(path)
    )

def create_pv_for_pvc(pvc):
    if provision_backoff(pvc) \
    or reject_invalid_pvc(pvc):
        return

    file_system_id = get_file_system_id_from_pvc(pvc)

    namespace = pvc.metadata.namespace
    volume_name = pvc.spec.selector.match_labels['volume_name']
    path = '/{}/{}'.format(pvc.metadata.namespace, volume_name)
    pv_name = "efs-stunnel-{}-{}-{}-{}".format(
        file_system_id,
        pvc.metadata.namespace,
        volume_name,
        ''.join(random.sample(string.lowercase+string.digits, 5))
    )

    initialize_pv_mountpoint(file_system_id, path)

    KUBEAPI.create_persistent_volume( kubernetes.client.V1PersistentVolume(
        metadata = kubernetes.client.V1ObjectMeta(
            name = pv_name,
            labels = pvc.spec.selector.match_labels,
            annotations = {
                "pv.kubernetes.io/provisioned-by": STORAGE_PROVISIONER,
                "efs-stunnel.gnuthought.com/file-system-id": file_system_id
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
                path = path,
                server = '127.0.0.1'
            ),
            persistent_volume_reclaim_policy = pvc.spec.selector.match_labels.get('reclaim_policy', 'Delete'),
            storage_class_name = pvc.spec.storage_class_name
        )
    ))

    logging.info("Created persistent volume {}".format(pv_name))

def pvc_is_root(pvc):
    return pvc.spec.selector \
       and pvc.spec.selector.match_labels \
       and pvc.spec.selector.match_labels.get('root_key', None)

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
        and pvc.spec.storage_class_name in STORAGE_CLASSES \
        and pvc.status.phase == 'Pending' \
        and not pvc.spec.volume_name \
        and not pvc_is_root(pvc) \
        and not pvc_has_been_rejected(pvc):
            create_pv_for_pvc(pvc)

def clean_persistentvolume(pv):
    logging.info("Cleaning persistent volume {}".format(pv.metadata.name))
    remove_pv_mountpoint(
        pv.metadata.annotations['efs-stunnel.gnuthought.com/file-system-id'],
        pv.spec.nfs.path
    )

def delete_persistentvolume(pv):
    logging.info("Deleting persistent volume {}".format(pv.metadata.name))
    KUBEAPI.delete_persistent_volume(pv.metadata.name, {})

def manage_persistentvolumes():
    w = kubernetes.watch.Watch()
    for event in w.stream(
        KUBEAPI.list_persistent_volume
    ):
        pv = event['object']
        if event['type'] in ['ADDED','MODIFIED'] \
        and pv.spec.storage_class_name in STORAGE_CLASSES \
        and pv.status.phase == 'Released' \
        and not pv.metadata.deletion_timestamp:
            if pv.spec.persistent_volume_reclaim_policy == 'Delete':
                clean_persistentvolume(pv)
            delete_persistentvolume(pv)

def register_storageclass(sc):
    STORAGE_CLASSES[sc.metadata.name] = {
        "default_file_system_id": sc.parameters.get('default_file_system_id', None) if sc.parameters else None,
        'file_system_ids': sc.parameters.get('file_system_ids', None) if sc.parameters else None
    }

def manage_storageclasses():
    w = kubernetes.watch.Watch()
    for event in w.stream(
        STORAGEAPI.list_storage_class
    ):
        sc = event['object']
        if event['type'] in ['ADDED','MODIFIED'] \
        and sc.provisioner == STORAGE_PROVISIONER \
        and not sc.metadata.deletion_timestamp:
            register_storageclass(sc)

def manage_persistentvolumeclaims_loop():
    while True:
        try:
            manage_persistentvolumeclaims()
        except Exception as e:
            logging.exception("Error in manage_persistentvolumeclaims_loop: " + str(e))
            time.sleep(60)

def manage_persistentvolumes_loop():
    while True:
        try:
            manage_persistentvolumes()
        except Exception as e:
            logging.exception("Error in manage_persistentvolumes_loop: " + str(e))
            time.sleep(60)

def manage_storageclasses_loop():
    while True:
        try:
            manage_storageclasses()
        except Exception as e:
            logging.exception("Error in manage_storageclasses_loop: " + str(e))
            time.sleep(60)

def main():
    set_globals()
    threading.Thread(target=manage_persistentvolumeclaims_loop).start()
    threading.Thread(target=manage_persistentvolumes_loop).start()
    threading.Thread(target=manage_storageclasses_loop).start()
    manage_stunnel_loop()

if __name__ == '__main__':
    main()
