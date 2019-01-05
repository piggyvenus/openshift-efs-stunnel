#!/usr/bin/env python

import boto3
import datetime
import kubernetes
import kubernetes.client.rest
import logging
import os
import random
import re
import requests
import string
import threading
import time
import yaml

base_stunnel_port = int(os.environ.get('BASE_STUNNEL_PORT', 20490))
efs_polling_interval = int(os.environ.get('EFS_POLLING_INTERVAL', 300))
efs_stunnel_targets = {}
logging_level = os.environ.get('LOGGING_LEVEL', 'INFO')
provision_backoff = {}
provision_backoff_interval = int(os.environ.get('PROVISION_BACKOFF_INTERVAL', 60))
storage_classes = {}
storage_provisioner_name = os.environ.get('STORAGE_PROVISIONER_NAME', 'gnuthought.com/efs-stunnel')
worker_image = os.environ.get('WORKER_IMAGE', 'rhel7:latest')

def init():
    init_logging()
    init_efs_api()
    init_kube_api()
    init_namespace()

def init_logging():
    global logger
    logging.basicConfig(
        format='%(asctime)-15s %(levelname)s %(message)s',
    )
    logger = logging.getLogger('efs-stunnel')
    logger.setLevel(logging_level)

def init_efs_api():
    global efs_api
    r = requests.get("http://169.254.169.254/latest/dynamic/instance-identity/document")
    response_json = r.json()
    efs_api = boto3.client('efs', region_name=response_json.get('region'))

def init_kube_api():
    global kube_api, kube_storage_api
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

def create_root_pvs_and_pvcs(efs_stunnel_targets):
    for file_system_id, efs_stunnel_target in efs_stunnel_targets.items():
        pvc = None
        pvc_name = "efs-stunnel-{}".format(file_system_id)
        try:
            pvc = kube_api.read_namespaced_persistent_volume_claim(pvc_name, namespace)
        except kubernetes.client.rest.ApiException as e:
            if e.status != 404:
                logger.error("Error getting pvc {}:".format(pvc_name) + str(e))
        if not pvc:
            create_root_pv_and_pvc(
                file_system_id,
                efs_stunnel_target['stunnel_port']
            )

def create_root_pv_and_pvc(file_system_id, stunnel_port):
    root_key = ''.join(random.sample(string.lowercase+string.digits, 10))
    pvc = create_root_pvc(file_system_id, root_key)
    create_root_pv(file_system_id, stunnel_port, pvc)

def create_root_pvc(file_system_id, root_key):
    return kube_api.create_namespaced_persistent_volume_claim(
        namespace,
        kubernetes.client.V1PersistentVolumeClaim(
            metadata = kubernetes.client.V1ObjectMeta(
                name = "efs-stunnel-{}".format(file_system_id),
                annotations = {
                    "volume.beta.kubernetes.io/storage-provisioner": storage_provisioner_name
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

def create_root_pv(file_system_id, stunnel_port, pvc):
    kube_api.create_persistent_volume(
        kubernetes.client.V1PersistentVolume(
            metadata = kubernetes.client.V1ObjectMeta(
                name = "efs-stunnel-{}".format(file_system_id),
                labels = pvc.spec.selector.match_labels,
                annotations = {
                    'pv.kubernetes.io/provisioned-by': storage_provisioner_name,
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
        )
    )

def remove_root_pvc(file_system_id):
    delete_namespaced_persistent_volume_claim(
        "efs-stunnel-{}".format(file_system_id),
        namespace,
        {}
    )

def manage_stunnel_conf():
    # FIXME - This function is too long
    global efs_stunnel_targets
    conf_changed = False
    configmap = None
    conf_efs_stunnel_targets = {}
    try:
        configmap = kube_api.read_namespaced_config_map(
            'efs-stunnel',
            namespace
        )
        stunnel_conf = yaml.load(configmap.data['efs-stunnel.yaml'])
        conf_efs_stunnel_targets = stunnel_conf.get('efs_stunnel_targets', {})
    except kubernetes.client.rest.ApiException as e:
        if e.status != 404:
            logger.error("Error getting efs-stunnel configmap" + str(e))
            return

    next_stunnel_port = base_stunnel_port
    used_stunnel_ports = {}
    for efs_stunnel_target in conf_efs_stunnel_targets.values():
        efs_stunnel_target['not_found'] = True
        used_stunnel_ports[efs_stunnel_target['stunnel_port']] = True

    file_systems = efs_api.describe_file_systems()
    for file_system in file_systems['FileSystems']:
        file_system_id = file_system['FileSystemId']
        mount_target_ip_by_subnet = {}
        if file_system_id in conf_efs_stunnel_targets:
            efs_stunnel_target = conf_efs_stunnel_targets[file_system_id]
            del conf_efs_stunnel_target['not_found']
        else:
            logger.info("New file system id {}".format(file_system_id))
            while next_stunnel_port in used_stunnel_ports:
                next_stunnel_port += 1
            efs_stunnel_target = {
                "name": file_system['Name'],
                "stunnel_port": next_stunnel_port
            }
            next_stunnel_port += 1
            conf_efs_stunnel_targets[file_system_id] = efs_stunnel_target
            conf_changed = True

        mount_target_ip_by_subnet = efs_stunnel_target.get('mount_target_ip_by_subnet', None)
        if not mount_target_ip_by_subnet:
            efs_stunnel_target['mount_target_ip_by_subnet'] = mount_target_ip_by_subnet = {}

        mount_targets = efs_api.describe_mount_targets(FileSystemId=file_system['FileSystemId'])
        for mount_target in mount_targets['MountTargets']:
            if mount_target_ip_by_subnet.get(mount_target['SubnetId'],'') != mount_target['IpAddress']:
                logger.info("Set mount target ip for {} to {} on {}".format(
                    file_system_id,
                    mount_target['IpAddress'] ,
                    mount_target['SubnetId']
                ))
                mount_target_ip_by_subnet[mount_target['SubnetId']] = mount_target['IpAddress']
                conf_changed = True

    for file_system_id, efs_stunnel_target in conf_efs_stunnel_targets.items():
        if 'not_found' in efs_stunnel_target:
            logger.info("Removing EFS {}".format(file_system-id))
            del conf_efs_stunnel_targets[file_system_id]
            remove_root_pvc(file_system_id)
            conf_changed = True

    create_root_pvs_and_pvcs(conf_efs_stunnel_targets)
    efs_stunnel_targets = conf_efs_stunnel_targets
    if not conf_changed:
        return

    configmap_data = {
        "efs-stunnel.yaml": yaml.safe_dump({
            "efs_stunnel_targets": efs_stunnel_targets,
            "last_update": datetime.datetime.utcnow().strftime('%FT%TZ')
        })
    }
    if configmap:
        logger.info("Updating efs-stunnel configmap")
        kube_api.patch_namespaced_config_map(
            'efs-stunnel',
            namespace,
            {"data": configmap_data}
        )
    else:
        logger.info("Creating efs-stunnel configmap")
        kube_api.create_namespaced_config_map(
            namespace,
            kubernetes.client.V1ConfigMap(
                data = configmap_data,
                metadata = kubernetes.client.V1ObjectMeta(
                    name = 'efs-stunnel',
                    labels = {
                        'component': 'efs-stunnel'
                    }
                )
            )
        )

def manage_stunnel_loop():
    while True:
        try:
            manage_stunnel_conf()
        except Exception as e:
            logger.exception("Error in manage_stunnel_loop: " + str(e))
        time.sleep(efs_polling_interval)

def check_provision_backoff(pvc):
    for pvc_uid, last_attempt in provision_backoff.items():
        if last_attempt < time.time() - provision_backoff_interval:
            del provision_backoff[pvc_uid]
    ret = pvc.metadata.uid in provision_backoff
    provision_backoff[pvc.metadata.uid] = time.time()
    return ret

def get_file_system_id_from_pvc(pvc):
    sc = storage_classes[pvc.spec.storage_class_name]

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

    if file_system_id == 'auto' and efs_stunnel_targets:
        return efs_stunnel_targets.keys()[0]
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
    if not file_system_id in efs_stunnel_targets:
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
    kube_api.replace_namespaced_persistent_volume_claim(
        pvc.metadata.name,
        pvc.metadata.namespace,
        pvc
    )

def reject_invalid_pvc(pvc):
    reject_reason, record_rejection = pvc_reject_reason(pvc)
    # FIXME - Create event on reject
    if not reject_reason:
        return

    logger.warn("Rejecting pvc {} in {}: {}".format(
        pvc.metadata.name,
        pvc.metadata.namespace,
        reject_reason
    ))

    if record_rejection:
        record_pvc_reject(pvc, reject_reason)
    return True

def start_mountpoint_worker(worker_name, file_system_id, path, command):
    logger.info("Starting worker pod {}".format(worker_name))
    kube_api.create_namespaced_pod(
        namespace,
        kubernetes.client.V1Pod(
            metadata = kubernetes.client.V1ObjectMeta(
                name = worker_name,
                labels = { "component": "efs-worker" }
            ),
            spec = kubernetes.client.V1PodSpec(
                containers = [ kubernetes.client.V1Container(
                    name = "worker",
                    image = worker_image,
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
    kube_api.delete_namespaced_pod(
        worker_name,
        namespace,
        {}
    )

def wait_for_worker_completion(worker_name):
    w = kubernetes.watch.Watch()
    for event in w.stream(
        kube_api.list_namespaced_pod,
        namespace,
        field_selector = "metadata.name={}".format(worker_name)
    ):
        pod = event['object']
        if event['type'] in ['ADDED','MODIFIED']:
            if pod.status.phase == 'Succeeded':
                logger.info("Worker pod {} completed".format(worker_name))
                delete_worker_pod(worker_name)
                return True
            elif pod.status.phase == 'Failed':
                logger.error("Worker pod {} failed".format(worker_name))
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
    if check_provision_backoff(pvc) \
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

    kube_api.create_persistent_volume( kubernetes.client.V1PersistentVolume(
        metadata = kubernetes.client.V1ObjectMeta(
            name = pv_name,
            labels = pvc.spec.selector.match_labels,
            annotations = {
                "pv.kubernetes.io/provisioned-by": storage_provisioner_name,
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
                    efs_stunnel_targets[file_system_id]['stunnel_port']
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

    logger.info("Created persistent volume {}".format(pv_name))

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
    # Wait for efs_stunnel_targets to be set
    while not efs_stunnel_targets:
        time.sleep(10)

    logger.info("Starting to manage efs-stunnel persistent volumes")

    w = kubernetes.watch.Watch()
    for event in w.stream(
        kube_api.list_persistent_volume_claim_for_all_namespaces
    ):
        pvc = event['object']
        if event['type'] in ['ADDED','MODIFIED'] \
        and pvc.spec.storage_class_name in storage_classes \
        and pvc.status.phase == 'Pending' \
        and not pvc.spec.volume_name \
        and not pvc_is_root(pvc) \
        and not pvc_has_been_rejected(pvc):
            create_pv_for_pvc(pvc)

def clean_persistentvolume(pv):
    logger.info("Cleaning persistent volume {}".format(pv.metadata.name))
    remove_pv_mountpoint(
        pv.metadata.annotations['efs-stunnel.gnuthought.com/file-system-id'],
        pv.spec.nfs.path
    )

def delete_persistentvolume(pv):
    logger.info("Deleting persistent volume {}".format(pv.metadata.name))
    kube_api.delete_persistent_volume(pv.metadata.name, {})

def manage_persistentvolumes():
    w = kubernetes.watch.Watch()
    for event in w.stream(
        kube_api.list_persistent_volume
    ):
        pv = event['object']
        if event['type'] in ['ADDED','MODIFIED'] \
        and pv.spec.storage_class_name in storage_classes \
        and pv.status.phase == 'Released' \
        and not pv.metadata.deletion_timestamp:
            if pv.spec.persistent_volume_reclaim_policy == 'Delete':
                clean_persistentvolume(pv)
            delete_persistentvolume(pv)

def register_storageclass(sc):
    storage_classes[sc.metadata.name] = {
        "default_file_system_id":
            sc.parameters.get('default_file_system_id', None) if sc.parameters else None,
        'file_system_ids':
            sc.parameters.get('file_system_ids', None) if sc.parameters else None
    }

def manage_storageclasses():
    w = kubernetes.watch.Watch()
    for event in w.stream(
        kube_storage_api.list_storage_class
    ):
        sc = event['object']
        if event['type'] in ['ADDED','MODIFIED'] \
        and sc.provisioner == storage_provisioner_name \
        and not sc.metadata.deletion_timestamp:
            register_storageclass(sc)

def manage_persistentvolumeclaims_loop():
    while True:
        try:
            manage_persistentvolumeclaims()
        except Exception as e:
            logger.exception("Error in manage_persistentvolumeclaims_loop: " + str(e))
            time.sleep(60)

def manage_persistentvolumes_loop():
    while True:
        try:
            manage_persistentvolumes()
        except Exception as e:
            logger.exception("Error in manage_persistentvolumes_loop: " + str(e))
            time.sleep(60)

def manage_storageclasses_loop():
    while True:
        try:
            manage_storageclasses()
        except Exception as e:
            logger.exception("Error in manage_storageclasses_loop: " + str(e))
            time.sleep(60)

def main():
    init()
    threading.Thread(target=manage_persistentvolumeclaims_loop).start()
    threading.Thread(target=manage_persistentvolumes_loop).start()
    threading.Thread(target=manage_storageclasses_loop).start()
    manage_stunnel_loop()

if __name__ == '__main__':
    main()
