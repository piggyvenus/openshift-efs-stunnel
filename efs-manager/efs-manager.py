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
provision_backoff = {}
provision_backoff_interval = int(os.environ.get('PROVISION_BACKOFF_INTERVAL', 60))
storage_classes = {}
storage_provisioner_name = os.environ.get('STORAGE_PROVISIONER_NAME', 'gnuthought.com/efs-stunnel')
worker_image = os.environ.get('WORKER_IMAGE', 'rhel7:latest')

def init():
    """Initialization function before management loops."""
    init_logging()
    init_efs_api()
    init_kube_api()
    init_namespace()

def init_logging():
    """Define logger global and set default logging level.
    Default logging level is INFO and may be overridden with the
    LOGGING_LEVEL environment variable.
    """
    global logger
    logging.basicConfig(
        format='%(asctime)-15s %(levelname)s %(message)s',
    )
    logger = logging.getLogger('efs-manager')
    logger.setLevel(os.environ.get('LOGGING_LEVEL', 'INFO'))

def init_efs_api():
    """Set efs_api global to communicate with the EFS API for this region."""
    global efs_api
    r = requests.get("http://169.254.169.254/latest/dynamic/instance-identity/document")
    response_json = r.json()
    efs_api = boto3.client('efs', region_name=response_json.get('region'))

def init_kube_api():
    """Set kube_api and kube_storage_api globals to communicate with the local
    kubernetes cluster."""
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
    """Set the namespace global based on the namespace in which this pod is
    running.
    """
    global namespace
    with open('/run/secrets/kubernetes.io/serviceaccount/namespace') as f:
        namespace = f.read()

def create_root_pvs_and_pvcs(conf_efs_stunnel_targets):
    """Create persistent volumes and persistent volume claims for the root
    of each EFS filesystem. These are used to launch worker pods to create
    and cleanup subdirectories for other volumes within the filesystem.
    """
    for file_system_id, efs_stunnel_target in conf_efs_stunnel_targets.items():
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
    """Create persistent volume and persistent volume claim for the root of a
    given EFS filesystem id using the given stunnel port for access.

    The storage class "efs-stunnel-system" is used for these resources to
    prevent them from being managed as normal volumes.

    A root_key label is designed as a security feature to prevent another 
    unexpected persistent volume claim from binding the root persistent volume.
    """
    root_key = ''.join(random.sample(string.lowercase+string.digits, 10))
    pvc = create_root_pvc(file_system_id, root_key)
    create_root_pv(file_system_id, stunnel_port, pvc)

def create_root_pvc(file_system_id, root_key):
    """Create persistent volume claim for the root of the given EFS filesystem
    id.
    """
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
    """Create persistent volume for the root of the given EFS filesystem id."""
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
                persistent_volume_reclaim_policy = 'Retain',
                storage_class_name = "efs-stunnel-system"
            )
        )
    )

def remove_root_pvc(file_system_id):
    """Remove the root persistent volume claim for an EFS file system."""
    delete_namespaced_persistent_volume_claim(
        "efs-stunnel-{}".format(file_system_id),
        namespace,
        {}
    )

def manage_stunnel_conf():
    """Main function for managing stunnel config map and global variable
    efs_stunnel_targets.
    """
    global efs_stunnel_targets

    config_map = get_config_map()
    conf_efs_stunnel_targets = get_config_map_efs_stunnel_targets(config_map)

    if update_efs_stunnel_targets(conf_efs_stunnel_targets):
        create_root_pvs_and_pvcs(conf_efs_stunnel_targets)
        efs_stunnel_targets = conf_efs_stunnel_targets

        if config_map:
            update_config_map()
        else:
            create_config_map()
    elif not efs_stunnel_targets:
        # Initialize efs_stunnel_targets if not set
        efs_stunnel_targets = conf_efs_stunnel_targets

def update_efs_stunnel_targets(efs_targets):
    """Update dictionary of EFS stunnel targets and return True if changed."""
    changed = False
    next_stunnel_port = base_stunnel_port

    used_stunnel_ports = {}
    for efs_stunnel_target in efs_targets.values():
        efs_stunnel_target['not_found'] = True
        used_stunnel_ports[efs_stunnel_target['stunnel_port']] = True

    file_systems = efs_api.describe_file_systems()
    for file_system in file_systems['FileSystems']:
        file_system_id = file_system['FileSystemId']
        mount_target_ip_by_subnet = {}
        if file_system_id in efs_targets:
            efs_stunnel_target = efs_targets[file_system_id]
            del efs_stunnel_target['not_found']
        else:
            logger.info("New file system id {}".format(file_system_id))
            while next_stunnel_port in used_stunnel_ports:
                next_stunnel_port += 1
            efs_stunnel_target = {
                "name": file_system['Name'],
                "stunnel_port": next_stunnel_port
            }
            next_stunnel_port += 1
            efs_targets[file_system_id] = efs_stunnel_target
            changed = True

        mount_target_ip_by_subnet = efs_stunnel_target.get('mount_target_ip_by_subnet', None)
        if not mount_target_ip_by_subnet:
            efs_stunnel_target['mount_target_ip_by_subnet'] = mount_target_ip_by_subnet = {}

        mount_targets = efs_api.describe_mount_targets(FileSystemId=file_system['FileSystemId'])
        for mount_target in mount_targets['MountTargets']:
            if mount_target_ip_by_subnet.get(mount_target['SubnetId'],'') != mount_target['IpAddress']:
                logger.info("Set mount target ip for {} to {} on {}".format(
                    file_system_id,
                    mount_target['IpAddress'],
                    mount_target['SubnetId']
                ))
                mount_target_ip_by_subnet[mount_target['SubnetId']] = mount_target['IpAddress']
                changed = True

    for file_system_id, efs_stunnel_target in efs_targets.items():
        if 'not_found' in efs_stunnel_target:
            logger.info("Removing EFS {}".format(file_system-id))
            del efs_targets[file_system_id]
            remove_root_pvc(file_system_id)
            changed = True

    return changed

def get_config_map():
    """Return efs-stunnel config map if it exists."""
    config_map = None
    try:
        config_map = kube_api.read_namespaced_config_map(
            'efs-stunnel',
            namespace
        )
    except kubernetes.client.rest.ApiException as e:
        if e.status != 404:
            raise Exception("Error getting efs-stunnel config map" + str(e))
    return config_map

def get_config_map_efs_stunnel_targets(config_map):
    """Return efs_stunnel_targets from config map if it is defined. If the
    config map is not defined or does not have efs_stunel_targets then return
    an empty dict.
    """
    if config_map:
        stunnel_conf = yaml.load(config_map.data['efs-stunnel.yaml'])
        return stunnel_conf.get('efs_stunnel_targets', {})
    else:
        return {}

def create_config_map():
    """Create the efs-stunnel config map with the efs_stunnel_targets."""
    logger.info("Creating efs-stunnel config map")
    kube_api.create_namespaced_config_map(
        namespace,
        kubernetes.client.V1ConfigMap(
            data = config_map_data(),
            metadata = kubernetes.client.V1ObjectMeta(
                name = 'efs-stunnel',
                labels = {
                    'component': 'efs-stunnel'
                }
            )
        )
    )

def update_config_map():
    """Patch the efs-stunnel config map with the efs_stunnel_targets."""
    logger.info("Updating efs-stunnel config map")
    kube_api.patch_namespaced_config_map(
        'efs-stunnel',
        namespace,
        { "data": config_map_data() }
    )

def config_map_data():
    """Return the data for the efs-stunnel config map containing
    efs_stunnel_targets and a timestamp of last_update.
    """
    return {
        "efs-stunnel.yaml": yaml.safe_dump({
            "efs_stunnel_targets": efs_stunnel_targets,
            "last_update": datetime.datetime.utcnow().strftime('%FT%TZ')
        })
    }

def check_provision_backoff(pvc):
    """Check if there was a recent attempt to provision a persistent volume
    for the given persistent volume claim. If so, then return a true value.
    """
    for pvc_uid, last_attempt in provision_backoff.items():
        if last_attempt < time.time() - provision_backoff_interval:
            del provision_backoff[pvc_uid]
    ret = pvc.metadata.uid in provision_backoff
    provision_backoff[pvc.metadata.uid] = time.time()
    return ret

def get_file_system_id_from_pvc(pvc):
    """Get file system id from a persistent volume claim and enforce
    restrictions on file system ids if set on the storage class.
    """
    sc = storage_classes[pvc.spec.storage_class_name]

    file_system_id = pvc.spec.selector.match_labels.get('file_system_id', None)
    if not file_system_id:
        if 'default_file_system_id' in sc:
            file_system_id = sc['default_file_system_id']
        else:
            return None

    if 'file_system_ids' in sc:
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
    """Check if a persistent volume claim should be rejected and return a
    string describing the reason if so.
    """
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
    if not re.match(r'^[a-z0-9_]+$', pvc.spec.selector.match_labels['volume_name']):
        return "Invalid value for pvc.spec.selector.match_labels.volume_name", True
    if not pvc.spec.selector.match_labels.get('reclaim_policy', 'Delete') in ['Delete','Retain']:
        return "Invalid value for pvc.spec.selector.match_labels.reclaim_policy", True

    return False, False

def record_pvc_reject(pvc, reject_reason):
    """Record that a persistent volume claim was rejected in the annotations
    of the persistent volume claim.
    """
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
    """Check if a persistent volume claim should be rejected and process the
    rejection. Return True if rejected.
    """
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
    """Start worker pod to manage mountpoint directories within the EFS
    filesystem.
    """
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
    """Delete a worker pod by name."""
    kube_api.delete_namespaced_pod(
        worker_name,
        namespace,
        {}
    )

def wait_for_worker_completion(worker_name):
    """Wait for worker to complete and delete if successful. Failed workers
    indicate a misconfiguration or bug and so are left for troubleshooting.
    """
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
    """Run a mountpoint worker pod and wait for it to complete."""
    worker_name = "efs-{}-{}{}-{}".format(
        action,
        file_system_id,
        re.sub('[^0-9a-zA-Z]+', '-', path),
        ''.join(random.sample(string.lowercase+string.digits, 5))
    )
    start_mountpoint_worker(worker_name, file_system_id, path, command)
    wait_for_worker_completion(worker_name)

def initialize_pv_mountpoint(file_system_id, path):
    """Launch mountpoint worker pod to create a mountpoint within an EFS
    filesystem.
    """
    run_mountpoint_worker(
        file_system_id,
        path,
        'init',
        'mkdir -p /efs{0}; chmod 777 /efs{0}'.format(path)
    )

def remove_pv_mountpoint(file_system_id, path):
    """Launch mountpoint worker pod to remove a mountpoint within an EFS
    filesystem.
    """
    run_mountpoint_worker(
        file_system_id,
        path,
        'clean',
        'rm -rf /efs{0}'.format(path)
    )

def create_pv_for_pvc(pvc):
    """Handle persistent volume creation for a persistent volume claim."""
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

    kube_api.create_persistent_volume(
        kubernetes.client.V1PersistentVolume(
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
                persistent_volume_reclaim_policy = pvc.spec.selector.match_labels.get(
                    'reclaim_policy',
                    storage_classes[pvc.spec.storage_class_name]['reclaim_policy']
                ),
                storage_class_name = pvc.spec.storage_class_name
            )
        )
    )

    logger.info("Created persistent volume {}".format(pv_name))

def pvc_is_root(pvc):
    """Return True if a persistent volume claim has a root_key."""
    return pvc.spec.selector \
       and pvc.spec.selector.match_labels \
       and pvc.spec.selector.match_labels.get('root_key', None)

def pvc_has_been_rejected(pvc):
    """Return True if a persistent volume claim has a rejected annotation."""
    annotations = pvc.metadata.annotations
    return (
        annotations and
        annotations.get('efs-stunnel.gnuthought.com/rejected','') == 'true'
    )

def manage_persistent_volume_claims():
    """Watch loop to manage persistent volume claims."""
    # Wait for efs_stunnel_targets to be set
    while not efs_stunnel_targets:
        time.sleep(10)

    logger.info("Starting to manage efs-stunnel persistent volumes")

    # FIXME: there should be a mechanism to periodically restart the watch to
    # reprocess all persistent volume claims so that claims that were rejected
    # because the EFS target did not exist can be picked up later if the EFS
    # target is then discovered.
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

def clean_persistent_volume(pv):
    """Remove mount point directory for a persistent volume."""
    logger.info("Cleaning persistent volume {}".format(pv.metadata.name))
    remove_pv_mountpoint(
        pv.metadata.annotations['efs-stunnel.gnuthought.com/file-system-id'],
        pv.spec.nfs.path
    )

def delete_persistent_volume(pv):
    """Delete a persistent volume using the kubernetes api."""
    logger.info("Deleting persistent volume {}".format(pv.metadata.name))
    kube_api.delete_persistent_volume(pv.metadata.name, {})

def manage_persistent_volumes():
    """Watch loop to manage persistent volume cleanup when released."""
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
                clean_persistent_volume(pv)
            delete_persistent_volume(pv)

def register_storage_class(sc):
    """Add storage class to storage_classes global valiable."""
    storage_class = {
        'reclaim_policy': sc.reclaim_policy
    }

    if sc.parameters:
        if 'default_file_system_id' in sc.parameters:
            storage_class['default_file_system_id'] = sc.parameters['default_file_system_id']
        if 'file_system_ids' in sc.parameters:
            storage_class['file_system_ids'] = sc.parameters['file_system_ids'].split(',')

    storage_classes[sc.metadata.name] = storage_class

def manage_storage_classes():
    """Storage class watch loop."""
    w = kubernetes.watch.Watch()
    for event in w.stream(
        kube_storage_api.list_storage_class
    ):
        sc = event['object']
        if event['type'] in ['ADDED','MODIFIED'] \
        and sc.provisioner == storage_provisioner_name \
        and not sc.metadata.deletion_timestamp:
            register_storage_class(sc)

def manage_persistent_volume_claims_loop():
    """Top level loop for managing persistent volume claims."""
    while True:
        try:
            manage_persistent_volume_claims()
        except Exception as e:
            logger.exception("Error in manage_persistent_volume_claims_loop: " + str(e))
            time.sleep(60)

def manage_persistent_volumes_loop():
    """Top level loop for managing persistent volumes."""
    while True:
        try:
            manage_persistent_volumes()
        except Exception as e:
            logger.exception("Error in manage_persistent_volumes_loop: " + str(e))
            time.sleep(60)

def manage_storage_classes_loop():
    """Top level loop for managing storage classes."""
    while True:
        try:
            manage_storage_classes()
        except Exception as e:
            logger.exception("Error in manage_storage_classes_loop: " + str(e))
            time.sleep(60)

def manage_stunnel_loop():
    """Top level loop for managing stunnel."""
    while True:
        try:
            manage_stunnel_conf()
        except Exception as e:
            logger.exception("Error in manage_stunnel_loop: " + str(e))
        time.sleep(efs_polling_interval)

def main():
    """Main function."""
    init()
    threading.Thread(target=manage_persistent_volume_claims_loop).start()
    threading.Thread(target=manage_persistent_volumes_loop).start()
    threading.Thread(target=manage_storage_classes_loop).start()
    manage_stunnel_loop()

if __name__ == '__main__':
    main()
