# OpenShift EFS Storage Class with stunnel

This project provides a complete implementation for TLS secured EFS storage
provisioner that can be used with OpenShift. While the build and installation
process is written specifically for OpenShift it could be easily adapted to work
with any Kubernetes deployment on AWS.

## Installation

As an administratior, create namespace openshift-efs-stunnel:

```
oc adm new-project openshift-efs-stunnel
```

Check if rhel7 imagestream is defined in the openshift namespace:

```
oc get imagestream rhel7 -n openshift
```

Create the rhel7 imagestream if needed:

```
oc create -f rhel7-imagestream.yaml
```

Process build template to create image streams and build configs for
efs-manager and efs-stunnel components:

```
oc process -f build-template.yaml | oc create -n openshift-efs-stunnel -f -
```

Build both components:

```
oc start-build --follow efs-manager -n openshift-efs-stunnel --from-dir=.
oc start-build --follow efs-stunnel -n openshift-efs-stunnel --from-dir=.
```

Process installation template to install:

```
oc process -f install-template.yaml | oc create -f -
```

## Usage

When persistent volumes are created the provisioner components will
automatically create persistent volumes to match the claim. Claims must
include a `storageClassName` to match a configured storage class with the
provisioner "gnuthought.com/efs-stunnel". Additionally claims must at
least specify a selector with "matchLabels" set including a "volume\_name"
label. The volume name must consist of only lowercase "a-z" or the numbers
"0-9" or underscore.

For example:

```
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: efs-example
spec:
  accessModes:
  - ReadWriteMany
  resources:
    requests:
      storage: 1Gi
  selector:
    matchLabels:
      volume_name: example
  storageClassName: efs-stunnel
```

This will cause an NFS persistent volume to be created that points to a
managed stunnel port to map to the path `/{namespace}/{volume_name}` within
the the assigned EFS file system.

Additional supported parameters include:

`file_system_id` - File system id to request from the provisioner.

`reclaim_policy` - Reclaim policy override. Can be set to "Delete" or "Retain".

## Configuration

The installation template creates a storage class with the following definition:

```
kind: StorageClass
apiVersion: storage.k8s.io/v1
metadata:
  name: efs-stunnel
provisioner: gnuthought.com/efs-stunnel
parameters:
  default_file_system_id: auto
reclaimPolicy: Delete
```

With this configuration the efs-manager process will automatically discover
available EFS filesystem ids by querying the EFS API. If multiple EFS
filesystems are available then one will be arbitrarily selected.

Supported parameters are:

`default_file_system_id` - Set this to the EFS filesystem id for the storage
class to provision persistent volumes.

`file_system_ids` - Restrict available file systems that can be requested to
this list. Must be given as a comma separated list.

The efs-manager deployment supports the following environment variables:

`BASE_STUNNEL_PORT` - Base local host port for creating stunnel local
configuration. Defaults to 20490.

`EFS_POLLING_INTERVAL` - How often in seconds the manager should poll the EFS
API to discover file systems. Defaults to 300 (5 minutes).

`LOGGING_LEVEL` - Logging level, defaults to "INFO".

`PROVISION_BACKOFF_INTERVAL` - Minimum time to wait between processing and
retrying after provisioning a persistent volume fails.


`STORAGE_PROVISIONER_NAME` - Provisioner name used on the storage class and in
annotations. Defaults to "gnuthought.com/efs-stunnel".

`WORKER_IMAGE` - Docker image used to launch worker containers for running
shell commands to create and clean up mount point directories. Defaults to
"rhel7:latest".

The efs-stunnel daemonset supports the following environment variables:

`LOGGING_LEVEL` - Logging level, defaults to "INFO".

`STUNNEL_CONF_PATH` - Location where the dynamically created stunnel
configuration will be created within the image. Defaults to "/tmp/stunnel.conf".

`STUNNEL_PATH` - Location of the stunnel binary within the image. By default it
searches for "/usr/local/bin/stunnel" and "/usr/bin/stunnel".
