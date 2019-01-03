# OpenShift EFS Storage Class with stunnel

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

Create imagestreams for efs-stunnel and efs-manager components:

```
oc create imagestream efs-manager -n openshift-efs-stunnel
oc create imagestream efs-stunnel -n openshift-efs-stunnel
```

Create build configs for both components:

```
oc create -f efs-manager-buildconfig.yaml -n openshift-efs-stunnel
oc create -f efs-stunnel-buildconfig.yaml -n openshift-efs-stunnel
```

Build both components:

```
oc start-build --follow efs-manager -n openshift-efs-stunnel --from-dir=efs-manager
oc start-build --follow efs-stunnel -n openshift-efs-stunnel --from-dir=efs-stunnel
```

Create service accounts for efs-stunnel and efs-manager components:

```
oc create serviceaccount efs-manager -n openshift-efs-stunnel
oc create serviceaccount efs-stunnel -n openshift-efs-stunnel
```

Grant namespace roles for efs-manager and efs-stunnel:

```
oc policy add-role-to-user edit system:serviceaccount:openshift-efs-stunnel:efs-manager -n openshift-efs-stunnel
oc policy add-role-to-user view system:serviceaccount:openshift-efs-stunnel:efs-stunnel -n openshift-efs-stunnel
```

Grant cluster role storage-admin to efs-manager

```
oc adm policy add-cluster-role-to-user storage-admin system:serviceaccount:openshift-efs-stunnel:efs-manager
```

Create SCC to allow efs-stunnel to use the host network:

```
oc create -f efs-stunnel-scc.yaml
```

Create the efs-stunnel storage class:

```
oc create -f efs-stunnel-storageclass.yml
```

Create efs-manager deployment:

```
oc create -f efs-manager-deployment.yaml -n openshift-efs-stunnel
```

Create efs-stunnel daemonset:

```
oc create -f efs-stunnel-daemonset.yaml -n openshift-efs-stunnel
```
