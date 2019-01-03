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
