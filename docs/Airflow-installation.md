# Installation:
## Configurations:
- [x] Automatic install requirements when the first start
- [x] Sync this repo. every 10 secs
- [x] Webserver login required

## How to start new cluster of Airflow by helm?

### Requirements
- k8s
- helm

### Step 0. Add helm repo.
```shell
$ helm repo add airflow-stable https://airflow-helm.github.io/charts
$ helm repo update
```

### Step 1. Clone this repo. 
```shell
$ git clone https://github.com/Modovision/Airflow-DAGS.git
$ cd Airflow-DAGS
```
### Step 2. Create Namespace.
```shell
$ kubectl create namespace <your name space here>
```
### Step 3. Create Secrets 

To generate  ```KEYS``` below (public key & private key), please follow this [tutorial](https://docs.github.com/en/free-pro-team@latest/developers/overview/managing-deploy-keys)

HINTS: copy secrets from local to k8s where saved keys
```shell
$ kubectl create secret generic  airflow-git-keys \
  --from-file=airflow-repo=/home/ub/.ssh/airflow-repo \
  --from-file=airflow-repo.pub=/home/ub/.ssh/airflow-repo.pub \
  --from-file=known_hosts=/home/ub/.ssh/known_hosts \
  --namespace <your name space here>
```
### Step 4. Modify custom-values.yml.

In ```line 959``` change url to your repo. 
```yaml
 git:
    ## url of the git repository
    url: "git@github.com:Modovision/Airflow-DAGS.git"   
```
If dags are in the subfolder of repo., ```line 1090``` 
change  ```syncSubPath``` to your path
```yaml
    syncSubPath: "/dags"
```

### Step 5. Deploy
```shell
$ helm install -n <your name space here> \
  airflow airflow-stable/airflow \
  --values ./custom-values.yaml
```
### Step 6. Enter the webserver pod to create webserver user.

```shell
$ kubectl exec \
  -it \
  --namespace <your name space here> \
  --container airflow-web \
  Deployment/airflow-web \
  /bin/bash

(airflow-web)$ airflow users create [-h] \
  -e EMAIL \
  -f FIRSTNAME \
  -u USERNAME \
  -l LASTNAME \  
  -r ROLE \
  [-p PASSWORD] \ # optional
  [--use-random-password] \ # optional

(airflow-web)$ Enter password: <your webservice password here>
```
### Step 7. Port Forwading.

HINTS: Just follow the notes displayed after ```Step 4.```
```shell
$ export POD_NAME=$(kubectl get pods --namespace arthur-lab -l "component=web,app=airflow" -o jsonpath="{.items[0].metadata.name}")
$ kubectl port-forward --namespace arthur-lab \
  $POD_NAME 8080:8080 \
  --address 0.0.0.0

```

## Not enough configured setting info?
please check [here](https://github.com/airflow-helm/charts/tree/main/charts/airflow)

---
## References:
- ```AIRFLOW python api doc.``` [here](https://airflow.apache.org/docs/apache-airflow/1.10.14/_api/index.html)
- ```HELM stable/airflow github``` [here](https://github.com/airflow-helm/charts/tree/main/charts/airflow)