#For raise kubernetes
cd $env:BasePath
cd terraform/gcp
#set the nf cluster
gcloud container clusters get-credentials $(terraform output -raw kubernetes_cluster_name) --region $(terraform output -raw location)
kubectl create namespace nfs
kubectl -n nfs apply -f nfs/nfs-server.yaml 
$env:NFS_SERVER=$(kubectl -n nfs get service/nfs-server -o jsonpath="{.spec.clusterIP}") 
cd $env:BasePath
#kubernetess configurations
cd kubernetes 
kubectl create namespace storage
helm repo add nfs-subdir-external-provisioner https://kubernetes-sigs.github.io/nfs-subdir-external-provisioner/
helm install nfs-subdir-external-provisioner nfs-subdir-external-provisioner/nfs-subdir-external-provisioner `
    --namespace storage `
    --set nfs.server=$env:NFS_SERVER `
    --set nfs.path=/
#Airlflow setup
kubectl create namespace airflow
helm install airflow -f airflow-values.yaml apache-airflow/airflow --namespace airflow
kubectl get pods -n airflow
pause
#start the localhost:8080 bridge
kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airflow
cd $env:BasePath\PS1tosetupinfra

