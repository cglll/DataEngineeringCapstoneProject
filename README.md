# DataEngineeringCapstoneProject
The objective of this repository is to record all the source files used mount the infraestructure.
## Project Bitacora

## Raising Infraestructure

1.-Clone this repository.

2.-Create a project in gcp.

3.-Edit the Terraform\gcp\terraform.tfvars file

```shell
project_id = "<YourProjectName>"
region     = "<YourProjectregion>"
location     = "<YourProjectlocation>"

#GKE
gke_num_nodes = 2
machine_type  = "n1-standard-1"

#CloudSQL
instance_name     = ""
database_version  = "POSTGRES_12"
instance_tier     = "db-f1-micro"
disk_space        = 10
database_name     = "dbname"
db_username       = "dbuser"
db_password       = "dbpassword"

#bucket
bucket="databootcampcglllbucket"

#service account
name="airflow"

#user
email_user="<YourUserEmail>"
```

Run the following ps1 files in powershell:


```shell
DataEngineeringCapstoneProject\PS1tosetupinfra> .\InfraRaise.ps1
```

This command will automatically setup the required infraestructure to run this project.
