cd $env:BasePath
cd terraform/gcp
terraform plan --destroy -out template2
pause 
terraform apply template2
cd $env:BasePath\PS1tosetupinfra