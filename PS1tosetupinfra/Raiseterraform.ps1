#saving base path
cd..
$env:BasePath = Get-Location
pause
cd terraform/gcp
#starting terraform
terraform init
#make plan
terraform plan --var-file=terraform.tfvars -out template
pause
#appling plan
terraform apply template
pause
cd $env:BasePath\PS1tosetupinfra



