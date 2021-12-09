#going to base
cd $env:BasePath
cd terraform/gcp
#save bucket name
$env:bucket=$(terraform output -raw bucketrandomname)
$env:bucket 
#go to base
cd $env:BasePath
#save required files to bucket
gsutil cp -r ./bucket_files gs://$env:bucket/k
cd $env:BasePath\PS1tosetupinfra
