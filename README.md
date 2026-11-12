# Lab 11 - Infraestructura Big Data como Codigo

Plataforma AWS local con Terraform y Floci: Data Lake S3 y Glue Data Catalog por dominio.

## Requisitos

Docker Desktop, Terraform, AWS CLI v2 y Git.

## Uso

    docker compose up -d
    cd terraform
    terraform init
    terraform validate
    terraform plan
    terraform apply

## Limpieza

    terraform destroy
    cd ..
    docker compose down