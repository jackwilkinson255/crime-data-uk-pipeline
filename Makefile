gcp_init:
	terraform -chdir=terraform/gcp init

gcp_plan:
	terraform -chdir=terraform/gcp plan

gcp_apply:
	terraform -chdir=terraform/gcp apply -auto-approve

gcp_destroy:
	terraform -chdir=terraform/gcp destroy


sf_init:
	terraform -chdir=terraform/snowflake init

sf_plan:
	terraform -chdir=terraform/snowflake plan

sf_apply:
	terraform -chdir=terraform/snowflake apply -auto-approve

sf_destroy:
	terraform -chdir=terraform/snowflake destroy