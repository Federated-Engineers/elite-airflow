# elite-data-airflow.

## OVERVIEW
This is a `dedicated` airflow repository for the `Elite Data Engineering Team`. This is the source of truth for all the DAG definition that run in the elite airflow instance on `Kubernetes`.

Before anything, it's worth to know that all changes or commit must go through `Pull Request (PR)`, direct push to main is `BLOCKED` on this repo, this will enable full visibility into workflows deployed to our Airflow instance. Every `PR` goes through a detail approval from any of the `Engineering Leads` to ensure optimal and efficient workflows are thoroughly reviewd before they hit our `Production` airflow instance.

### REPOSITORY FLOW SUMMARY

<img width="1431" height="663" alt="Screenshot 2026-02-27 at 16 23 31" src="https://github.com/user-attachments/assets/f5442518-de1f-4330-9205-15418a79cc71" />

- The `DAG` development starts from the the Engineer's `local branch`.
- Local branch is `merged`, this is subject to `approval`.
- A `CI/CD pipeline` triggers, new image is built, tagged and pushed to our `Elastic Container Registry`.
  - This pipeline also update [this file](https://github.com/Federated-Engineers/kubernetes-deployments/blob/main/applications/production-values/elite-airflow-values.yaml#L83) in the `Kubernetes-deployment` repository.
- `ArgoCD` is our `GitOps` tool, Argo will rollout the deployment based on the image tag on `ECR`.
  - Note: `ArgoCD` is the one responsible managing the `lifecycle` of our Airflow instance in `Kubernetes`.
  - If you create a new `Airflow DAG`, the `CI/CD` will build a new image based on that, push to `ECR, update the `Kubernetes-deployment` repo.
  - Then, Argo` takes care of the rest in `Kubernetes`.

REPOSITORY LAYOUT
- `.github` ---> This is the directory containing our `CI/CD` Workflows.
- `business_logic` ---> This directory hold specific `DAG code logic` that will be imported in the DAG file.
- `config` ---> This is airflow config file that controls airflow behaviour locally, the one that controls airflow in production is in the [elite-airflow-values.yaml](https://github.com/Federated-Engineers/kubernetes-deployments/blob/main/applications/production-values/elite-airflow-values.yaml#L2836).
- `plugins` ---> This directory holds `non DAG specific modules`, this must be maintain for better readability.
- `Dockerfile` ---> The Dockerfile used by the `CI/CD Pipeline` to build the DAG in the repository.
- `docker-compose.yaml` ---> This is only valid for `local development` before a PR is opened. Since it uses the layout of what we have in `production, it's important to test locally before a `PR` is made.
- `requirements-dev.txt` ---> This is only used for the CI part of the CI/CD pipeline.
- `requirements.txt` ---> This is the actual file we use to manage `production dependencies`. If you need a new library for your DAG, simply add it here and start your `local development`.

