from prefect import flow
from prefect.runner.storage import GitRepository
from prefect_github import GitHubCredentials
# from prefect.deployments import Deployment

def deploy():
 # 1. Load the GitHub credentials block
 # Note: Ensure the block name matches what you created in the UI
    github_creds = GitHubCredentials.load("git-api")

 # 2. Define the remote source (The "Pull" step)
    source = GitRepository(
        url="https://github.com/mai2204/RCV-to-L0-Data-Pipeline-with-Prefect-and-AWS",
        branch="main",
        credentials=github_creds
    )

 # 3. Create the deployment from the source
 # .from_source() replaces the 'pull' section of your YAML
    my_flow = flow.from_source(
        source=source,
        entrypoint="dag/L1/ecom/ecom_flow.py:ecom_flow"
    )

 # 4. Ship it to Prefect Cloud (The 'deployments' section)
    my_flow.deploy(
        name="ecom-deployment",
        work_pool_name="Serverless",
        job_variables={
            "pip_packages": "-r requirements.txt" 
        }
    )

    print(my_flow)

if __name__ == "__main__":
    deploy()