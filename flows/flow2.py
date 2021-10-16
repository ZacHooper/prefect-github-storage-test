from prefect import task, Flow
from prefect.run_configs import DockerRun
from prefect.storage.github import GitHub
from time import sleep

def sleepy_function(func):
    def run():
        func()
        sleep(1)
    return run

@sleepy_function
@task
def extract():
    print("Doing the Extract for ETL1")

@sleepy_function
@task
def transfrom():
    print("Doing the transform for ETL1")

@sleepy_function
@task
def load():
    print("Doing the load for ETL1")

with Flow("prefect-docker-example") as flow:
    extract()
    transfrom()
    load()
    
# flow.run() # If you want to test the flow uncomment this line and run the python script

flow.run_config = DockerRun(
    image="zhooper/prefect-docker-example-etl2"
)

flow.storage = GitHub(repo="ZacHooper/prefect-github-storage-test", path="/flows/flow.py")

flow.register("prefect-docker-test")