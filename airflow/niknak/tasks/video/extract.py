from airflow.decorators import task

@task
def extract():
    from niknak.utils.hello import hello

    return hello()