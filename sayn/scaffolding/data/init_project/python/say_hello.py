from sayn import task


@task
def say_hello(context):
    context.info("Hello!")
