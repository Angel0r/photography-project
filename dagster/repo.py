from dagster import Definitions, job, op

@op
def noop():
    pass

@job
def skeleton_job():
    noop()

defs = Definitions(jobs=[skeleton_job])
