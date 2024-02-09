import boto3, pandas


class Bedrock:
    def __init__(self, region:str="us-east-1"):
        self.client = boto3.client(
            service_name = "bedrock",
            region_name = region
        )

    def group_jobs_by_status(self) -> pandas.DataFrame:
        return self.get_dataframe_of_jobs().groupby("status").size()

    def get_dataframe_of_jobs(self) -> pandas.DataFrame:
        jobs:list = self.list_batch_jobs()
        return pandas.DataFrame(jobs).set_index("jobArn")

    def list_batch_jobs(self, max_results:int=1000, sort_order:str="Descending") -> list:
        jobs:list = self.client.list_model_invocation_jobs(
            maxResults = max_results,
            # statusEquals = status,
            # nameContains = name_contains,
            # submitTimeBefore = submit_time_before,
            # submitTimeAfter = submit_time_after,
            # sortBy = sort_by,
            sortOrder = sort_order,
            # nextToken = next_token,
        ).get("invocationJobSummaries")
        return jobs