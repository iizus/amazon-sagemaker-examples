import boto3, pandas, utils


class Bedrock:
    def __init__(self, region:str="us-east-1"):
        self.client = boto3.client(
            service_name = "bedrock",
            region_name = region
        )
        config:dict = utils.load_config(file_name='config.yaml')
        self.__role:str = config.get('role')

    def group_jobs_by_status(self) -> pandas.DataFrame:
        jobs:pandas.DataFrame = self.get_dataframe_of_jobs()
        print(jobs.sum(axis='status'))
        return jobs.groupby("status").size()

    def get_dataframe_of_jobs(self, max_results:int=1000) -> pandas.DataFrame:
        jobs:list = self.list_batch_jobs(max_results=max_results)
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

    def create_job(self, model_id:str, job_name:str, input_key:str, output_dir:str) -> dict:
        place_of_input:tuple = ({
            "s3InputDataConfig": {
                "s3Uri": input_key
            }
        })
        place_of_output:tuple = ({
            "s3OutputDataConfig": {
                "s3Uri": output_dir
            }
        })
        return self.client.create_model_invocation_job(
            roleArn = self.__role,
            modelId = model_id,
            jobName = f"{job_name}-{utils.get_formatted_time()}".replace('/', '-').replace(':', '-'),
            inputDataConfig = place_of_input,
            outputDataConfig = place_of_output
        )

    def stop_job(self, job_arn:str) -> dict:
        return self.client.stop_model_invocation_job(jobIdentifier=job_arn)

    def get_job_info(self, job_arn:str) -> dict:
        return self.client.get_model_invocation_job(jobIdentifier=job_arn)