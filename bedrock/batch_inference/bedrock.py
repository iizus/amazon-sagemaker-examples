import json, boto3, utils, s3, pandas
from pprint import pprint


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




def create_input_of(prompts:tuple, make_body_by) -> str:
    jsonl = str()
    for record_id, prompt in enumerate(prompts):
        body:dict = make_body_by(prompt)
        record:dict = __make_record_by(record_id, body)
        jsonl = jsonl + record + "\n"
    else:
        return jsonl


def __make_record_by(record_id:int, body:dict) -> dict:
    record:dict = {
        "recordId": str(record_id).zfill(12),
        "modelInput": body
    }
    return json.dumps(record)


class Batch:
    def __init__(
        self,
        model_id:str,
        prompts:tuple,
        functions,
        bedrock,
    ):
        self.model_id:str = model_id

        inputs:str = create_input_of(
            prompts = prompts,
            make_body_by = functions.get(model_id)
        )
        self.__bedrock = bedrock
        config:dict = utils.load_config(file_name="config.yaml")
        
        self.__bucket_name=config.get("bucket_name")
        self.bucket = s3.Bucket(name=self.__bucket_name)
        response:dict = self.__upload(inputs)

        response:dict = self.__create(role=config.get("role"))

        self.arn = response.get("jobArn")
        self.id = self.arn.split("/")[-1].strip()
        self.get_job_info()


    def __upload(self, inputs:str) -> dict:
        self.__condition:str = f"{self.model_id}/{inputs.count('recordId')}"
        output_dir:str = f"Bedrock/Batch-Inference/{self.__condition}"
        input_key:str = f"{output_dir}/input.jsonl"
        input_oblect = self.bucket.Object(key=input_key)
        response:dict = input_oblect.put(Body=inputs)

        self.output_dir:str = f"s3://{self.__bucket_name}/{output_dir}/"
        self.input_key:str = f"{self.output_dir}input.jsonl"
        
        return response


    def __create(self, role:str) -> dict:
        place_of_input = ({
            "s3InputDataConfig": {
                "s3Uri": self.input_key
            }
        })
        place_of_output = ({
            "s3OutputDataConfig": {
                "s3Uri": self.output_dir
            }
        })
        response:dict = self.__bedrock.create_model_invocation_job(
            roleArn = role,
            modelId = self.model_id,
            jobName = f"{self.__condition}/{utils.get_formatted_time()}".replace("/", "-"),
            inputDataConfig = place_of_input,
            outputDataConfig = place_of_output
        )
        return response


    def get_status(self) -> str:
        job_info:dict = self.get_job_info()
        return job_info.get("status")


    def get_job_info(self) -> dict:
        job_info:dict = self.__bedrock.get_model_invocation_job(jobIdentifier=self.arn)
        
        self.name:str = job_info.get("jobName")
        self.status:str = job_info.get("status")
        self.error:str = job_info.get("message")
        self.submit_time:datetime = job_info.get("submitTime")
        self.last_modified_time:datetime = job_info.get("lastModifiedTime")
        
        self.progress_time:delta = self.last_modified_time - self.submit_time

        return job_info