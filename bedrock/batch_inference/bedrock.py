# from time import perf_counter, sleep
# from boto3 import client
# bedrock = client(service_name="bedrock", region_name=region)


# def get_job_status_of(job_arn:str) -> str:
#     job:dict = bedrock.get_model_invocation_job(jobIdentifier=job_arn)
#     job_status:str = job.get("status")
#     sleep(1)
#     return job_status


# def wait(job_arn:str, monitored_status:str) -> str:
#     job_status:str = monitored_status
#     print(job_status)

#     started_time = perf_counter()
#     while job_status == monitored_status:
#         job_status:str = get_job_status_of(job_arn)
#     ended_time = perf_counter()

#     time_delta = round(ended_time - started_time)
#     print(f" | {time_delta} sec")
#     return job_status


# def wait_until_complete(job_arn:str):
#     print("----- Job status -----")
#     job_status:str = wait(job_arn=job_arn, monitored_status="Submitted")
#     job_status:str = wait(job_arn=job_arn, monitored_status=job_status)
#     print(job_status)

#     error_message:str = bedrock.get_model_invocation_job(jobIdentifier=job_arn).get("message")
#     print(error_message)
#     ! aws s3 cp $output_dir$job_id/manifest.json.out -



# response = bedrock.create_model_invocation_job(
#     roleArn = role,
#     modelId = model_id,
#     jobName = job_name,
#     inputDataConfig = inputDataConfig,
#     outputDataConfig = outputDataConfig
# )
# from pprint import pprint
# # pprint(response, width=30, compact=False)

# job_arn = response.get("jobArn")
# job_id = job_arn.split("/")[-1].strip()
# print(job_id)

# wait_until_complete(job_arn)


import json, boto3, utils
from pprint import pprint

class Bucket:
    def __init__(self, name:str):
        __s3 = boto3.resource('s3')
        self.bucket = __s3.Bucket(name=name)

    def Object(self, key:str):
        return self.bucket.Object(key=key)


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
        region:str = "us-east-1",
    ):
        self.model_id:str = model_id

        inputs:str = create_input_of(
            prompts = prompts,
            make_body_by = functions.get(model_id)
        )
        # pprint(inputs)

        self.__bedrock = boto3.client(
            service_name = "bedrock",
            region_name = region
        )
        config:dict = utils.load_config(file_name="config.yaml")
        
        self.__bucket_name=config.get("bucket_name")
        self.bucket = Bucket(name=self.__bucket_name)
        response:dict = self.__upload(inputs)

        response:dict = self.__create(role=config.get("role"))

        self.arn = response.get("jobArn")
        self.id = self.arn.split("/")[-1].strip()

        pprint(self.get_status())
        

    def __upload(self, inputs:str) -> dict:
        self.__condition:str = f"{self.model_id}/{inputs.count('recordId')}"
        self.output_dir:str = f"s3://{self.__bucket_name}/Bedrock/Batch-Inference/{self.__condition}/"
        self.input_key:str = f"{self.output_dir}input.jsonl"

        input_oblect = self.bucket.Object(key=self.input_key)
        response:dict = input_oblect.put(Body=inputs)
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


    def get_status(self) -> dict:
        __job_info:dict = self.__bedrock.get_model_invocation_job(jobIdentifier=self.arn)
        
        self.name:str = __job_info.get("jobName")
        self.status:str = __job_info.get("status")
        self.error:str = __job_info.get("message")
        self.submit_time:datetime = __job_info.get("submitTime")
        self.last_modified_time:datetime = __job_info.get("lastModifiedTime")
        
        self.progress_time:delta = self.last_modified_time - self.submit_time

        return __job_info