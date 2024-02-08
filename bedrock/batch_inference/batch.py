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






import boto3, utils

class Bucket:
    def __init__(name:str) -> Bucket:
        __s3 = boto3.resource('s3')
        self.bucket = __s3.Bucket(name=key)

    def Object(key:str) -> Object:
        return self.bucket.Object(key=key)



class Bedrock:
    def __init__(region:str="us-east-1") -> Bedrock:
        self.bedrock:boto3.client = boto3.client(
            service_name = "bedrock",
            region_name = region
        )

class Batch:
    def __init__(
        bedrock:Bedrock,
        model_id:str,
        number_of_images:int,
        inputs:dict
    ) -> Batch:
        self.__bedrock:Bedrock = bedrock
        self.model_id:str = model_id
        self.number_of_images:int = number_of_images
        self.__config = utils.load_config()
        self.__upload(inputs)
        __response:dict = self.__create_job()
        self.arn = __response.get("jobArn")
        self.id = self.arn.split("/")[-1].strip()


    def __upload(inputs) -> (str, str):
        __bucket_name = self.__config.get("bucket_name")
        __bucket = Bucket(name=__bucket_name)

        __condition:str = f"{self.model_id}/{self.number_of_images}"
        self.name:str = f"{__condition}/{utils.get_formatted_time()}".replace("/", "-")

        self.output_dir = f"s3://{__bucket_name}/Bedrock/Batch-Inference/{__condition}/"
        self.input_key = f"{self.output_dir}/input.jsonl"

        __input = __bucket.Object(key=self.input_key)
        __input.put(Body=inputs)


    def __create_job() -> dict:
        __place_of_input = ({
            "s3InputDataConfig": {
                "s3Uri": self.input_key
            }
        })
        __place_of_output = ({
            "s3OutputDataConfig": {
                "s3Uri": self.output_dir
            }
        })
        __response:dict = self.__bedrock.create_model_invocation_job(
            roleArn = self.__config.get("role"),
            modelId = self.model_id,
            jobName = self.name,
            inputDataConfig = __place_of_input,
            outputDataConfig = __place_of_output
        )
        return __response


    def get_status():
        self.info:dict = self.__bedrock.get_model_invocation_job(jobIdentifier=self.arn)