import json, utils, s3

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
    def __init__(self, model_id:str, functions, bedrock):
        self.model_id:str = model_id
        self.__make_body_by = functions.get(model_id)
        self.__bedrock = bedrock
        self.__config:dict = utils.load_config(file_name="config.yaml")


    def create_inputs_by(self, prompts:tuple) -> (str, str):
        inputs:str = create_input_of(
            prompts = prompts,
            make_body_by = self.__make_body_by,
        )
        bucket_name:str = self.__config.get('bucket_name')
        self.bucket = s3.Bucket(name=bucket_name)
        return self.__upload(inputs, bucket_name)


    def create_job(self, input_key:str, output_dir:str) -> dict:
        response:dict = self.__create_by(input_key, output_dir)
        self.arn = response.get("jobArn")
        self.id = self.arn.split("/")[-1].strip()
        self.get_job_info()
        return response


    def __upload(self, inputs:str, bucket_name:str) -> (str, str):
        self.__condition:str = f"{self.model_id}/{inputs.count('recordId')}"
        _output_dir:str = f"Bedrock/Batch-Inference/{self.__condition}"
        _input_key:str = f"{_output_dir}/input.jsonl"
        input_oblect = self.bucket.Object(key=_input_key)
        response:dict = input_oblect.put(Body=inputs)

        output_dir:str = f"s3://{bucket_name}/{_output_dir}/"
        input_key:str = f"{output_dir}input.jsonl"
        
        return input_key, output_dir


    def __create_by(self, input_key:str, output_dir:str) -> dict:
        place_of_input = ({
            "s3InputDataConfig": {
                "s3Uri": input_key
            }
        })
        place_of_output = ({
            "s3OutputDataConfig": {
                "s3Uri": output_dir
            }
        })
        response:dict = self.__bedrock.create_model_invocation_job(
            roleArn = self.__config.get("role"),
            modelId = self.model_id,
            jobName = f"{self.__condition}/{utils.get_formatted_time()}".replace('/', '-').replace(':', '-'),
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


    def wait(self):
        print(f"ID: {self.id}")
        print(f"Name: {self.name}")

        utils.wait_until_complete(
            get_status = self.get_status,
            stopped_status = ('Completed', 'Failed', 'Stopped'),
        )

        print(f"Error: {self.error}")
        print(f"Total time: {self.progress_time}")