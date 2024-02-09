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
    jsonl_file_name:str = 'input.jsonl'
    config:dict = utils.load_config(file_name='config.yaml')
    bucket_name:str = config.get('bucket_name')
    bucket = s3.Bucket(name=bucket_name)

    def __init__(self, model_id:str, functions, bedrock):
        self.model_id:str = model_id
        self.__make_body_by = functions.get(model_id)
        self.__bedrock = bedrock


    def create_inputs_by(self, prompts:tuple) -> (str, str):
        inputs:str = create_input_of(
            prompts = prompts,
            make_body_by = self.__make_body_by,
        )
        return self.__upload(inputs)


    def create_job(self, condition:str, output_dir:str) -> dict:
        response:dict = self.__create_by(condition, output_dir)
        self.arn = response.get("jobArn")
        self.id = self.arn.split("/")[-1].strip()
        self.get_job_info()
        return response


    def __upload(self, inputs:str) -> (str, str):
        condition:str = f"{self.model_id}/{inputs.count('recordId')}"
        output_dir:str = f"Bedrock/Batch-Inference/{condition}"
        input_key:str = f"{_output_dir}/{jsonl_file_name}"
        input_object = bucket.Object(key=_input_key)
        response:dict = input_object.put(Body=inputs)
        return condition, output_dir


    def __create_by(self, condition:str, output_dir:str) -> dict:
        _output_dir:str = f"s3://{bucket_name}/{output_dir}/"
        return self.__bedrock.create_job(
            model_id = self.model_id,
            job_name = condition,
            input_key = f"{_output_dir}{jsonl_file_name}",
            output_dir = output_dir,
        )

    def get_status(self) -> str:
        job_info:dict = self.get_job_info()
        return job_info.get("status")


    def get_job_info(self) -> dict:
        job_info:dict = self.__bedrock.get_job_info(job_arn=self.arn)
        
        self.name:str = job_info.get("jobName")
        self.status:str = job_info.get("status")
        self.error:str = job_info.get("message")
        self.submit_time:datetime = job_info.get("submitTime")
        self.last_modified_time:datetime = job_info.get("lastModifiedTime")
        
        self.progress_time:delta = self.last_modified_time - self.submit_time

        return job_info


    def stop(self) -> dict:
        return self.__bedrock.stop_job(job_arn=self.arn)


    def wait(self):
        print(f"ID: {self.id}")
        print(f"Name: {self.name}")
        utils.wait_until_complete(
            get_status = self.get_status,
            stopped_status = ('Completed', 'Failed', 'Stopped'),
        )
        print(f"Error: {self.error}")
        print(f"Total time: {self.progress_time}")