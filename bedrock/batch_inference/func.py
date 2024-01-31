import json
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


import boto3
import base64

def generate_image(model_id, body):
    logger.info(f"Generating image with {model_id} model.")

    bedrock = boto3.client(service_name='bedrock-runtime')
    accept = "application/json"
    content_type = "application/json"

    response = bedrock.invoke_model(
        body = body,
        modelId = model_id,
        accept = accept,
        contentType = content_type
    )
    response_body = json.loads(response.get("body").read())
    # print(response_body)

    if "titan" in model_id:
        base64_image = response_body.get("images")[0]
    elif "stable" in model_id:
        base64_image = response_body.get("artifacts")[0].get("base64")
    
    base64_bytes = base64_image.encode('ascii')
    image_bytes = base64.b64decode(base64_bytes)

    finish_reason = response_body.get("error")

    if finish_reason is not None:
        raise ImageError(f"Image generation error. Error is {finish_reason}")

    logger.info(f"Successfully generated image with {model_id} model.")

    return image_bytes


from PIL import Image
import io

def display_image(image_bytes):
    image = Image.open(io.BytesIO(image_bytes))
    image.show()


class ImageError(Exception):
    def __init__(self, message):
        self.message = message

from botocore.exceptions import ClientError

def generate_image_by(model_id:str, body:dict) -> None:
    logging.basicConfig(
        level = logging.INFO,
        format = "%(levelname)s: %(message)s"
    )
    body = json.dumps(body)

    try:
        image_bytes = generate_image(
            model_id = model_id,
            body = body
        )
        display_image(image_bytes)        
    except ClientError as err:
        message = err.response["Error"]["Message"]
        logger.error("A client error occurred: %s", message)
        print("A client error occured: " + format(message))
    except ImageError as err:
        logger.error(err.message)
        print(err.message)

    else:
        print(f"Finished generating image with {model_id} model.")