def create_input_of(prompts:tuple, make_body_by) -> str:
    jsonl = str()
    for record_id, prompt in enumerate(prompts):
        body:dict = make_body_by(prompt)
        record:dict = __make_record_by(record_id, body)
        jsonl = jsonl + record + "\n"
    else:
        return jsonl


import json

def __make_record_by(record_id:int, body:dict) -> dict:
    record:dict = {
        "recordId": str(record_id).zfill(12),
        "modelInput": body
    }
    return json.dumps(record)