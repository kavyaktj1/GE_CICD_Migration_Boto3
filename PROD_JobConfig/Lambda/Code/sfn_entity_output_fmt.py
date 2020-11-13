import json

def list_unpack(a):
    temp_dict = {}
    
    for i in a:
        temp_dict = i
        break
    
    return temp_dict


def lambda_handler(event, context):
    
    return_value = {}
    
    try:
        if isinstance(event, list):
            return_value = list_unpack(event)
        else:
            return_value.update({"Input": "Input not list type"})
            
        return return_value
    
    except Exception as e:
        print("Error {}".format(e))
        raise e