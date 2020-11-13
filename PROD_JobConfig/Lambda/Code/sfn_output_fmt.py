import json

def list_unpack(a):
    temp_dict = {}

    for i in a:
        # print(i)
        for k in i.keys():
            try:
                if k != 'INPUT':
                    print(k)
                    # print(json.dumps(i, indent=4))
                    # print("{}".format(json.loads(i[k]["Output"])))
                    temp_dict = json.loads(i[k]["Output"])

            except KeyError:
                pass
            except Exception as e:
                print("Error {}".format(e))

    return temp_dict

def unpack_dict(d):
    temp_dict  = {}
    try:
        for k in d.keys():
            if k != "INPUT":
                temp_dict = d[k]["Output"]
    except KeyError:
        pass
    except Exception as e:
        print("Dict error {}".format(e))

    return temp_dict

def lambda_handler(event, context):
    temp_dict = {}

    print("{} {}".format(event, type(event)))

    if isinstance(event, str):
        temp_dict = json.loads(event)["Output"]

    elif isinstance(event, list):
        temp_dict = list_unpack(event)

    elif isinstance(event, dict):
        print("extracted input {}".format(event))
        temp_dict = unpack_dict(event)
        print("after {} {}".format(event, type(event)))

    return temp_dict