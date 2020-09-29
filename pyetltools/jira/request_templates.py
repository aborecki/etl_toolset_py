import base64
import json


def get_authentication_headers(username, password):
    user_pass_base64=base64.b64encode(f"{username}:{password}".encode("ascii"))
    headers = {'Authorization': 'Basic ' + str(user_pass_base64, "utf-8"),
            'Content-Type': 'application/json'
          }
    return headers



def get_create_subissue_body_json(project_key, parent_key, subissue_summary, subissue_description):

    template=json.loads("""{
        "fields":
        {
            "project":
            {
                "key": ""
            },
            "parent":
            {
                "key": ""
            },
            "summary": "",
            "description": "",
            "issuetype":
             {
                "id": "5"
            }
        }
    }""")
    template["fields"]["project"]["key"]=project_key
    template["fields"]["parent"]["key"]=parent_key
    template["fields"]["summary"]=subissue_summary
    template["fields"]["description"] = subissue_description
    return json.dumps(template)
