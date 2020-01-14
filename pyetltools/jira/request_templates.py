import base64

def get_authentication_headers(username, password):
    user_pass_base64=base64.b64encode(f"{username}:{password}".encode("ascii"))
    headers = {'Authorization': 'Basic ' + str(user_pass_base64, "utf-8"),
            'Content-Type': 'application/json'
          }
    return headers


def get_create_subissue_body(project_key, parent_key, subissue_summary, subissue_description):
    subissue_description=subissue_description.replace("\"","\\\"")
    return f"""{{
        "fields":
        {{
            "project":
            {{
                "key": "{project_key}"
            }},
            "parent":
            {{
                "key": "{parent_key}"
            }},
            "summary": "{subissue_summary}",
            "description": "{subissue_description}",
            "issuetype":
             {{
                "id": "5"
            }}
        }}
    }}"""
