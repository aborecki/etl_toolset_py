import base64

def get_authentication_headers(username, password):
    user_pass_base64=base64.b64encode(f"{username}:{password}".encode("ascii"))
    headers = {'Authorization': 'Basic ' + str(user_pass_base64, "utf-8"),
            'Content-Type': 'application/json'
          }
    return headers

