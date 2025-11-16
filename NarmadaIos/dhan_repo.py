from db import db

userdetail = db["userdetail"]
document = userdetail.find_one({"name": "token"})


def get_token():
   print(document.dtoken)
   return document.dtoken