import json
import xmltodict

with open("1.xml", 'r') as f:
    xmlString = f.read()

jsonString = json.dumps(xmltodict.parse(xmlString), indent=4)
#print(jsonString)
with open("output1.json", 'w') as f:
    f.write(jsonString)