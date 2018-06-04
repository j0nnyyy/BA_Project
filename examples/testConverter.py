import json
import xmltodict

xml_file = "11.xml"
json_file = "output.json"


def xml_to_json():
    with open(xml_file, 'r') as f:
        xmlString = f.read()

    jsonString = json.dumps(xmltodict.parse(xmlString), indent=None)

    print("\nJSON output(output.json):")
    print(jsonString)

    with open(json_file, 'w') as f:
        f.write(jsonString)
        f.write("\n")

xml_to_json()