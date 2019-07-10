inpath = "/scratch/wikipedia-dump/categorylinks.sql"
outpath = "/scratch/wikipedia-dump/categorylinks.json"

#test data
#inpath = "/home/ubuntu/BA_Project/spark/Python/seitzjon/test_data.sql"
#outpath = "/home/ubuntu/BA_Project/spark/Python/seitzjon/test_json.json"

class Entry:

    def __init__(self, id):
        self.id = id
        self.categories = []

    def add_category(self, category):
        self.categories.append('"' + category + '"')

    def to_json_object(self):
        json = "{"
        json = json + '"id": "{}", "category": ['.format(self.id)
        for category in self.categories:
            json = json + category + ', '
        if json[-2:] == ', ':
            json = json[:-2]
        json = json + "]}"
        return json

infile = open(inpath, "r", encoding="utf-8", errors="ignore")
outfile = open(outpath, "+a")
old = None
count = 1

line = infile.readline()

while line:
    print("starting", count)
    if line.startswith("INSERT INTO"):
        sub = line[36:-3]
        entries = sub.split("),(")
        for entry in entries:
            parts = entry.split(",")
            id = int(parts[0])
            if old == None:
                #first entry
                old = Entry(id)
            elif old.id != id:
                #no more categories for this entry
                outfile.write(old.to_json_object() + "\n")
                old = Entry(id)
            category = parts[1][1:-1]
            old.add_category(category)
    print("done with", count)
    count += 1
    line = infile.readline()

if old != None:
    outfile.write(old.to_json_object() + "\n")

outfile.close()
infile.close()
