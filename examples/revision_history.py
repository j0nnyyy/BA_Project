from urllib.request import urlopen
import re


def get_revision_history(pageTitle):
    url = "https://en.wikipedia.org/w/api.php?action=query&format=xml&prop=revisions&rvlimit=500&titles=" + pageTitle
    revisions = []
    next = ''
    while True:
        response = urlopen(url + next).read()
        revisions += re.findall('<rev [^>]*>', response)

        cont = re.search('<continue rvcontinue="([^"]+)"', response)
        if not cont:
            break

        next = "&rvcontinue=" + cont.group(1)

    return revisions;


revisions = get_revision_history("Coffee")

print(len(revisions))