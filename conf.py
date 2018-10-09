from xml.etree.ElementTree import parse
targetXML = open("conf.xml",'r')

tree = parse(targetXML)

root = tree.getroot()

for i in root.findall('property'):
    print(i.findtext('name'))