from functools import reduce

from urllib.request import urlopen
from lxml import etree
from xmltodict import parse
import json


class ECB:
    def __init__(self):
        self.url = "https://sdw-wsrest.ecb.europa.eu/service"
        self.namespaces = {
            "xsi":"http://www.w3.org/2001/XMLSchema-instance",
            "xml":"http://www.w3.org/XML/1998/namespace",
            "mes":"http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
            "str":"http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
            "com":"http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
            "gen":"http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic"
        }
    
    def getflowids(self):
        root = etree.XML(urlopen("%s/dataflow" % self.url).read())
        return root.xpath("//str:Dataflow/@id", namespaces=self.namespaces)
    
    def getflowinfo(self, fid):
        root = etree.XML(urlopen("%s/dataflow" % self.url).read())
        li = root.xpath("//str:Dataflow[@id='%s']" % fid, namespaces=self.namespaces)
        l = [(l.attrib,[parse(etree.tostring(c)) for c in l.getchildren()]) for l in li]
        d = [l[0][0],] + l[0][1]
        return reduce(lambda a, b: dict(a, **b), d)

#params:startPeriod=value&endPeriod=value&updatedAfter=value&firstNObservations=value&lastNObservations=value&detail=value&includeHistory=value
    def getflowdata(self, fid, key, params):
        if params is None:
            params = ""
        root = etree.XML(urlopen("%s/data/%s/%s%s" % (self.url, fid, key, "?" + params if params != "" else "")).read())
        raws = (etree.tostring(r) for r in root.xpath("//mes:DataSet/gen:Series", namespaces=self.namespaces))
        
        def _parse():
            for r in raws:
                S = parse(r)["generic:Series"]
                skey = {s["@id"]:s["@value"] for s in S["generic:SeriesKey"]["generic:Value"]}
                sattr = {s["@id"]:s["@value"] for s in S["generic:Attributes"]["generic:Value"]}
                sobs = {s["generic:ObsDimension"]["@value"]:s["generic:ObsValue"]["@value"] for s in S["generic:Obs"]}
                #sobsattr = {s["generic:ObsDimension"]["@value"]:{ss["@id"]:ss["@value"] for ss in s["generic:Attributes"]["generic:Value"]} for s in S["generic:Obs"]}
                yield {"key": skey, "attrs": sattr, "obs": sobs, "obsattrs": None}
        #return raws
        return _parse()