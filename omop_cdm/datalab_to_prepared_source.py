import logging
import json
import os
import argparse
import csv
import hashlib
import sys

try:
    from mapping_classes import InputClass
except ImportError:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.split(__file__)[0], os.path.pardir, "src")))

from mapping_classes import InputClass

from mapping_classes import OutputClassCSVRealization, InputOutputMapperDirectory, OutputClassDirectory, \
            CoderMapperJSONClass, TransformMapper, FunctionMapper, FilterHasKeyValueMapper, ChainMapper, CascadeKeyMapper, \
            CascadeMapper, KeyTranslator, PassThroughFunctionMapper, CodeMapperDictClass, CodeMapperDictClass, ConstantMapper, \
            ReplacementMapper, MapperClass

from prepared_source_classes import SourcePersonObject, SourceCareSiteObject, SourceEncounterObject, \
        SourceObservationPeriodObject, SourceEncounterCoverageObject, SourceResultObject, SourceConditionObject, \
        SourceProcedureObject, SourceMedicationObject, SourceLocationObject, SourceEncounterDetailObject

from source_to_cdm_functions import generate_mapper_obj, IntFloatMapper
from utility_functions import generate_observation_period

from prepared_source_functions import build_name_lookup_csv, build_key_func_dict

logging.basicConfig(level=logging.INFO)




class PopulationDemographics(InputClass):
    def fields(self):
        return ["empiPersonId", "gender_code", "gender_code_oid", "gender_code_text", "birthsex_code", "birthsex_code_oid",
                "birthsex_code_text", "birthdate", "dateofdeath", "zip_code", "race_code", "race_code_oid", "race_code_text",
                "ethnicity_code", "ethnicity_code_oid", "ethnicity_code_text", "active"]


class PopulationEncounter(InputClass):
    def fields(self):
        return ["encounterid", "empiPersonId", "hospitalizationstartdate", "readmission", "dischargedate", "servicedate",
                "financialclass_code", "financialclass_code_oid", "financialclass_code_text", "hospitalservice_code",
                "hospitalservice_code_oid", "hospitalservice_code_text", "classfication_code", "classification_code_oid",
                "classification_code_text", "type_code", "type_code_oid", "type_code_text", "dischargedisposition_code",
                "dischargedisposition_code_oid", "dischargedisposition_code_text", "dischargetolocation_code",
                "dischargetolocation_code_oid", "dischargetolocation_code_text", "admissionsource_code",
                "admissionsource_code_oid", "admissionsource_code_text", "admissiontype_code", "admissiontype_code_oid",
                "admissiontype_code_text", "status_code", "status_code_oid", "status_code_text", "estimatedarrivaldate",
                "estimateddeparturedate", "actualarrivaldate", "source", "active"]


class PopulationCondition(InputClass):
    def fields(self):
        return ["conditionid", "empiPersonId", "encounterid", "condition_code", "condition_code_oid", "condition_code_text",
                "effectiveDate", "billingrank", "presentonadmission_code", "presentonadmission_code_oid",
                "presentonadmission_text", "type_primary_code", "type_primary_code_oid", "type_primary_text",
                "source"]


class PopulationProcedure(InputClass):
    def fields(self):
        return ["procedureid", "empiPersonId", "encounterid", "procedure_code", "procedure_code_oid",
                "procedure_code_display", "modifier_code", "modifier_oid", "modifier_text", "servicestartdate",
                "serviceenddate", "status_code", "status_oid", "active"]


class PopulationMedication(InputClass):
    def fields(self):
        return ["medicationid", "encounterid", "empiPersonId", "intendeddispenser", "startdate", "stopdate", "doseunit_code",
                "doseunit_code_oid", "doseunit_code_text", "category_id", "category_code_oid", "category_code_text",
                "frequency_id", "frequency_code_oid", "frequency_code_text", "status_code", "status_code_oid",
                "status_code_text", "route_code", "route_code_oid", "route_code_text", "drug_code", "drug_code_oid",
                "drug_code_text", "dosequantity", "source", "detailLine"]


class PopulationResult(InputClass):
    def fields(self):
        return ["resultid", "encounterid", "empiPersonId", "result_code", "result_code_oid", "result_code_text",
                "result_type", "servicedate", "value_text", "value_numeric", "value_numeric_modifier", "unit_code",
                "unit_code_oid", "unit_code_text", "value_codified_code", "value_codified_code_oid",
                "value_codified_code_text", "date", "interpretation_code", "interpretation_code_oid",
                "interpretation_code_text", "specimen_type_code", "specimen_type_code_oid", "specimen_type_code_text",
                "bodysite_code", "bodysite_code_oid", "bodysite_code_text", "specimen_collection_date",
                "specimen_received_date", "measurementmethod_code", "measurementmethod_code_oid",
                "measurementmethod_code_text", "recordertype", "issueddate", "year"]


class PopulationObservationPeriod(InputClass):
    def fields(self):
        return []


class PopulationCareSite(InputClass):
    def fields(self):
        return []


class AddressLookup(InputClass):
    def fields(self):
        return []


class PopulationEncounterLocation(InputClass):
    def fields(self):
        return []

class DuplicateExcludeMapper(MapperClass):
    """Indicates that a row is a duplicate"""
    def __init__(self, id_field):
        self.id_field = id_field
        self.id_dict = {"i_exclude": ""}

    def map(self, input_dict):
        if self.id_field in input_dict:
            id_value = input_dict[self.id_field]

            if id_value in self.id_dict:
                return {"i_exclude": 1}

            else:
                self.id_dict[id_value] = 1
                return {"i_exclude": ""}

        else:
            return {}


def main(input_csv_directory, output_csv_directory, file_name_dict):

    output_class_obj = OutputClassDirectory()
    in_out_map_obj = InputOutputMapperDirectory()

    location_lookup_csv = os.path.join(input_csv_directory, "address_lookup.csv")

    address_csv = os.path.join(input_csv_directory, "population_address.csv")

    md5_func = lambda x: hashlib.md5(x.encode("utf8")).hexdigest()

    source_location_csv = os.path.join(output_csv_directory, "source_location.csv")

    key_location_mapper = build_name_lookup_csv(address_csv, location_lookup_csv,
                                                ["street_1", "street_2", "city", "state", "zip_code"],
                                                ["street_1", "street_2", "city", "state", "zip_code"], hashing_func=md5_func)

    key_address_name_mapper = FunctionMapper(
        build_key_func_dict(["street_1", "street_2", "city", "state", "zip_code"], separator="|"))

    # k_location,s_address_1,s_address_2,s_city,s_state,s_zip,s_county,s_location_name
    location_rules = [("key_name", "k_location"),
                      (("street_1", "street_2", "city", "state",
                        "zip_code"),
                       key_address_name_mapper,
                       {"mapped_value": "s_location_name"}),
                      ("street_1", "s_address_1"),
                      ("street_2", "s_address_2"),
                      ("city", "s_city"),
                      ("state", "s_state"),
                      ("zip_code", "s_zip")
                      ]

    location_runner_obj = generate_mapper_obj(location_lookup_csv, AddressLookup(), source_location_csv,
                                              SourceLocationObject(), location_rules,
                                              output_class_obj, in_out_map_obj)

    location_runner_obj.run()

    input_patient_file_name = os.path.join(input_csv_directory, file_name_dict["demographic"])

    # Source: https://www.hl7.org/fhir/v3/Race/cs.html
    hl7_race_dict = {
        "1002-5": "American Indian or Alaska Native",
        "1004-1": "American Indian",
        "1006-6": "Abenaki",
        "1008-2": "Algonquian",
        "1010-8": "Apache",
        "1011-6": "Chiricahua",
        "1012-4": "Fort Sill Apache",
        "1013-2": "Jicarilla Apache",
        "1014-0": "Lipan Apache",
        "1015-7": "Mescalero Apache",
        "1016-5": "Oklahoma Apache",
        "1017-3": "Payson Apache",
        "1018-1": "San Carlos Apache",
        "1019-9": "White Mountain Apache",
        "1021-5": "Arapaho",
        "1022-3": "Northern Arapaho",
        "1023-1": "Southern Arapaho",
        "1024-9": "Wind River Arapaho",
        "1026-4": "Arikara",
        "1028-0": "Assiniboine",
        "1030-6": "Assiniboine Sioux",
        "1031-4": "Fort Peck Assiniboine Sioux",
        "1033-0": "Bannock",
        "1035-5": "Blackfeet",
        "1037-1": "Brotherton",
        "1039-7": "Burt Lake Band",
        "1041-3": "Caddo",
        "1042-1": "Oklahoma Cado",
        "1044-7": "Cahuilla",
        "1045-4": "Agua Caliente Cahuilla",
        "1046-2": "Augustine",
        "1047-0": "Cabazon",
        "1048-8": "Los Coyotes",
        "1049-6": "Morongo",
        "1050-4": "Santa Rosa Cahuilla",
        "1051-2": "Torres-Martinez",
        "1053-8": "California Tribes",
        "1054-6": "Cahto",
        "1055-3": "Chimariko",
        "1056-1": "Coast Miwok",
        "1057-9": "Digger",
        "1058-7": "Kawaiisu",
        "1059-5": "Kern River",
        "1060-3": "Mattole",
        "1061-1": "Red Wood",
        "1062-9": "Santa Rosa",
        "1063-7": "Takelma",
        "1064-5": "Wappo",
        "1065-2": "Yana",
        "1066-0": "Yuki",
        "1068-6": "Canadian and Latin American Indian",
        "1069-4": "Canadian Indian",
        "1070-2": "Central American Indian",
        "1071-0": "French American Indian",
        "1072-8": "Mexican American Indian",
        "1073-6": "South American Indian",
        "1074-4": "Spanish American Indian",
        "1076-9": "Catawba",
        "1741-8": "Alatna",
        "1742-6": "Alexander",
        "1743-4": "Allakaket",
        "1744-2": "Alanvik",
        "1745-9": "Anvik",
        "1746-7": "Arctic",
        "1747-5": "Beaver",
        "1748-3": "Birch Creek",
        "1749-1": "Cantwell",
        "1750-9": "Chalkyitsik",
        "1751-7": "Chickaloon",
        "1752-5": "Chistochina",
        "1753-3": "Chitina",
        "1754-1": "Circle",
        "1755-8": "Cook Inlet",
        "1756-6": "Copper Center",
        "1757-4": "Copper River",
        "1758-2": "Dot Lake",
        "1759-0": "Doyon",
        "1760-8": "Eagle",
        "1761-6": "Eklutna",
        "1762-4": "Evansville",
        "1763-2": "Fort Yukon",
        "1764-0": "Gakona",
        "1765-7": "Galena",
        "1766-5": "Grayling",
        "1767-3": "Gulkana",
        "1768-1": "Healy Lake",
        "1769-9": "Holy Cross",
        "1770-7": "Hughes",
        "1771-5": "Huslia",
        "1772-3": "Iliamna",
        "1773-1": "Kaltag",
        "1774-9": "Kluti Kaah",
        "1775-6": "Knik",
        "1776-4": "Koyukuk",
        "1777-2": "Lake Minchumina",
        "1778-0": "Lime",
        "1779-8": "Mcgrath",
        "1780-6": "Manley Hot Springs",
        "1781-4": "Mentasta Lake",
        "1782-2": "Minto",
        "1783-0": "Nenana",
        "1784-8": "Nikolai",
        "1785-5": "Ninilchik",
        "1786-3": "Nondalton",
        "1787-1": "Northway",
        "1788-9": "Nulato",
        "1789-7": "Pedro Bay",
        "1790-5": "Rampart",
        "1791-3": "Ruby",
        "1792-1": "Salamatof",
        "1793-9": "Seldovia",
        "1794-7": "Slana",
        "1795-4": "Shageluk",
        "1796-2": "Stevens",
        "1797-0": "Stony River",
        "1798-8": "Takotna",
        "1799-6": "Tanacross",
        "1800-2": "Tanaina",
        "1801-0": "Tanana",
        "1802-8": "Tanana Chiefs",
        "1803-6": "Tazlina",
        "1804-4": "Telida",
        "1805-1": "Tetlin",
        "1806-9": "Tok",
        "1807-7": "Tyonek",
        "1808-5": "Venetie",
        "1809-3": "Wiseman",
        "1078-5": "Cayuse",
        "1080-1": "Chehalis",
        "1082-7": "Chemakuan",
        "1083-5": "Hoh",
        "1084-3": "Quileute",
        "1086-8": "Chemehuevi",
        "1088-4": "Cherokee",
        "1089-2": "Cherokee Alabama",
        "1090-0": "Cherokees of Northeast Alabama",
        "1091-8": "Cherokees of Southeast Alabama",
        "1092-6": "Eastern Cherokee",
        "1093-4": "Echota Cherokee",
        "1094-2": "Etowah Cherokee",
        "1095-9": "Northern Cherokee",
        "1096-7": "Tuscola",
        "1097-5": "United Keetowah Band of Cherokee",
        "1098-3": "Western Cherokee",
        "1100-7": "Cherokee Shawnee",
        "1102-3": "Cheyenne",
        "1103-1": "Northern Cheyenne",
        "1104-9": "Southern Cheyenne",
        "1106-4": "Cheyenne-Arapaho",
        "1108-0": "Chickahominy",
        "1109-8": "Eastern Chickahominy",
        "1110-6": "Western Chickahominy",
        "1112-2": "Chickasaw",
        "1114-8": "Chinook",
        "1115-5": "Clatsop",
        "1116-3": "Columbia River Chinook",
        "1117-1": "Kathlamet",
        "1118-9": "Upper Chinook",
        "1119-7": "Wakiakum Chinook",
        "1120-5": "Willapa Chinook",
        "1121-3": "Wishram",
        "1123-9": "Chippewa",
        "1124-7": "Bad River",
        "1125-4": "Bay Mills Chippewa",
        "1126-2": "Bois Forte",
        "1127-0": "Burt Lake Chippewa",
        "1128-8": "Fond du Lac",
        "1129-6": "Grand Portage",
        "1130-4": "Grand Traverse Band of Ottawa-Chippewa",
        "1131-2": "Keweenaw",
        "1132-0": "Lac Courte Oreilles",
        "1133-8": "Lac du Flambeau",
        "1134-6": "Lac Vieux Desert Chippewa",
        "1135-3": "Lake Superior",
        "1136-1": "Leech Lake",
        "1137-9": "Little Shell Chippewa",
        "1138-7": "Mille Lacs",
        "1139-5": "Minnesota Chippewa",
        "1140-3": "Ontonagon",
        "1141-1": "Red Cliff Chippewa",
        "1142-9": "Red Lake Chippewa",
        "1143-7": "Saginaw Chippewa",
        "1144-5": "St. Croix Chippewa",
        "1145-2": "Sault Ste. Marie Chippewa",
        "1146-0": "Sokoagon Chippewa",
        "1147-8": "Turtle Mountain",
        "1148-6": "White Earth",
        "1150-2": "Chippewa Cree",
        "1151-0": "Rocky Boy's Chippewa Cree",
        "1153-6": "Chitimacha",
        "1155-1": "Choctaw",
        "1156-9": "Clifton Choctaw",
        "1157-7": "Jena Choctaw",
        "1158-5": "Mississippi Choctaw",
        "1159-3": "Mowa Band of Choctaw",
        "1160-1": "Oklahoma Choctaw",
        "1162-7": "Chumash",
        "1163-5": "Santa Ynez",
        "1165-0": "Clear Lake",
        "1167-6": "Coeur D'Alene",
        "1169-2": "Coharie",
        "1171-8": "Colorado River",
        "1173-4": "Colville",
        "1175-9": "Comanche",
        "1176-7": "Oklahoma Comanche",
        "1178-3": "Coos, Lower Umpqua, Siuslaw",
        "1180-9": "Coos",
        "1182-5": "Coquilles",
        "1184-1": "Costanoan",
        "1186-6": "Coushatta",
        "1187-4": "Alabama Coushatta",
        "1189-0": "Cowlitz",
        "1191-6": "Cree",
        "1193-2": "Creek",
        "1194-0": "Alabama Creek",
        "1195-7": "Alabama Quassarte",
        "1196-5": "Eastern Creek",
        "1197-3": "Eastern Muscogee",
        "1198-1": "Kialegee",
        "1199-9": "Lower Muscogee",
        "1200-5": "Machis Lower Creek Indian",
        "1201-3": "Poarch Band",
        "1202-1": "Principal Creek Indian Nation",
        "1203-9": "Star Clan of Muscogee Creeks",
        "1204-7": "Thlopthlocco",
        "1205-4": "Tuckabachee",
        "1207-0": "Croatan",
        "1209-6": "Crow",
        "1211-2": "Cupeno",
        "1212-0": "Agua Caliente",
        "1214-6": "Delaware",
        "1215-3": "Eastern Delaware",
        "1216-1": "Lenni-Lenape",
        "1217-9": "Munsee",
        "1218-7": "Oklahoma Delaware",
        "1219-5": "Rampough Mountain",
        "1220-3": "Sand Hill",
        "1222-9": "Diegueno",
        "1223-7": "Campo",
        "1224-5": "Capitan Grande",
        "1225-2": "Cuyapaipe",
        "1226-0": "La Posta",
        "1227-8": "Manzanita",
        "1228-6": "Mesa Grande",
        "1229-4": "San Pasqual",
        "1230-2": "Santa Ysabel",
        "1231-0": "Sycuan",
        "1233-6": "Eastern Tribes",
        "1234-4": "Attacapa",
        "1235-1": "Biloxi",
        "1236-9": "Georgetown",
        "1237-7": "Moor",
        "1238-5": "Nansemond",
        "1239-3": "Natchez",
        "1240-1": "Nausu Waiwash",
        "1241-9": "Nipmuc",
        "1242-7": "Paugussett",
        "1243-5": "Pocomoke Acohonock",
        "1244-3": "Southeastern Indians",
        "1245-0": "Susquehanock",
        "1246-8": "Tunica Biloxi",
        "1247-6": "Waccamaw-Siousan",
        "1248-4": "Wicomico",
        "1250-0": "Esselen",
        "1252-6": "Fort Belknap",
        "1254-2": "Fort Berthold",
        "1256-7": "Fort Mcdowell",
        "1258-3": "Fort Hall",
        "1260-9": "Gabrieleno",
        "1262-5": "Grand Ronde",
        "1264-1": "Gros Ventres",
        "1265-8": "Atsina",
        "1267-4": "Haliwa",
        "1269-0": "Hidatsa",
        "1271-6": "Hoopa",
        "1272-4": "Trinity",
        "1273-2": "Whilkut",
        "1275-7": "Hoopa Extension",
        "1277-3": "Houma",
        "1279-9": "Inaja-Cosmit",
        "1281-5": "Iowa",
        "1282-3": "Iowa of Kansas-Nebraska",
        "1283-1": "Iowa of Oklahoma",
        "1285-6": "Iroquois",
        "1286-4": "Cayuga",
        "1287-2": "Mohawk",
        "1288-0": "Oneida",
        "1289-8": "Onondaga",
        "1290-6": "Seneca",
        "1291-4": "Seneca Nation",
        "1292-2": "Seneca-Cayuga",
        "1293-0": "Tonawanda Seneca",
        "1294-8": "Tuscarora",
        "1295-5": "Wyandotte",
        "1297-1": "Juaneno",
        "1299-7": "Kalispel",
        "1301-1": "Karuk",
        "1303-7": "Kaw",
        "1305-2": "Kickapoo",
        "1306-0": "Oklahoma Kickapoo",
        "1307-8": "Texas Kickapoo",
        "1309-4": "Kiowa",
        "1310-2": "Oklahoma Kiowa",
        "1312-8": "Klallam",
        "1313-6": "Jamestown",
        "1314-4": "Lower Elwha",
        "1315-1": "Port Gamble Klallam",
        "1317-7": "Klamath",
        "1319-3": "Konkow",
        "1321-9": "Kootenai",
        "1323-5": "Lassik",
        "1325-0": "Long Island",
        "1326-8": "Matinecock",
        "1327-6": "Montauk",
        "1328-4": "Poospatuck",
        "1329-2": "Setauket",
        "1331-8": "Luiseno",
        "1332-6": "La Jolla",
        "1333-4": "Pala",
        "1334-2": "Pauma",
        "1335-9": "Pechanga",
        "1336-7": "Soboba",
        "1337-5": "Twenty-Nine Palms",
        "1338-3": "Temecula",
        "1340-9": "Lumbee",
        "1342-5": "Lummi",
        "1344-1": "Maidu",
        "1345-8": "Mountain Maidu",
        "1346-6": "Nishinam",
        "1348-2": "Makah",
        "1350-8": "Maliseet",
        "1352-4": "Mandan",
        "1354-0": "Mattaponi",
        "1356-5": "Menominee",
        "1358-1": "Miami",
        "1359-9": "Illinois Miami",
        "1360-7": "Indiana Miami",
        "1361-5": "Oklahoma Miami",
        "1363-1": "Miccosukee",
        "1365-6": "Micmac",
        "1366-4": "Aroostook",
        "1368-0": "Mission Indians",
        "1370-6": "Miwok",
        "1372-2": "Modoc",
        "1374-8": "Mohegan",
        "1376-3": "Mono",
        "1378-9": "Nanticoke",
        "1380-5": "Narragansett",
        "1382-1": "Navajo",
        "1383-9": "Alamo Navajo",
        "1384-7": "Canoncito Navajo",
        "1385-4": "Ramah Navajo",
        "1387-0": "Nez Perce",
        "1389-6": "Nomalaki",
        "1391-2": "Northwest Tribes",
        "1392-0": "Alsea",
        "1393-8": "Celilo",
        "1394-6": "Columbia",
        "1395-3": "Kalapuya",
        "1396-1": "Molala",
        "1397-9": "Talakamish",
        "1398-7": "Tenino",
        "1399-5": "Tillamook",
        "1400-1": "Wenatchee",
        "1401-9": "Yahooskin",
        "1403-5": "Omaha",
        "1405-0": "Oregon Athabaskan",
        "1407-6": "Osage",
        "1409-2": "Otoe-Missouria",
        "1411-8": "Ottawa",
        "1412-6": "Burt Lake Ottawa",
        "1413-4": "Michigan Ottawa",
        "1414-2": "Oklahoma Ottawa",
        "1416-7": "Paiute",
        "1417-5": "Bishop",
        "1418-3": "Bridgeport",
        "1419-1": "Burns Paiute",
        "1420-9": "Cedarville",
        "1421-7": "Fort Bidwell",
        "1422-5": "Fort Independence",
        "1423-3": "Kaibab",
        "1424-1": "Las Vegas",
        "1425-8": "Lone Pine",
        "1426-6": "Lovelock",
        "1427-4": "Malheur Paiute",
        "1428-2": "Moapa",
        "1429-0": "Northern Paiute",
        "1430-8": "Owens Valley",
        "1431-6": "Pyramid Lake",
        "1432-4": "San Juan Southern Paiute",
        "1433-2": "Southern Paiute",
        "1434-0": "Summit Lake",
        "1435-7": "Utu Utu Gwaitu Paiute",
        "1436-5": "Walker River",
        "1437-3": "Yerington Paiute",
        "1439-9": "Pamunkey",
        "1441-5": "Passamaquoddy",
        "1442-3": "Indian Township",
        "1443-1": "Pleasant Point Passamaquoddy",
        "1445-6": "Pawnee",
        "1446-4": "Oklahoma Pawnee",
        "1448-0": "Penobscot",
        "1450-6": "Peoria",
        "1451-4": "Oklahoma Peoria",
        "1453-0": "Pequot",
        "1454-8": "Marshantucket Pequot",
        "1456-3": "Pima",
        "1457-1": "Gila River Pima-Maricopa",
        "1458-9": "Salt River Pima-Maricopa",
        "1460-5": "Piscataway",
        "1462-1": "Pit River",
        "1464-7": "Pomo",
        "1465-4": "Central Pomo",
        "1466-2": "Dry Creek",
        "1467-0": "Eastern Pomo",
        "1468-8": "Kashia",
        "1469-6": "Northern Pomo",
        "1470-4": "Scotts Valley",
        "1471-2": "Stonyford",
        "1472-0": "Sulphur Bank",
        "1474-6": "Ponca",
        "1475-3": "Nebraska Ponca",
        "1476-1": "Oklahoma Ponca",
        "1478-7": "Potawatomi",
        "1479-5": "Citizen Band Potawatomi",
        "1480-3": "Forest County",
        "1481-1": "Hannahville",
        "1482-9": "Huron Potawatomi",
        "1483-7": "Pokagon Potawatomi",
        "1484-5": "Prairie Band",
        "1485-2": "Wisconsin Potawatomi",
        "1487-8": "Powhatan",
        "1489-4": "Pueblo",
        "1490-2": "Acoma",
        "1491-0": "Arizona Tewa",
        "1492-8": "Cochiti",
        "1493-6": "Hopi",
        "1494-4": "Isleta",
        "1495-1": "Jemez",
        "1496-9": "Keres",
        "1497-7": "Laguna",
        "1498-5": "Nambe",
        "1499-3": "Picuris",
        "1500-8": "Piro",
        "1501-6": "Pojoaque",
        "1502-4": "San Felipe",
        "1503-2": "San Ildefonso",
        "1504-0": "San Juan Pueblo",
        "1505-7": "San Juan De",
        "1506-5": "San Juan",
        "1507-3": "Sandia",
        "1508-1": "Santa Ana",
        "1509-9": "Santa Clara",
        "1510-7": "Santo Domingo",
        "1511-5": "Taos",
        "1512-3": "Tesuque",
        "1513-1": "Tewa",
        "1514-9": "Tigua",
        "1515-6": "Zia",
        "1516-4": "Zuni",
        "1518-0": "Puget Sound Salish",
        "1519-8": "Duwamish",
        "1520-6": "Kikiallus",
        "1521-4": "Lower Skagit",
        "1522-2": "Muckleshoot",
        "1523-0": "Nisqually",
        "1524-8": "Nooksack",
        "1525-5": "Port Madison",
        "1526-3": "Puyallup",
        "1527-1": "Samish",
        "1528-9": "Sauk-Suiattle",
        "1529-7": "Skokomish",
        "1530-5": "Skykomish",
        "1531-3": "Snohomish",
        "1532-1": "Snoqualmie",
        "1533-9": "Squaxin Island",
        "1534-7": "Steilacoom",
        "1535-4": "Stillaguamish",
        "1536-2": "Suquamish",
        "1537-0": "Swinomish",
        "1538-8": "Tulalip",
        "1539-6": "Upper Skagit",
        "1541-2": "Quapaw",
        "1543-8": "Quinault",
        "1545-3": "Rappahannock",
        "1547-9": "Reno-Sparks",
        "1549-5": "Round Valley",
        "1551-1": "Sac and Fox",
        "1552-9": "Iowa Sac and Fox",
        "1553-7": "Missouri Sac and Fox",
        "1554-5": "Oklahoma Sac and Fox",
        "1556-0": "Salinan",
        "1558-6": "Salish",
        "1560-2": "Salish and Kootenai",
        "1562-8": "Schaghticoke",
        "1564-4": "Scott Valley",
        "1566-9": "Seminole",
        "1567-7": "Big Cypress",
        "1568-5": "Brighton",
        "1569-3": "Florida Seminole",
        "1570-1": "Hollywood Seminole",
        "1571-9": "Oklahoma Seminole",
        "1573-5": "Serrano",
        "1574-3": "San Manual",
        "1576-8": "Shasta",
        "1578-4": "Shawnee",
        "1579-2": "Absentee Shawnee",
        "1580-0": "Eastern Shawnee",
        "1582-6": "Shinnecock",
        "1584-2": "Shoalwater Bay",
        "1586-7": "Shoshone",
        "1587-5": "Battle Mountain",
        "1588-3": "Duckwater",
        "1589-1": "Elko",
        "1590-9": "Ely",
        "1591-7": "Goshute",
        "1592-5": "Panamint",
        "1593-3": "Ruby Valley",
        "1594-1": "Skull Valley",
        "1595-8": "South Fork Shoshone",
        "1596-6": "Te-Moak Western Shoshone",
        "1597-4": "Timbi-Sha Shoshone",
        "1598-2": "Washakie",
        "1599-0": "Wind River Shoshone",
        "1600-6": "Yomba",
        "1602-2": "Shoshone Paiute",
        "1603-0": "Duck Valley",
        "1604-8": "Fallon",
        "1605-5": "Fort McDermitt",
        "1607-1": "Siletz",
        "1609-7": "Sioux",
        "1610-5": "Blackfoot Sioux",
        "1611-3": "Brule Sioux",
        "1612-1": "Cheyenne River Sioux",
        "1613-9": "Crow Creek Sioux",
        "1614-7": "Dakota Sioux",
        "1615-4": "Flandreau Santee",
        "1616-2": "Fort Peck",
        "1617-0": "Lake Traverse Sioux",
        "1618-8": "Lower Brule Sioux",
        "1619-6": "Lower Sioux",
        "1620-4": "Mdewakanton Sioux",
        "1621-2": "Miniconjou",
        "1622-0": "Oglala Sioux",
        "1623-8": "Pine Ridge Sioux",
        "1624-6": "Pipestone Sioux",
        "1625-3": "Prairie Island Sioux",
        "1626-1": "Prior Lake Sioux",
        "1627-9": "Rosebud Sioux",
        "1628-7": "Sans Arc Sioux",
        "1629-5": "Santee Sioux",
        "1630-3": "Sisseton-Wahpeton",
        "1631-1": "Sisseton Sioux",
        "1632-9": "Spirit Lake Sioux",
        "1633-7": "Standing Rock Sioux",
        "1634-5": "Teton Sioux",
        "1635-2": "Two Kettle Sioux",
        "1636-0": "Upper Sioux",
        "1637-8": "Wahpekute Sioux",
        "1638-6": "Wahpeton Sioux",
        "1639-4": "Wazhaza Sioux",
        "1640-2": "Yankton Sioux",
        "1641-0": "Yanktonai Sioux",
        "1643-6": "Siuslaw",
        "1645-1": "Spokane",
        "1647-7": "Stewart",
        "1649-3": "Stockbridge",
        "1651-9": "Susanville",
        "1653-5": "Tohono O'Odham",
        "1654-3": "Ak-Chin",
        "1655-0": "Gila Bend",
        "1656-8": "San Xavier",
        "1657-6": "Sells",
        "1659-2": "Tolowa",
        "1661-8": "Tonkawa",
        "1663-4": "Tygh",
        "1665-9": "Umatilla",
        "1667-5": "Umpqua",
        "1668-3": "Cow Creek Umpqua",
        "1670-9": "Ute",
        "1671-7": "Allen Canyon",
        "1672-5": "Uintah Ute",
        "1673-3": "Ute Mountain Ute",
        "1675-8": "Wailaki",
        "1677-4": "Walla-Walla",
        "1679-0": "Wampanoag",
        "1680-8": "Gay Head Wampanoag",
        "1681-6": "Mashpee Wampanoag",
        "1683-2": "Warm Springs",
        "1685-7": "Wascopum",
        "1687-3": "Washoe",
        "1688-1": "Alpine",
        "1689-9": "Carson",
        "1690-7": "Dresslerville",
        "1692-3": "Wichita",
        "1694-9": "Wind River",
        "1696-4": "Winnebago",
        "1697-2": "Ho-chunk",
        "1698-0": "Nebraska Winnebago",
        "1700-4": "Winnemucca",
        "1702-0": "Wintun",
        "1704-6": "Wiyot",
        "1705-3": "Table Bluff",
        "1707-9": "Yakama",
        "1709-5": "Yakama Cowlitz",
        "1711-1": "Yaqui",
        "1712-9": "Barrio Libre",
        "1713-7": "Pascua Yaqui",
        "1715-2": "Yavapai Apache",
        "1717-8": "Yokuts",
        "1718-6": "Chukchansi",
        "1719-4": "Tachi",
        "1720-2": "Tule River",
        "1722-8": "Yuchi",
        "1724-4": "Yuman",
        "1725-1": "Cocopah",
        "1726-9": "Havasupai",
        "1727-7": "Hualapai",
        "1728-5": "Maricopa",
        "1729-3": "Mohave",
        "1730-1": "Quechan",
        "1731-9": "Yavapai",
        "1732-7": "Yurok",
        "1733-5": "Coast Yurok",
        "1735-0": "Alaska Native",
        "1737-6": "Alaska Indian",
        "1739-2": "Alaskan Athabascan",
        "1740-0": "Ahtna",
        "1811-9": "Southeast Alaska",
        "1813-5": "Tlingit-Haida",
        "1814-3": "Angoon",
        "1815-0": "Central Council of Tlingit and Haida Tribes",
        "1816-8": "Chilkat",
        "1817-6": "Chilkoot",
        "1818-4": "Craig",
        "1819-2": "Douglas",
        "1820-0": "Haida",
        "1821-8": "Hoonah",
        "1822-6": "Hydaburg",
        "1823-4": "Kake",
        "1824-2": "Kasaan",
        "1825-9": "Kenaitze",
        "1826-7": "Ketchikan",
        "1827-5": "Klawock",
        "1828-3": "Pelican",
        "1829-1": "Petersburg",
        "1830-9": "Saxman",
        "1831-7": "Sitka",
        "1832-5": "Tenakee Springs",
        "1833-3": "Tlingit",
        "1834-1": "Wrangell",
        "1835-8": "Yakutat",
        "1837-4": "Tsimshian",
        "1838-2": "Metlakatla",
        "1840-8": "Eskimo",
        "1842-4": "Greenland Eskimo",
        "1844-0": "Inupiat Eskimo",
        "1845-7": "Ambler",
        "1846-5": "Anaktuvuk",
        "1847-3": "Anaktuvuk Pass",
        "1848-1": "Arctic Slope Inupiat",
        "1849-9": "Arctic Slope Corporation",
        "1850-7": "Atqasuk",
        "1851-5": "Barrow",
        "1852-3": "Bering Straits Inupiat",
        "1853-1": "Brevig Mission",
        "1854-9": "Buckland",
        "1855-6": "Chinik",
        "1856-4": "Council",
        "1857-2": "Deering",
        "1858-0": "Elim",
        "1859-8": "Golovin",
        "1860-6": "Inalik Diomede",
        "1861-4": "Inupiaq",
        "1862-2": "Kaktovik",
        "1863-0": "Kawerak",
        "1864-8": "Kiana",
        "1865-5": "Kivalina",
        "1866-3": "Kobuk",
        "1867-1": "Kotzebue",
        "1868-9": "Koyuk",
        "1869-7": "Kwiguk",
        "1870-5": "Mauneluk Inupiat",
        "1871-3": "Nana Inupiat",
        "1872-1": "Noatak",
        "1873-9": "Nome",
        "1874-7": "Noorvik",
        "1875-4": "Nuiqsut",
        "1876-2": "Point Hope",
        "1877-0": "Point Lay",
        "1878-8": "Selawik",
        "1879-6": "Shaktoolik",
        "1880-4": "Shishmaref",
        "1881-2": "Shungnak",
        "1882-0": "Solomon",
        "1883-8": "Teller",
        "1884-6": "Unalakleet",
        "1885-3": "Wainwright",
        "1886-1": "Wales",
        "1887-9": "White Mountain",
        "1888-7": "White Mountain Inupiat",
        "1889-5": "Mary's Igloo",
        "1891-1": "Siberian Eskimo",
        "1892-9": "Gambell",
        "1893-7": "Savoonga",
        "1894-5": "Siberian Yupik",
        "1896-0": "Yupik Eskimo",
        "1897-8": "Akiachak",
        "1898-6": "Akiak",
        "1899-4": "Alakanuk",
        "1900-0": "Aleknagik",
        "1901-8": "Andreafsky",
        "1902-6": "Aniak",
        "1903-4": "Atmautluak",
        "1904-2": "Bethel",
        "1905-9": "Bill Moore's Slough",
        "1906-7": "Bristol Bay Yupik",
        "1907-5": "Calista Yupik",
        "1908-3": "Chefornak",
        "1909-1": "Chevak",
        "1910-9": "Chuathbaluk",
        "1911-7": "Clark's Point",
        "1912-5": "Crooked Creek",
        "1913-3": "Dillingham",
        "1914-1": "Eek",
        "1915-8": "Ekuk",
        "1916-6": "Ekwok",
        "1917-4": "Emmonak",
        "1918-2": "Goodnews Bay",
        "1919-0": "Hooper Bay",
        "1920-8": "Iqurmuit (Russian Mission)",
        "1921-6": "Kalskag",
        "1922-4": "Kasigluk",
        "1923-2": "Kipnuk",
        "1924-0": "Koliganek",
        "1925-7": "Kongiganak",
        "1926-5": "Kotlik",
        "1927-3": "Kwethluk",
        "1928-1": "Kwigillingok",
        "1929-9": "Levelock",
        "1930-7": "Lower Kalskag",
        "1931-5": "Manokotak",
        "1932-3": "Marshall",
        "1933-1": "Mekoryuk",
        "1934-9": "Mountain Village",
        "1935-6": "Naknek",
        "1936-4": "Napaumute",
        "1937-2": "Napakiak",
        "1938-0": "Napaskiak",
        "1939-8": "Newhalen",
        "1940-6": "New Stuyahok",
        "1941-4": "Newtok",
        "1942-2": "Nightmute",
        "1943-0": "Nunapitchukv",
        "1944-8": "Oscarville",
        "1945-5": "Pilot Station",
        "1946-3": "Pitkas Point",
        "1947-1": "Platinum",
        "1948-9": "Portage Creek",
        "1949-7": "Quinhagak",
        "1950-5": "Red Devil",
        "1951-3": "St. Michael",
        "1952-1": "Scammon Bay",
        "1953-9": "Sheldon's Point",
        "1954-7": "Sleetmute",
        "1955-4": "Stebbins",
        "1956-2": "Togiak",
        "1957-0": "Toksook",
        "1958-8": "Tulukskak",
        "1959-6": "Tuntutuliak",
        "1960-4": "Tununak",
        "1961-2": "Twin Hills",
        "1962-0": "Georgetown",
        "1963-8": "St. Mary's",
        "1964-6": "Umkumiate",
        "1966-1": "Aleut",
        "1968-7": "Alutiiq Aleut",
        "1969-5": "Tatitlek",
        "1970-3": "Ugashik",
        "1972-9": "Bristol Bay Aleut",
        "1973-7": "Chignik",
        "1974-5": "Chignik Lake",
        "1975-2": "Egegik",
        "1976-0": "Igiugig",
        "1977-8": "Ivanof Bay",
        "1978-6": "King Salmon",
        "1979-4": "Kokhanok",
        "1980-2": "Perryville",
        "1981-0": "Pilot Point",
        "1982-8": "Port Heiden",
        "1984-4": "Chugach Aleut",
        "1985-1": "Chenega",
        "1986-9": "Chugach Corporation",
        "1987-7": "English Bay",
        "1988-5": "Port Graham",
        "1990-1": "Eyak",
        "1992-7": "Koniag Aleut",
        "1993-5": "Akhiok",
        "1994-3": "Agdaagux",
        "1995-0": "Karluk",
        "1996-8": "Kodiak",
        "1997-6": "Larsen Bay",
        "1998-4": "Old Harbor",
        "1999-2": "Ouzinkie",
        "2000-8": "Port Lions",
        "2002-4": "Sugpiaq",
        "2004-0": "Suqpigaq",
        "2006-5": "Unangan Aleut",
        "2007-3": "Akutan",
        "2008-1": "Aleut Corporation",
        "2009-9": "Aleutian",
        "2010-7": "Aleutian Islander",
        "2011-5": "Atka",
        "2012-3": "Belkofski",
        "2013-1": "Chignik Lagoon",
        "2014-9": "King Cove",
        "2015-6": "False Pass",
        "2016-4": "Nelson Lagoon",
        "2017-2": "Nikolski",
        "2018-0": "Pauloff Harbor",
        "2019-8": "Qagan Toyagungin",
        "2020-6": "Qawalangin",
        "2021-4": "St. George",
        "2022-2": "St. Paul",
        "2023-0": "Sand Point",
        "2024-8": "South Naknek",
        "2025-5": "Unalaska",
        "2026-3": "Unga",
        "2028-9": "Asian",
        "2029-7": "Asian Indian",
        "2030-5": "Bangladeshi",
        "2031-3": "Bhutanese",
        "2032-1": "Burmese",
        "2033-9": "Cambodian",
        "2034-7": "Chinese",
        "2035-4": "Taiwanese",
        "2036-2": "Filipino",
        "2037-0": "Hmong",
        "2038-8": "Indonesian",
        "2039-6": "Japanese",
        "2040-4": "Korean",
        "2041-2": "Laotian",
        "2042-0": "Malaysian",
        "2043-8": "Okinawan",
        "2044-6": "Pakistani",
        "2045-3": "Sri Lankan",
        "2046-1": "Thai",
        "2047-9": "Vietnamese",
        "2048-7": "Iwo Jiman",
        "2049-5": "Maldivian",
        "2050-3": "Nepalese",
        "2051-1": "Singaporean",
        "2052-9": "Madagascar",
        "2054-5": "Black or African American",
        "2056-0": "Black",
        "2058-6": "African American",
        "2060-2": "African",
        "2061-0": "Botswanan",
        "2062-8": "Ethiopian",
        "2063-6": "Liberian",
        "2064-4": "Namibian",
        "2065-1": "Nigerian",
        "2066-9": "Zairean",
        "2067-7": "Bahamian",
        "2068-5": "Barbadian",
        "2069-3": "Dominican",
        "2070-1": "Dominica Islander",
        "2071-9": "Haitian",
        "2072-7": "Jamaican",
        "2073-5": "Tobagoan",
        "2074-3": "Trinidadian",
        "2075-0": "West Indian",
        "2076-8": "Native Hawaiian or Other Pacific Islander",
        "2078-4": "Polynesian",
        "2079-2": "Native Hawaiian",
        "2080-0": "Samoan",
        "2081-8": "Tahitian",
        "2082-6": "Tongan",
        "2083-4": "Tokelauan",
        "2085-9": "Micronesian",
        "2086-7": "Guamanian or Chamorro",
        "2087-5": "Guamanian",
        "2088-3": "Chamorro",
        "2089-1": "Mariana Islander",
        "2090-9": "Marshallese",
        "2091-7": "Palauan",
        "2092-5": "Carolinian",
        "2093-3": "Kosraean",
        "2094-1": "Pohnpeian",
        "2095-8": "Saipanese",
        "2096-6": "Kiribati",
        "2097-4": "Chuukese",
        "2098-2": "Yapese",
        "2100-6": "Melanesian",
        "2101-4": "Fijian",
        "2102-2": "Papua New Guinean",
        "2103-0": "Solomon Islander",
        "2104-8": "New Hebrides",
        "2500-7": "Other Pacific Islander",
        "2106-3": "White",
        "2108-9": "European",
        "2109-7": "Armenian",
        "2110-5": "English",
        "2111-3": "French",
        "2112-1": "German",
        "2113-9": "Irish",
        "2114-7": "Italian",
        "2115-4": "Polish",
        "2116-2": "Scottish",
        "2118-8": "Middle Eastern or North African",
        "2119-6": "Assyrian",
        "2120-4": "Egyptian",
        "2121-2": "Iranian",
        "2122-0": "Iraqi",
        "2123-8": "Lebanese",
        "2124-6": "Palestinian",
        "2125-3": "Syrian",
        "2126-1": "Afghanistani",
        "2127-9": "Israeili",
        "2129-5": "Arab",
        "2131-1": "Other Race"

    }

    hl7_ethnicity_dict = {
        "2135-2": "Hispanic or Latino",
        "2137-8": "Spaniard",
        "2138-6": "Andalusian",
        "2139-4": "Asturian",
        "2140-2": "Castillian",
        "2141-0": "Catalonian",
        "2142-8": "Belearic Islander",
        "2143-6": "Gallego",
        "2144-4": "Valencian",
        "2145-1": "Canarian",
        "2146-9": "Spanish Basque",
        "2148-5": "Mexican",
        "2149-3": "Mexican American",
        "2150-1": "Mexicano",
        "2151-9": "Chicano",
        "2152-7": "La Raza",
        "2153-5": "Mexican American Indian",
        "2155-0": "Central American",
        "2156-8": "Costa Rican",
        "2157-6": "Guatemalan",
        "2158-4": "Honduran",
        "2159-2": "Nicaraguan",
        "2160-0": "Panamanian",
        "2161-8": "Salvadoran",
        "2162-6": "Central American Indian",
        "2163-4": "Canal Zone",
        "2165-9": "South American",
        "2166-7": "Argentinean",
        "2167-5": "Bolivian",
        "2168-3": "Chilean",
        "2169-1": "Colombian",
        "2170-9": "Ecuadorian",
        "2171-7": "Paraguayan",
        "2172-5": "Peruvian",
        "2173-3": "Uruguayan",
        "2174-1": "Venezuelan",
        "2175-8": "South American Indian",
        "2176-6": "Criollo",
        "2178-2": "Latin American",
        "2180-8": "Puerto Rican",
        "2182-4": "Cuban",
        "2184-0": "Dominican",
        "2186-5": "Not Hispanic or Latino",
    }

    def gender_correct(input_dict):

        if "gender_code" in input_dict:
            gender_code = input_dict["gender_code"]

            if gender_code == "248152002":
                return {"gender_code": "F"}
            elif gender_code == "248153007":
                return {"gender_code": "M"}
            else:
                return input_dict

        else:
            return {}

    person_id_duplicate_mapper = DuplicateExcludeMapper("empiPersonId")
    population_patient_rules = [("empiPersonId", "s_person_id"),
                                ("gender_code", PassThroughFunctionMapper(gender_correct), {"gender_code": "s_gender"}),
                                ("gender_code",  "m_gender"),
                                ("birthdate", "s_birth_datetime"),
                                ("dateofdeath", "s_death_datetime"),
                                ("race_code", "s_race"),
                                ("race_code",  CodeMapperDictClass(hl7_race_dict, key_to_map_to="m_race"), {"m_race": "m_race"}),
                                ("ethnicity_code", "s_ethnicity"),
                                ("ethnicity_code", CodeMapperDictClass(hl7_ethnicity_dict, key_to_map_to="m_ethnicity"), {"m_ethnicity": "m_ethnicity"}),
                                (("street_1", "street_2", "city", "state", "zip_code"),
                                key_location_mapper, {"mapped_value": "k_location"}),
                                ("empiPersonId", person_id_duplicate_mapper, {"i_exclude": "i_exclude"})
                                ]

    output_person_csv = os.path.join(output_csv_directory, "source_person.csv")

    source_person_runner_obj = generate_mapper_obj(input_patient_file_name, PopulationDemographics(), output_person_csv,
                                                   SourcePersonObject(), population_patient_rules,
                                                   output_class_obj, in_out_map_obj)

    source_person_runner_obj.run()  # Run the mapper

    # Care site
    care_site_csv = os.path.join(input_csv_directory, "care_site.csv")
    md5_func = lambda x: hashlib.md5(x.encode("utf8")).hexdigest()

    population_care_site = os.path.join(input_csv_directory, "population_care_site.csv")

    key_care_site_mapper = build_name_lookup_csv(population_care_site, care_site_csv,
                                                 ["facility_name", "building_name",
                                                  "nurseunit_name", "hospitalservice_code_text"],
                                                 ["facility_name", "building_name",
                                                  "nurseunit_name", "hospitalservice_code_text"], hashing_func=md5_func)

    care_site_name_mapper = FunctionMapper(
        build_key_func_dict(["facility_name", "building_name",
                                                  "nurseunit_name", "hospitalservice_code_text"], separator=" -- "))

    care_site_rules = [("key_name", "k_care_site"),
                       (("facility_name", "building_name", "nurseunit_name", "hospitalservice_code_text"),
                        care_site_name_mapper,
                        {"mapped_value": "s_care_site_name"})]

    source_care_site_csv = os.path.join(output_csv_directory, "source_care_site.csv")

    care_site_runner_obj = generate_mapper_obj(care_site_csv, PopulationCareSite(), source_care_site_csv,
                                               SourceCareSiteObject(), care_site_rules,
                                               output_class_obj, in_out_map_obj)

    care_site_runner_obj.run()

    # Encounters
    encounter_file_name = os.path.join(input_csv_directory, file_name_dict["encounter"])
    encounter_id_duplicate_mapper = DuplicateExcludeMapper("encounterid")
    encounter_rules = [
        ("encounterid", "s_encounter_id"),
        ("empiPersonId", "s_person_id"),
        ("servicedate", "s_visit_start_datetime"),
        ("dischargedate", "s_visit_end_datetime"),
        ("type_code_text", "s_visit_type"),
        ("classification_code_text", "m_visit_type"),
        ("dischargedisposition_code_text", "s_discharge_to"),
        ("dischargedisposition_code", "m_discharge_to"),
        ("admissionsource_code_text", "s_admitting_source"),
        ("admissionsource_code", "m_admitting_source"),
        (("facility_name", "building_name", "nurseunit_name", "hospitalservice_code_text"), key_care_site_mapper, {"mapped_value": "k_care_site"}),
        ("encounterid", encounter_id_duplicate_mapper, {"i_exclude": "i_exclude"})
    ]
    source_encounter_csv = os.path.join(output_csv_directory, "source_encounter.csv")

    # Generate care site combination of tenant and hospitalservice_code_text

    encounter_runner_obj = generate_mapper_obj(encounter_file_name, PopulationEncounter(), source_encounter_csv,
                                               SourceEncounterObject(), encounter_rules,
                                               output_class_obj, in_out_map_obj)

    encounter_runner_obj.run()

    observation_csv_file = os.path.join(input_csv_directory, "population_observation.csv")

    generate_observation_period(source_encounter_csv, observation_csv_file,
                                "s_person_id", "s_visit_start_datetime", "s_visit_end_datetime")

    observation_period_rules = [("s_person_id", "s_person_id"),
                                ("s_visit_start_datetime", "s_start_observation_datetime"),
                                ("s_visit_end_datetime", "s_end_observation_datetime")]

    source_observation_period_csv = os.path.join(output_csv_directory, "source_observation_period.csv")

    observation_runner_obj = generate_mapper_obj(observation_csv_file, PopulationObservationPeriod(),
                                                 source_observation_period_csv,
                                                 SourceObservationPeriodObject(), observation_period_rules,
                                                 output_class_obj, in_out_map_obj)
    observation_runner_obj.run()


    # Encounter plan or insurance coverage

    source_encounter_coverage_csv = os.path.join(output_csv_directory, "source_encounter_coverage.csv")

    encounter_coverage_rules = [("empiPersonId", "s_person_id"),
                                ("encounterid", "s_encounter_id"),
                                ("servicedate", "s_start_payer_date"),
                                ("dischargedate", "s_end_payer_date"),
                                ("financialclass_code_text", "s_payer_name"),
                                ("financialclass_code_text", "m_payer_name"),
                                ("financialclass_code_text", "s_plan_name"),
                                ("financialclass_code_text", "m_plan_name")]

    encounter_benefit_runner_obj = generate_mapper_obj(encounter_file_name,
                                                       PopulationEncounter(),
                                                       source_encounter_coverage_csv, SourceEncounterCoverageObject(),
                                                       encounter_coverage_rules, output_class_obj, in_out_map_obj)

    encounter_benefit_runner_obj.run()


    population_location_csv = os.path.join(input_csv_directory, "population_encounter_location.csv")
    source_encounter_detail_csv = os.path.join(output_csv_directory, "source_encounter_detail.csv")

    def check_if_not_empty(input_dict):
        if "begindate" in input_dict:
            if not len(input_dict["begindate"]):
                return {"i_exclude": 1}
            else:
                return {"i_exclude": ""}
        else:
            return {"i_exclude": 1}


    source_encounter_detail_rules = [
        ("encounterid", "s_encounter_id"),
        ("encounterid", "s_encounter_detail_id"),
        ("empiPersonId", "s_person_id"),
        ("begindate", "s_start_datetime"),
        ("enddate", "s_end_datetime"),
        #("classification_display", "s_visit_detail_type"),
        #("classification_display", "m_visit_detail_type"),
        (("facility_name", "building_name", "nurseunit_name", "hospitalservice_code_text"), key_care_site_mapper, {"mapped_value": "k_care_site"}),
        ("begindate", PassThroughFunctionMapper(check_if_not_empty), {"i_exclude": "i_exclude"})
    ]

    encounter_detail_runner_obj = generate_mapper_obj(population_location_csv, PopulationEncounterLocation(),
                                                      source_encounter_detail_csv,
                                                      SourceEncounterDetailObject(),
                                                      source_encounter_detail_rules, output_class_obj, in_out_map_obj)

    encounter_detail_runner_obj.run()

    def m_rank_func(input_dict):
        if input_dict["billingrank"] == "PRIMARY":
            return {"m_rank": "Primary"}
        elif input_dict["billingrank"] == "SECONDARY":
            return {"m_rank": "Secondary"}
        else:
            return {}

    condition_rules = [("empiPersonId", "s_person_id"),
                       ("encounterid", "s_encounter_id"),
                       ("effectiveDate", "s_start_condition_datetime"),
                       ("condition_code", "s_condition_code"),
                       ("condition_code_oid", "m_condition_code_oid"),
                       ("billingrank", PassThroughFunctionMapper(m_rank_func), {"m_rank": "m_rank"}),
                       ("source", "s_condition_type"),
                       ("presentonadmission_code", "s_present_on_admission_indicator")]

    condition_csv = os.path.join(input_csv_directory, file_name_dict["condition"])
    source_condition_csv = os.path.join(output_csv_directory, "source_condition.csv")
    condition_mapper_obj = generate_mapper_obj(condition_csv, PopulationCondition(), source_condition_csv,
                                               SourceConditionObject(),
                                               condition_rules, output_class_obj, in_out_map_obj)

    condition_mapper_obj.run()

    procedure_csv = os.path.join(input_csv_directory, file_name_dict["procedure"])
    source_procedure_csv = os.path.join(output_csv_directory, "source_procedure.csv")

    procedure_rules = [("empiPersonId", "s_person_id"),
                       ("encounterid", "s_encounter_id"),
                       ("servicestartdate", "s_start_procedure_datetime"),
                       ("serviceenddate", "s_end_procedure_datetime"),
                       ("procedure_code", "s_procedure_code"),
                       ("procedure_code_oid", "s_procedure_code_type"),
                       ("procedure_code_oid", "m_procedure_code_oid")
                       ]

    procedure_mapper_obj = generate_mapper_obj(procedure_csv, PopulationProcedure(), source_procedure_csv,
                                               SourceProcedureObject(),
                                               procedure_rules, output_class_obj, in_out_map_obj)

    procedure_mapper_obj.run()

    def active_medications(input_dict):
        if "status_code_text" in input_dict:
            if input_dict["status_code_text"] not in ('Complete', 'Discontinued', 'Active', 'Suspended'):
                return {"i_exclude": 1}
            else:
                if "detailLine" in input_dict:
                    if " PRN" in input_dict["detailLine"]:
                        return {"i_exclude": 1}
                    else:
                        return {}
                else:
                    return {}
        else:
            return {}

    ["medicationid", "encounterid", "empiPersonId", "intendeddispenser", "startdate", "stopdate", "doseunit_code",
     "doseunit_code_oid", "doseunit_code_text", "category_id", "category_code_oid", "category_code_text",
     "frequency_id", "frequency_code_oid", "frequency_code_text", "status_code", "status_code_oid",
     "status_code_text", "route_code", "route_code_oid", "route_code_text", "drug_code", "drug_code_oid",
     "drug_code_text", "dosequantity", "source"]

    medication_rules = [("empiPersonId", "s_person_id"),
                        ("encounterid", "s_encounter_id"),
                        ("drug_code", "s_drug_code"),
                        ("drug_code_oid", "m_drug_code_oid"),
                        ("drug_code_text", "s_drug_text"),
                        ("startdate", "s_start_medication_datetime"),
                        ("stopdate", "s_end_medication_datetime"),
                        ("route_code_text", "s_route"),
                        ("route_code", "m_route"),
                        ("dosequantity", "s_quantity"),
                        ("doseunit_code_text", "s_dose_unit"),
                        ("doseunit_code", "m_dose_unit"),
                        ("intendeddispenser", "s_drug_type"),
                        ("intendeddispenser", "m_drug_type"),
                        ("status_code", "s_status"),
                        (("status_code_text", "detailLine"), PassThroughFunctionMapper(active_medications),
                        {"i_exclude": "i_exclude"})
                        ]

    medication_csv = os.path.join(input_csv_directory, file_name_dict["medication"])
    source_medication_csv = os.path.join(output_csv_directory, "source_medication.csv")

    medication_mapper_obj = generate_mapper_obj(medication_csv, PopulationMedication(), source_medication_csv,
                                                SourceMedicationObject(), medication_rules,
                                                output_class_obj, in_out_map_obj)

    medication_mapper_obj.run()

    result_csv = os.path.join(input_csv_directory, file_name_dict["result"])
    source_result_csv = os.path.join(output_csv_directory, "source_result.csv")

    ["resultid", "encounterid", "empiPersonId", "result_code", "result_code_oid", "result_code_text",
     "result_type", "servicedate", "value_text", "value_numeric", "value_numeric_modifier", "unit_code",
     "unit_code_oid", "unit_code_text", "value_codified_code", "value_codified_code_oid",
     "value_codified_code_text", "date", "interpretation_code", "interpretation_code_oid",
     "interpretation_code_text", "specimen_type_code", "specimen_type_code_oid", "specimen_type_code_text",
     "bodysite_code", "bodysite_code_oid", "bodysite_code_text", "specimen_collection_date",
     "specimen_received_date", "measurementmethod_code", "measurementmethod_code_oid",
     "measurementmethod_code_text", "recordertype", "issueddate", "year"]

    def remove_equals(input):
        return "".join(input["value_text"].split("="))

    result_rules = [("empiPersonId", "s_person_id"),
                    ("encounterid", "s_encounter_id"),
                    ("servicedate", "s_obtained_datetime"),
                    ("result_code_text", "s_name"),
                    ("result_code", "s_code"),
                    ("result_code_oid", "m_type_code_oid"),
                    ("value_text", "s_result_text"),
                    (("value_codified_code_text", "interpretation_code_text"),
                     FilterHasKeyValueMapper(["value_codified_code_text", "interpretation_code_text"]),
                     {"value_codified_code_text": "m_result_text", "interpretation_code_text": "m_result_text"}),
                    (("value_numeric", "value_text"), CascadeMapper(FilterHasKeyValueMapper(["value_numeric"]),
                                                                    ChainMapper(FunctionMapper(remove_equals, "value_text"), IntFloatMapper(), KeyTranslator({"value_text": "value_numeric"}))),
                     {"value_numeric": "s_result_numeric"}),
                    ("date", "s_result_datetime"),
                    ("value_codified_code", "s_result_code"),
                    ("value_codified_code_oid", "m_result_code_oid"),
                    ("unit_code", "s_result_unit"),
                    ("unit_code", "s_result_unit_code"),
                    ("unit_code_oid", "m_result_unit_code_oid"),
                    #("norm_unit_of_measure_code", "s_result_unit_code")
                    ("lower_limit", "s_result_numeric_lower"),
                    ("upper_limit", "s_result_numeric_upper")
                    ]



    result_mapper_obj = generate_mapper_obj(result_csv, PopulationResult(), source_result_csv, SourceResultObject(),
                                            result_rules, output_class_obj, in_out_map_obj)

    result_mapper_obj.run()


if __name__ == "__main__":
    arg_parse_obj = argparse.ArgumentParser(description="Mapping Realworld CSV files to Prepared source format for OHDSI mapping")
    arg_parse_obj.add_argument("-c", "--config-file-name", dest="config_file_name", help="JSON config file",
                               default="sbm_config.json")

    arg_obj = arg_parse_obj.parse_args()

    print("Reading config file '%s'" % arg_obj.config_file_name)
    with open(arg_obj.config_file_name, "r") as f:
        config_dict = json.load(f)

    file_name_dict = {
        "demographic": "population_demographics.consolidated.csv",
        "encounter": "population_encounter.csv",
        "condition": "population_condition.csv",
        "measurement": "population_measurement.csv",
        "medication": "population_medication.csv",
        "procedure": "population_procedure.csv",
        "result": "population_results.csv"
    }

    main(config_dict["csv_input_directory"], config_dict["csv_input_directory"], file_name_dict)