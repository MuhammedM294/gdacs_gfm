# --- CONSTANTS / DEFINITIONS ---

from collections import OrderedDict
DIMENSIONS = ["algorithm", "var_name", "time", "pol", "product_res", "tile_name"]

# Fields definition for the flood extent layer of the GFM and NRT Archive products
FIELDS_DEF = OrderedDict(
    [
        ("algorithm", {"len": 8}),
        ("var_name", {"len": 5}),
        ("time", {"len": 15}),
        ("pol", {"len": 2}),
        ("product_res", {"len": 6}),
        ("tile_name", {"len": 10}),
    ]
)

U_Fields_DEF = OrderedDict(
    [
        ("algorithm", {"len": 8}),
        ("var_name", {"len": 11}),
        ("time", {"len": 15}),
        ("pol", {"len": 2}),
        ("product_res", {"len": 6}),
        ("tile_name", {"len": 10}),
    ]
)

# Fields definition for the TU Wien Algorithm Product
TUW_FILED_DEF = OrderedDict(
    [
        ("algorithm", {"len": 3}),
        ("var_name", {"len": 5}),
        ("time", {"len": 15}),
        ("pol", {"len": 2}),
        ("product_res", {"len": 6}),
        ("tile_name", {"len": 10}),
    ]
)
HM_FILED_DEF = OrderedDict(
    [
        ("algorithm", {"len": 8}),
        ("time", {"len": 15}),
        ("pol", {"len": 2}),
        ("param1", {"len": 4}),
        ("tile_name", {"len": 10}),
        ("param2", {"len": 6}),
        ("param3", {"len": 6}),
        ("param4", {"len": 2}),
    ]
)

DLR_FILED_DEF = OrderedDict(
    [
        ("algorithm", {"len": 3}),
        ("var_name", {"len": 5}),
        ("time", {"len": 15}),
        ("pol", {"len": 2}),
        ("product_res", {"len": 6}),
        ("tile_name", {"len": 10}),
    ]
)

LIST_FILED_DEF = OrderedDict(
    [
        ("algorithm", {"len": 4}),
        ("var_name", {"len": 5}),
        ("time", {"len": 15}),
        ("pol", {"len": 2}),
        ("product_res", {"len": 6}),
        ("tile_name", {"len": 10}),
    ]
)

DEF_DICT = {
    "ensemble": FIELDS_DEF,
    "tuw": TUW_FILED_DEF,
    "hm": HM_FILED_DEF,
    "dlr": DLR_FILED_DEF,
    "list": LIST_FILED_DEF,
}