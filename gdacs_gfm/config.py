# --- CONSTANTS / DEFINITIONS ---

from collections import OrderedDict

DIMENSIONS = ["algorithm", "var_name", "time", "pol", "equi7_grid", "tile_name"]

# Fields definition for the flood extent layer of the GFM and NRT Archive products
FIELDS_DEF = OrderedDict(
    [
        ("algorithm", {"len": 8}),
        ("var_name", {"len": 5}),
        ("time", {"len": 15}),
        ("pol", {"len": 2}),
        ("equi7_grid", {"len": 6}),
        ("tile_name", {"len": 10}),
    ]
)

U_Fields_DEF = OrderedDict(
    [
        ("algorithm", {"len": 8}),
        ("var_name", {"len": 11}),
        ("time", {"len": 15}),
        ("pol", {"len": 2}),
        ("equi7_grid", {"len": 6}),
        ("tile_name", {"len": 10}),
    ]
)

# Fields definition for the TU Wien Algorithm Product
TUW_FIELDS_DEF = OrderedDict(
    [
        ("algorithm", {"len": 3}),
        ("var_name", {"len": 5}),
        ("time", {"len": 15}),
        ("pol", {"len": 2}),
        ("equi7_grid", {"len": 6}),
        ("tile_name", {"len": 10}),
    ]
)
HM_FIELDS_DEF = OrderedDict(
    [
        ("algorithm", {"len": 8}),
        ("time", {"len": 15}),
        ("pol", {"len": 3}),
        ("orbit", {"len": 4}),
        ("tile_name", {"len": 10}),
        ("equi7_grid", {"len": 6}),
        ("param3", {"len": 6}),
        ("param4", {"len": 2}),
    ]
)

DLR_FIELDS_DEF = OrderedDict(
    [
        ("algorithm", {"len": 3}),
        ("var_name", {"len": 5}),
        ("time", {"len": 15}),
        ("pol", {"len": 2}),
        ("equi7_grid", {"len": 6}),
        ("tile_name", {"len": 10}),
    ]
)

LIST_FIELDS_DEF = OrderedDict(
    [
        ("algorithm", {"len": 4}),
        ("var_name", {"len": 5}),
        ("time", {"len": 15}),
        ("pol", {"len": 2}),
        ("equi7_grid", {"len": 6}),
        ("tile_name", {"len": 10}),
    ]
)

FL_DEF_DICT = {
    "ensemble": FIELDS_DEF,
    "tuw": TUW_FIELDS_DEF,
    "dlr": DLR_FIELDS_DEF,
    "list": LIST_FIELDS_DEF,
}


UNCERTAINTY_FIELDS_DEF = OrderedDict(
    [
        ("algorithm", {"len": 8}),
        ("var_name", {"len": 11}),
        ("time", {"len": 15}),
        ("pol", {"len": 2}),
        ("equi7_grid", {"len": 6}),
        ("tile_name", {"len": 10}),
    ]
)

NRT_UNCERTAINTY_FIELDS_DEF = FIELDS_DEF = OrderedDict(
    [
        ("algorithm", {"len": 3}),
        ("var_name", {"len": 11}),
        ("time", {"len": 15}),
        ("pol", {"len": 2}),
        ("equi7_grid", {"len": 6}),
        ("tile_name", {"len": 10}),
    ]
)

ARCH_UNCERTAINTY_FIELDS_DEF = OrderedDict(
    [
        ("algorithm", {"len": 3}),
        ("var_name", {"len": 4}),
        ("time", {"len": 15}),
        ("pol", {"len": 2}),
        ("equi7_grid", {"len": 6}),
        ("tile_name", {"len": 10}),
    ]
)


UN_DEF_DICT = {
    "ensemble": UNCERTAINTY_FIELDS_DEF,
    "nrt": NRT_UNCERTAINTY_FIELDS_DEF,
    "archive": ARCH_UNCERTAINTY_FIELDS_DEF,
}


NRT_EXCLUSION_FIELDS_DEF = OrderedDict(
    [
        ("algorithm", {"len": 8}),
        ("var_name", {"len": 8}),
        ("time", {"len": 15}),
        ("pol", {"len": 2}),
        ("equi7_grid", {"len": 6}),
        ("tile_name", {"len": 10}),
    ]
)

ARCH_EXCLUSION_FIELDS_DEF = OrderedDict(
    [
        ("algorithm", {"len": 9}),   # EXCLUSION
        ("var_name", {"len": 5}),   # LAYER
        ("time", {"len": 15}),
        ("pol", {"len": 2}),
        ("equi7_grid", {"len": 6}),
        ("tile_name", {"len": 10}),
    ]
)

NRT_OBS_FIELDS_DEF = OrderedDict(
    [
        ("algorithm", {"len": 8}),
        ("var_name", {"len": 8}),   # OBSWATER
        ("time", {"len": 15}),
        ("pol", {"len": 2}),
        ("equi7_grid", {"len": 6}),
        ("tile_name", {"len": 10}),
    ]
)


ARCH_OBS_FIELDS_DEF = OrderedDict(
    [
        ("algorithm", {"len": 8}),   # OBSERVED
        ("var_name", {"len": 5}),   # WATER
        ("time", {"len": 15}),
        ("pol", {"len": 2}),
        ("equi7_grid", {"len": 6}),
        ("tile_name", {"len": 10}),
    ]
)

NRT_ADV_FIELDS_DEF = FIELDS_DEF = OrderedDict(
    [
        ("var_name", {"len": 7}),      # ADVFLAG
        ("time", {"len": 15}),
        ("empty", {"len": 0}),         # empty field between double underscores
        ("pol", {"len": 2}),           # VV
        ("orbit", {"len": 4}),        # A099
        ("tile_name", {"len": 10}),    # E048N021T3
        ("equi7_tag", {"len": 5}),     # EQUI7
        ("equi7_grid", {"len": 6}),    # AS020M
        ("version", {"len": 6}),       # V0M2R2
        ("sensor", {"len": 2}),        # S1
    ]
)


ARCH_ADV_FIELDS_DEF = OrderedDict(
    [
        ("var_name", {"len": 7}),     # ADVFLAG
        ("time", {"len": 15}),
        ("pol", {"len": 2}),
        ("equi7_grid", {"len": 6}),
        ("tile_name", {"len": 10}),
    ]
)