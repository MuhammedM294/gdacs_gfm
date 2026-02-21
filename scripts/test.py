import pandas as pd
import ast
from pathlib import Path
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature

# -------------------------
# Configuration
# -------------------------
CSV_PATH = Path(
    "/eodc/private/tuwgeo/users/mabdelaa/repos/GDACS_Flood_DB/data/latest_gdacs_flood_db.csv"
)

SIZE_MAP = {
    "red": 50,
    "orange": 25,
    "green": 10
}

COLOR_MAP = {
    "red": "#8b0000",
    "orange": "#ff8c00",
    "green": "#2ca25f"
}

# -------------------------
# Load and prepare data
# -------------------------
df = pd.read_csv(CSV_PATH)


print(df[df["alertscore"] == 3]['equi7_grid_code'])


# Normalize alert level naming
df["alertlevel"] = df["alertlevel"].str.lower()

# Convert geometry string to dict
df["geometry_dict"] = df["geometry"].apply(ast.literal_eval)

# Extract coordinates
df["lon"] = df["geometry_dict"].apply(lambda x: x["coordinates"][0])
df["lat"] = df["geometry_dict"].apply(lambda x: x["coordinates"][1])

# Count events per alert level
counts = df["alertlevel"].value_counts().to_dict()

red_count = counts.get("red", 0)
orange_count = counts.get("orange", 0)
green_count = counts.get("green", 0)

# -------------------------
# Create map
# -------------------------
plt.figure(figsize=(14, 7))
ax = plt.axes(projection=ccrs.Robinson())

ax.set_global()
ax.add_feature(cfeature.OCEAN, facecolor="#e6f2ff")
ax.add_feature(cfeature.LAND, facecolor="#f7f7f7")
ax.add_feature(cfeature.COASTLINE, linewidth=0.5)
ax.add_feature(cfeature.BORDERS, linewidth=0.3, alpha=0.5)

# Plot events
for level in ["green", "orange", "red"]:
    subset = df[df["alertlevel"] == level]

    ax.scatter(
        subset["lon"],
        subset["lat"],
        s=SIZE_MAP[level],
        color=COLOR_MAP[level],
        alpha=0.75,
        edgecolor="black",
        linewidth=0.4,
        transform=ccrs.PlateCarree(),
        label=level.capitalize()
    )

# Legend
plt.legend(title="Flood Alert Level", loc="lower left", frameon=True)

# -------------------------
# Add count annotation
# -------------------------
text = (
    f"Event Counts\n"
    f"Red: {red_count}\n"
    f"Orange: {orange_count}\n"
    f"Green: {green_count}"
)

plt.text(
    0.02, 0.95,
    text,
    transform=ax.transAxes,
    fontsize=11,
    verticalalignment="top",
    bbox=dict(
        boxstyle="round,pad=0.5",
        facecolor="white",
        alpha=0.85,
        edgecolor="gray"
    )
)

# Title
plt.title("Global GDACS Flood Events 2015-2026", fontsize=16, weight="bold", pad=20)

plt.tight_layout()
plt.savefig("global_flood_map.png", dpi=300, bbox_inches="tight")
plt.show()
