# Import of required libraries-------------------------------------------------
import glob
from os.path import join as opj
import pandas as pd
from traffic.core import Traffic

# Import of MeteoSchweiz T/RH/P data-------------------------------------------
weather_data_path = "/mnt/beegfs/store/MIAR/raw/meteo/T-RH_QFE-SMN_KLO"
list_csv = glob.glob(opj(weather_data_path, "*.csv"))
df_weather = pd.concat(
    [
        pd.read_csv(
            f,
            sep=";",
            header=None,
            index_col=None,
            parse_dates=False,
            names=[
                "timestamp",
                "temperature_gnd",
                "humidity_gnd",
                "pressure_gnd",
            ],
        )
        for f in list_csv
    ],
    axis=0,
)
df_weather["timestamp"] = pd.to_datetime(
    df_weather["timestamp"], format="%d.%m.%Y %H:%M:%S", utc=True
)


# Import of MeteoSchweiz wind data---------------------------------------------
wind_data_path = "/mnt/beegfs/store/MIAR/raw/meteo/Wind_LSZH_2018-2023"
list_csv_C = glob.glob(
    opj(wind_data_path, "**/*Kloten_Wind_C*.csv.zip"), recursive=True
)


def read_wind(list_csv: list) -> pd.DataFrame:
    df_wind = pd.concat(
        [
            pd.read_csv(
                f,
                sep=";",
                header=None,
                index_col=0,
                parse_dates=True,
                names=["date", "wind_speed_gnd", "wind_direction_gnd"],
                usecols=[0, 1, 2],
            )
            for f in list_csv
        ],
        axis=0,
    ).sort_index()
    df_wind = df_wind.reset_index().rename(columns={"date": "timestamp"})
    df_wind["timestamp"] = df_wind["timestamp"].dt.tz_localize("UTC")
    return df_wind


wind_28 = read_wind(list_csv_C)


# Import of FZAG mass/typecode data--------------------------------------------
fzag_data_path = "/mnt/beegfs/store/MIAR/raw/FZAG"
df_departures = pd.read_csv(
    f"{fzag_data_path}/df_departure.csv",
    sep=",",
    header=0,
    index_col=0,
)
df_departures = (
    pd.read_csv(
        f"{fzag_data_path}/df_departure.csv", sep=",", header=0, index_col=0
    )
    .rename(
        columns={
            "SDT": "date",
            "CSG": "callsign",
            "TWT": "toff_weight_kg",
            "ITY": "typecode",
        }
    )
    .drop(columns=["REG"])
)
df_departures["date"] = pd.to_datetime(df_departures["date"])


# Import of SAMAX data---------------------------------------------------------
samax_data_path = "/mnt/beegfs/store/MIAR/raw/SAMAX"

cols2keep = {
    "Time [ms since 1.1.1970]": "timestamp",
    "SwissGrid LV95 x Float [m]": "x",
    "SwissGrid LV95 y Float [m]": "y",
    "NC-131 Aircraft ID (Downlinked Callsign) - String": "callsign",
    "NC-059 Mode S Address - [hex]": "icao24",
    "I081/090 Mode C - Float [ft]": "altitude",
    "NC-012 Calculated Altitude - Float [ft]": "geoaltitude",
    "I081/140 Calculated Rate of Climb/Descent - Float [ft/s]": "vertical_rate",
    "NC-114 Velocity Real vx - Float [m/s]": "v_x",
    "NC-114 Velocity Real vy - Float [m/s]": "v_y",
    "Velocity Real - Float [kt]": "groundspeed",
    "NC-027 Arrival Airport - String": "destination",
    "NC-056 QNH - Float [hPa]": "QNH",
    "Target Nose Pos. LV95 x - Float [m]": "nose_x",
    "Target Nose Pos. LV95 y - Float [m]": "nose_y",
    "Target Tail Pos. LV95 x - Float [m]": "tail_x",
    "Target Tail Pos. LV95 y - Float [m]": "tail_y",
    "NC-125 Clearance Status - String": "clearance",
    "NC-109 Ground Bit - Int [ |0|1]": "on_ground",
    "NC-138 BDS60 Register Magnetic Heading - Float [deg]": "mag_heading",
    "NC-139 BDS50 Register True Track - Float [deg]": "track",
    "NC-138 BDS60 Register IAS - Float [kt]": "IAS",
    "NC-139 BDS50 Register TAS - Int [kt]": "TAS",
    "NC-046 Target Direction - Int [ |1..6] (1=ARR/2=DEP/3=LOC/4=TRANS/5=MOV/6=UNK)": "direction",
    "NC-052 Allocated RWY - String": "RWY",
}

col2type = {
    "RWY_lnd": str,
    "destination": str,
    "departure": str,
    "RWY": str,
    "registration": str,
    "thr": str,
    "FR": str,
}

# load selected columns and rename with values
df_samax = pd.read_csv(
    f"{samax_data_path}/dtt.csv.gz",
    sep=";",
    usecols=cols2keep.keys(),
    parse_dates=True,
).rename(columns=cols2keep)
# cahnge type of some columns:
df_samax["timestamp"] = pd.to_datetime(
    df_samax["timestamp"], unit="ms", utc=True
)
df_samax["RWY"] = df_samax["RWY"].apply(
    lambda x: str(int(x)) if pd.notna(x) and isinstance(x, float) else x
)
df_samax["RWY_lnd"] = df_samax["RWY_lnd"].apply(
    lambda x: str(int(x)) if pd.notna(x) and isinstance(x, float) else x
)

# Reduce to Takeoffs from RWY 28
allflights = Traffic(df_samax).assign_id().eval(max_workers=20, desc="eval")
toffs28 = allflights.query("RWY=='28' and direction==2")
toffs28.data = toffs28.data.drop(["direction", "RWY"], axis=1)


# Data merging-----------------------------------------------------------------
# Additional data is added to the SAMAX data
# FZAG mass/typecode data
toffs28.data["date"] = pd.to_datetime(toffs28.data["timestamp"].dt.date)
toffs28.data = pd.merge(
    left=toffs28.data,
    right=df_departures,
    left_on=["date", "callsign"],
    right_on=["date", "callsign"],
    how="left",
)

# MeteoSchweiz wind data
toffs28.data = pd.merge_asof(
    left=toffs28.data.sort_values("timestamp"),
    right=wind_28.sort_values("timestamp"),
    on="timestamp",
    tolerance=pd.Timedelta("30min"),
)

# MeteoSchweiz T/RH/P data
toffs28.data = pd.merge_asof(
    left=toffs28.data.sort_values("timestamp"),
    right=df_weather.sort_values("timestamp"),
    on="timestamp",
    tolerance=pd.Timedelta("30min"),
)


# Data export------------------------------------------------------------------
output_dir = "/mnt/beegfs/store/MIAR/merged"
toffs28.to_parquet(f"{output_dir}/takeoffs28.parquet")
