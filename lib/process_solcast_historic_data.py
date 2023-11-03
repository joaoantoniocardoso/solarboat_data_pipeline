import pandas as pd
from pytz import timezone
import scipy as sp
from pvlib import irradiance, location


def integrate(df, time_constant):
    """
    Integrates a datetime indexed dataframe relative to a time_constant (3600 to return in hours)
    """
    if df.index.freq is None:
        df.index.freq = pd.infer_freq(df.index)
    return df.apply(
        sp.integrate.cumtrapz,
        initial=0,
        dx=((df.index.freq.nanos * 1e-9) / time_constant),  # type: ignore
    )


def get_irradiance(site_location, tilt, surface_azimuth, weather_data):
    solar_position = site_location.get_solarposition(times=weather_data.index)

    POA_irradiance = irradiance.get_total_irradiance(
        surface_tilt=tilt,
        surface_azimuth=surface_azimuth,
        dni=weather_data["dni"],
        ghi=weather_data["ghi"],
        dhi=weather_data["dhi"],
        solar_zenith=solar_position["apparent_zenith"],
        solar_azimuth=solar_position["azimuth"],
    )

    return pd.DataFrame({"POA": POA_irradiance["poa_global"]})


def process(input_file: str, output_file: str, site: location.Location, event: dict):
    df = pd.read_csv(
        input_file,
        index_col="PeriodStart",
    )
    df.drop(columns=["PeriodEnd", "Period"], inplace=True)
    df.rename(columns={"Dni": "dni", "Ghi": "ghi", "Dhi": "dhi"}, inplace=True)
    df.index = pd.to_datetime(df.index).tz_convert(site.tz)
    df.index.freq = pd.infer_freq(df.index)  # type: ignore

    df["poa"] = get_irradiance(
        site_location=site, tilt=0, surface_azimuth=0, weather_data=df
    )

    df = df[event["time"]["start"] : event["time"]["end"]]

    df["energy"] = integrate(df, 3600)["poa"]

    df.to_csv(output_file)
