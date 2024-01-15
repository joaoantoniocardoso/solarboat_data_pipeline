import numpy as np
import pandas as pd
import pvlib
import scipy as sp


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


def get_irradiance(
    site_location: pvlib.location.Location,
    weather_data: pd.DataFrame,
    surface_tilt: float,
    surface_azimuth: float,
):
    """
    Get irradiance components to plane-of-array, incorporating
    a timeshift in the solar position calculation of T/2,
    being T the data's period.

    Parameters
    ----------
        weather_data: DataFrame
            Has columns dni, ghi, dhi, albedo
        timeshift: float
            Number of minutes to shift for solar position calculation
    Outputs:
        Series of POA irradiance
    """
    idx = weather_data.index
    data_period = np.diff(weather_data.index[:2])

    # calculate solar position for shifted timestamps:
    idx = idx + (data_period / 2)
    solar_position = site_location.get_solarposition(idx)
    # but still report the values with the original timestamps:
    solar_position.index = weather_data.index

    dni_extra = pvlib.irradiance.get_extra_radiation(solar_position.index)

    air_mass = site_location.get_airmass(
        times=solar_position.index,
        solar_position=solar_position,
        model="kastenyoung1989",
    )

    poa_components = pvlib.irradiance.get_total_irradiance(
        surface_tilt=surface_tilt,
        surface_azimuth=surface_azimuth,
        solar_zenith=solar_position["apparent_zenith"],
        solar_azimuth=solar_position["azimuth"],
        dni=weather_data["dni"],
        ghi=weather_data["ghi"],
        dhi=weather_data["dhi"],
        dni_extra=dni_extra,
        airmass=air_mass,
        albedo=weather_data["albedo"],
        surface_type="sea",
        model="isotropic",
    )
    return pd.DataFrame({"POA": poa_components["poa_global"]})


def process(
    input_file: str, output_file: str, site: pvlib.location.Location, event: dict
):
    df = pd.read_csv(
        input_file,
        index_col="PeriodStart",
    )
    df.drop(columns=["PeriodEnd", "Period"], inplace=True)
    df.rename(
        columns={
            "Dni": "dni",
            "Ghi": "ghi",
            "Dhi": "dhi",
            "Airmass": "airmass",
            "AlbedoDaily": "albedo",
        },
        inplace=True,
    )
    df.index = pd.to_datetime(df.index).tz_convert(site.tz)
    df.index.freq = pd.infer_freq(df.index)  # type: ignore

    df["poa"] = get_irradiance(
        site_location=site,
        weather_data=df,
        surface_tilt=0,
        surface_azimuth=0,
    )

    df = df[event["time"]["start"] : event["time"]["end"]]

    df["energy"] = integrate(df, 3600)["poa"]

    df.to_csv(output_file)
