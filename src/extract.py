from typing import Dict

import requests
from pandas import DataFrame, read_csv, read_json, to_datetime

def temp() -> DataFrame:
    """Get the temperature data.
    Returns:
        DataFrame: A dataframe with the temperature data.
    """
    return read_csv("dataset/temperature.csv")

def get_public_holidays(public_holidays_url: str, year: str) -> DataFrame:
    """Get the public holidays for the given year for Brazil.
    Args:
        public_holidays_url (str): url to the public holidays.
        year (str): The year to get the public holidays for.
    Raises:
        SystemExit: If the request fails.
    Returns:
        DataFrame: A dataframe with the public holidays.
    """
    
    #--------------------------------------------------------------------------------------------------
    # TODO: Implementa esta función.                                                                   
    # Debes usar la biblioteca requests para obtener los días festivos públicos del año dado.✅          
    # La URL es public_holidays_url/{year}/BR. ✅
    # Debes eliminar las columnas "types" y "counties" del DataFrame. ✅
    # Debes convertir la columna "date" a datetime. ✅
    # Debes lanzar SystemExit si la solicitud falla. Investiga el método raise_for_status. ✅
    # de la biblioteca requests.
    #--------------------------------------------------------------------------------------------------

    url = f"{public_holidays_url}/{year}/BR"

    try:
        response = requests.get(url)
        response.raise_for_status() 

        data = response.json()  
        df = DataFrame.from_records(data)  
        
        if "date" in df.columns:
            df["date"] = to_datetime(df["date"]) # Convertir la columna "date" a datetime.
        
        df = df.drop(columns=["types", "counties"], errors="ignore") # eliminar las columnas "types" y "counties" del DataFrame.

        return df
    except requests.exceptions.RequestException as err:
        print(f"Error en la solicitud: {err}")
        raise SystemExit(err) # lanzar SystemExit si la solicitud falla. Investiga el método raise_for_status

def extract(
    csv_folder: str, csv_table_mapping: Dict[str, str], public_holidays_url: str
) -> Dict[str, DataFrame]:
    """Extract the data from the csv files and load them into the dataframes.
    Args:
        csv_folder (str): Ruta de la carpeta con los CSV.
        csv_table_mapping (Dict[str, str]): Mapeo de nombres de archivo CSV a nombres de tabla.
        public_holidays_url (str): URL base de los días festivos.
    Returns:
        Dict[str, DataFrame]: Diccionario con los nombres de tabla como claves y los DataFrames como valores.
    """
    dataframes = {
        table_name: read_csv(f"{csv_folder}/{csv_file}")
        for csv_file, table_name in csv_table_mapping.items()
    }

    holidays = get_public_holidays(public_holidays_url, "2017")

    dataframes["public_holidays"] = holidays

    return dataframes

if __name__ == "__main__":
    df = get_public_holidays("https://date.nager.at/api/v3/PublicHolidays", "2017")
    print(df.shape)
