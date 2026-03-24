import pandas as pd
import numpy as np
from pandas import DataFrame

class DataTransformer:
    """
    Clase que contiene toda la lógica para la transformación del dataset
    para dejarlo listo para el siguiente paso de la ETL.
    """
    __data_frame_raw: DataFrame | None = None
    __data_frame_clean: DataFrame | None = None
    __path_data_frame_clean: str = ""

    def __init__(self, path_raw: str, path_clean: str, path_dic: str):
        self.__data_frame_raw = pd.read_csv(path_raw)
        self.__data_frame_dictionary = pd.read_csv(path_dic)
        self.__path_data_frame_clean = path_clean

    @staticmethod
    def __parse_mixed_dates(series: pd.Series) -> pd.Series:
        """Parsea fechas con formatos mixtos (YYYY-MM-DD, DD/MM/YYYY, MM-DD-YYYY)."""
        parsed = pd.to_datetime(series, format='%Y-%m-%d', errors='coerce')

        mask_null = parsed.isna()
        if mask_null.any():
            parsed[mask_null] = pd.to_datetime(series[mask_null], format='%d/%m/%Y', errors='coerce')

        mask_null = parsed.isna()
        if mask_null.any():
            parsed[mask_null] = pd.to_datetime(series[mask_null], format='%m-%d-%Y', errors='coerce')

        return parsed

    @staticmethod
    def __cap_outliers_iqr(series: pd.Series, factor: float = 1.5):
        """Capea outliers usando el método IQR."""
        Q1 = series.quantile(0.25)
        Q3 = series.quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - factor * IQR
        upper = Q3 + factor * IQR
        capped = series.clip(lower=lower, upper=upper)
        return capped, (series < lower).sum(), (series > upper).sum(), lower, upper

    @staticmethod
    def __compare_dataframes_dtypes(df_raw, df_clean, df_dic):
       """
       Compara los tipos de datos entre el DataFrame original y el transformado.
       Incluye un ejemplo de valor de cada uno para mayor claridad.
       """
       records = []
       dtype_mapping = {
           'string': ['str'],
           'string/date': ['datetime64[us]'],
           'integer': ['int64', 'int32'],
           'numeric': ['float64', 'int64'],
           'categorical': ['object', 'category', "str"],
           'binary': ['int64', 'int32', 'uint8'],
           'numeric/string': ['float64', 'int64', 'int32'],
       }
       dict_types = df_dic.set_index('column_name')['data_type'].to_dict()
       for col in df_raw.columns:
           clean_dtype = str(df_clean[col].dtype) if col in df_clean.columns else 'NO EXISTE'
           expected_type = dict_types.get(col, 'No definido')
           acceptable_types = dtype_mapping.get(expected_type, [])
           is_valid = clean_dtype in acceptable_types if acceptable_types else None
           records.append({
               'Columna': col,
               'Dtype Dictionario': expected_type,
               'Dtype Original': str(df_raw[col].dtype),
               'Dtype Transformado': str(df_clean[col].dtype),
               'Válido': '✅' if is_valid else '❌' if is_valid is False else '⚠️',
           })

       comparison_df = pd.DataFrame(records)

       return comparison_df

    def transform(self) -> None:
        """
        Aplica el pipeline completo de transformación sobre el DataFrame raw:
            1. Verificación de que las columnas del dataframe tenga el mismo tipo de acuerdo al diccionario.
             Cada vez que se hace un transformacion, se compara como esta quedado el dataframe para verificar que tenga
             los tipos de datos correspondientes.

                A. Verificacion de tipos de columnas para saber como vamos a procesar la información y detectar posibles errores de formato.
                 Cada vez que se hace un transformacion, se compara como esta quedado el dataframe para verificar
                B. Corrección de formatos en los campos signup_date y trial_end_date
                C. Eliminación de duplicados
                D. Ajuste de formatos numericos
                E. Imputación de Valores Nulos
                F. Gestión de Outliers (IQR)
                G. Feature Engineering.
        """

        self.__data_frame_clean = self.__data_frame_raw.copy()
        print(f"Registros iniciales cargados: {len(self.__data_frame_clean )}")

        print("\n" + "+"*140)
        print(f"A. Comparación de tipos de datos entre DataFrame original, transformado y diccionario:")
        print(self.__compare_dataframes_dtypes(self.__data_frame_raw, self.__data_frame_clean, self.__data_frame_dictionary).to_string())

        print("\n" + "+" * 140)
        print("B. Corrección de formatos en los campos signup_date y trial_end_date.")

        if 'signup_date' in self.__data_frame_clean.columns:
            self.__data_frame_clean['signup_date'] = self.__parse_mixed_dates(self.__data_frame_clean['signup_date'])
        if 'trial_end_date' in self.__data_frame_clean.columns:
            self.__data_frame_clean['trial_end_date'] = self.__parse_mixed_dates(self.__data_frame_clean['trial_end_date'])

        print("\n" + "+" * 140)
        print(f"Comparación de tipos de datos entre DataFrame original, transformado y diccionario despues del arreglas "
              f"los campos de fecha")
        print(self.__compare_dataframes_dtypes(self.__data_frame_raw, self.__data_frame_clean, self.__data_frame_dictionary).to_string())

        print("\n" + "+" * 140)
        print("C. Eliminación de duplicados por user_id, manteniendo el último registro para cada usuario.")

        self.__data_frame_clean = self.__data_frame_clean.drop_duplicates(subset=['user_id'], keep='last')

        print(f"Registros tras eliminar duplicados por user_id: {len(self.__data_frame_clean)}")

        print("\n" + "+" * 140)
        print("D. Ajuste de formatos numéricos: eliminamos símbolos y convertimos a tipos numéricos.")

        self.__data_frame_clean['device_type'] = self.__data_frame_clean['device_type'].str.lower().str.strip()

        self.__data_frame_clean['discount_offered_pct'] = (
            self.__data_frame_clean['discount_offered_pct']
            .astype(str)
            .str.replace('%', '', regex=False)
            .astype(float)
        )

        self.__data_frame_clean['monthly_income_usd'] = (
            self.__data_frame_clean['monthly_income_usd']
            .astype(str)
            .str.replace('$', '', regex=False)
            .str.replace('<NA>', 'nan', regex=False)
            .astype(float)
        )

        print(f"Comparación de tipos de datos entre DataFrame original, transformado y diccionario despues del arreglas "
              f"discount_offered_pct y monthly_income_usd")
        print(self.__compare_dataframes_dtypes(self.__data_frame_raw, self.__data_frame_clean,
                                               self.__data_frame_dictionary).to_string())


        print("\n" + "+" * 140)
        print("E. Imputación de Valores Nulos: se imputan valores nulos en columnas numéricas con la mediana y en columnas categóricas con 'desconocido'. ")
        cols_num_nulas = ['age', 'avg_session_minutes', 'satisfaction_score', 'monthly_income_usd']
        for col in cols_num_nulas:
            n_null = self.__data_frame_clean[col].isna().sum()
            if n_null > 0:
                median_val = self.__data_frame_clean[col].median()
                self.__data_frame_clean[col] = self.__data_frame_clean[col].fillna(median_val)
                print(f"  {col}: {n_null} nulos imputados con mediana ({median_val:.2f})")

        self.__data_frame_clean['device_type'] = self.__data_frame_clean['device_type'].fillna('desconocido')
        self.__data_frame_clean['country'] = self.__data_frame_clean['country'].fillna('desconocido')


        print("\n" + "+" * 140)
        print("F. Gestión de Outliers: se identifican y capean outliers en columnas numéricas usando el método IQR.")

        columnas_con_outliers = ['avg_session_minutes', 'monthly_income_usd', 'sessions_count']
        for col in columnas_con_outliers:
            self.__data_frame_clean[col], n_low, n_up, lb, ub = self.__cap_outliers_iqr(self.__data_frame_clean[col])
            if n_low + n_up > 0:
                print(f"  {col}: {n_low} outliers inferiores, {n_up} outliers superiores "
                      f"(rango: [{lb:.1f}, {ub:.1f}])")


        print("\n" + "+" * 140)
        print("G. Feature Engineering: se crean nuevas columnas basadas en combinaciones de las existentes para capturar patrones más complejos.")
        self.__data_frame_clean['sessions_per_active_day'] = np.where(
            self.__data_frame_clean['days_active_trial'] > 0,
            self.__data_frame_clean['sessions_count'] / self.__data_frame_clean['days_active_trial'],
            0
        )
        self.__data_frame_clean['total_minutes_used'] = self.__data_frame_clean['sessions_count'] * self.__data_frame_clean['avg_session_minutes']
        self.__data_frame_clean['activity_intensity'] = self.__data_frame_clean['features_used'] * self.__data_frame_clean['days_active_trial']
        self.__data_frame_clean['high_commercial_intent'] = (
            (self.__data_frame_clean['plan_page_views'] > 1) & (self.__data_frame_clean['payment_method_on_file'] == 1)
        ).astype(int)

        print(f"Registros finales tras transformación: {len(self.__data_frame_clean)}")

    def guardar_data_frame_clean(self) -> None:
        """
            Esta funcion guarda el dataframe limpio con todas sus columnas incluidas las nuevas columnas de feature engineering,
            en un nuevo archivo CSV para su posterior uso en la etapa de carga.
        """
        if self.__data_frame_clean is not None:
            self.__data_frame_clean.to_csv(self.__path_data_frame_clean, index=False)
            print(f"DataFrame limpio guardado en: {self.__path_data_frame_clean}")
        else:
            print("No hay DataFrame limpio para guardar.")

    def get_dataframe_clean(self) -> pd.DataFrame:
        """
            Retorna el DataFrame limpio después de aplicar todas las transformaciones.
        """
        return self.__data_frame_clean
