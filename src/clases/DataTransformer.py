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
    __data_frame_dictionary: DataFrame | None = None

    def __init__(self, path_raw: str, path_clean: str, path_dic: str):
        self.__data_frame_raw = pd.read_csv(path_raw)
        self.__data_frame_clean = pd.read_csv(path_clean)
        self.__data_frame_dictionary = pd.read_csv(path_dic)

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

    # ------------------------------------------------------------------
    # Comparación de tipos de datos
    # ------------------------------------------------------------------

    def __compare_dataframes_dtypes(self) -> DataFrame:
        """
        Compara los tipos de datos entre el DataFrame original y el transformado.
        Valida contra el diccionario de datos.
        """
        dtype_mapping = {
            'string': ['str'],
            'string/date': ['datetime64[us]'],
            'integer': ['int64', 'int32'],
            'numeric': ['float64', 'int64'],
            'categorical': ['object', 'category', 'str'],
            'binary': ['int64', 'int32', 'uint8'],
            'numeric/string': ['float64', 'int64', 'int32'],
        }
        dict_types = self.__data_frame_dictionary.set_index('column_name')['data_type'].to_dict()

        records = []
        for col in self.__data_frame_raw.columns:
            raw_dtype = str(self.__data_frame_raw[col].dtype)
            clean_dtype = (
                str(self.__data_frame_clean[col].dtype)
                if col in self.__data_frame_clean.columns
                else 'NO EXISTE'
            )
            expected_type = dict_types.get(col, 'No definido')
            acceptable_types = dtype_mapping.get(expected_type, [])
            is_valid = clean_dtype in acceptable_types if acceptable_types else None

            records.append({
                'Columna': col,
                'Dtype Diccionario': expected_type,
                'Dtype Original': raw_dtype,
                'Dtype Transformado': clean_dtype,
                'Válido': '✅' if is_valid else '❌' if is_valid is False else '⚠️',
            })

        return pd.DataFrame(records)

    # ------------------------------------------------------------------
    # Transformación principal
    # ------------------------------------------------------------------

    def transform(self) -> DataFrame:
        """
        Aplica el pipeline completo de transformación sobre el DataFrame raw:
          A. Verificación de que las columnas del dataframe tenga el mismo tipo de acuerdo al diccionario.
          A. Eliminación de duplicados
          B. Corrección de formatos
          C. Imputación de valores nulos
          D. Gestión de outliers (IQR)
          E. Feature engineering
        Retorna el DataFrame transformado y lo almacena internamente.
        """
        df = self.__data_frame_raw.copy()
        print(f"Registros iniciales cargados: {len(df)}")

        # ── A. Verificación de tipos de columnas ──────────────────────────────
        print(f"Comparación de tipos de datos entre DataFrame original, transformado y diccionario:")
        print(self.__compare_dataframes_dtypes(self.__data_frame_raw, df, self.__data_frame_dictionary).to_string())

        # ── A. Tratamiento de Duplicados ──────────────────────────────
        df = df.drop_duplicates(subset=['user_id'], keep='last')
        print(f"Registros tras eliminar duplicados por user_id: {len(df)}")

        # ── B. Corrección de Formatos ─────────────────────────────────
        df['device_type'] = df['device_type'].str.lower().str.strip()

        df['discount_offered_pct'] = (
            df['discount_offered_pct']
            .astype(str)
            .str.replace('%', '', regex=False)
            .astype(float)
        )

        df['monthly_income_usd'] = (
            df['monthly_income_usd']
            .astype(str)
            .str.replace('$', '', regex=False)
            .str.replace('<NA>', 'nan', regex=False)
            .astype(float)
        )

        # Parsear columnas de fecha con formatos mixtos
        if 'signup_date' in df.columns:
            df['signup_date'] = self.__parse_mixed_dates(df['signup_date'])
        if 'trial_end_date' in df.columns:
            df['trial_end_date'] = self.__parse_mixed_dates(df['trial_end_date'])

        # ── C. Imputación de Valores Nulos ────────────────────────────
        cols_num_nulas = ['age', 'avg_session_minutes', 'satisfaction_score', 'monthly_income_usd']
        for col in cols_num_nulas:
            n_null = df[col].isna().sum()
            if n_null > 0:
                median_val = df[col].median()
                df[col] = df[col].fillna(median_val)
                print(f"  {col}: {n_null} nulos imputados con mediana ({median_val:.2f})")

        df['device_type'] = df['device_type'].fillna('desconocido')
        df['country'] = df['country'].fillna('desconocido')

        # ── D. Gestión de Outliers (IQR) ──────────────────────────────
        columnas_con_outliers = ['avg_session_minutes', 'monthly_income_usd', 'sessions_count']
        for col in columnas_con_outliers:
            df[col], n_low, n_up, lb, ub = self.__cap_outliers_iqr(df[col])
            if n_low + n_up > 0:
                print(f"  {col}: {n_low} outliers inferiores, {n_up} outliers superiores "
                      f"(rango: [{lb:.1f}, {ub:.1f}])")

        # ── E. Feature Engineering ────────────────────────────────────
        df['sessions_per_active_day'] = np.where(
            df['days_active_trial'] > 0,
            df['sessions_count'] / df['days_active_trial'],
            0
        )
        df['total_minutes_used'] = df['sessions_count'] * df['avg_session_minutes']
        df['activity_intensity'] = df['features_used'] * df['days_active_trial']
        df['high_commercial_intent'] = (
            (df['plan_page_views'] > 1) & (df['payment_method_on_file'] == 1)
        ).astype(int)

        print(f"Registros finales tras transformación: {len(df)}")

        self.__data_frame_clean = df
        return df
