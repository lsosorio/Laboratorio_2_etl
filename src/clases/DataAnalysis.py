import os
import pprint
import warnings

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pandas import DataFrame

warnings.filterwarnings('ignore')

sns.set_theme(style="whitegrid", palette="muted")
plt.rcParams.update({'figure.dpi': 150, 'font.size': 10})


class DataAnalysis:
    __data_frame: DataFrame | None = None

    def __init__(self, path: str) -> None:
        self.__data_frame = pd.read_csv(path)

    # ------------------------------------------------------------------
    # Utilidades privadas de graficación (sin modificar el dataframe)
    # ------------------------------------------------------------------

    @staticmethod
    def __to_num(series: pd.Series) -> pd.Series:
        """Convierte una Serie a numérico en una variable local temporal,
        sin modificar el DataFrame original.
        Elimina prefijos/sufijos $ y % solo para poder graficar."""
        s = series.astype(str).str.replace(r'[$%]', '', regex=True).str.strip()
        return pd.to_numeric(s, errors='coerce')

    @staticmethod
    def __save(fig: plt.Figure, path: str) -> None:
        fig.savefig(path, bbox_inches='tight')
        plt.close(fig)
        print(f"  ✅  Guardada: {path}")

    def EDA(self, output_path: str = "../EDA") -> None:
        """
            Funcion que se encarga de hacer el analisis exploratorio de los datos, con el fin de entender mejor el dataset
            y generar gráficas que nos ayuden a visualizar la información para hacer los cambios respectivos en el proceso de limpieza y transformación de los datos.
            :param output_path: Path donde se guardaran las imagenes que se van a generar
            :return:
        """
        os.makedirs(output_path, exist_ok=True)
        df = self.__data_frame           # referencia — NO se modifica

        # ── Resumen en consola ─────────────────────────────────────────
        print("Análisis del dataset:")

        print("\n" + "=" * 140)
        print("Cantidad de filas y columnas del dataframe:", df.shape)

        print("\n" + "=" * 140)
        print("Columnas en el dataset")
        pprint.pprint(df.columns.tolist())

        print("\n" + "=" * 140)
        print("Tipo de columnas: ")
        print(df.dtypes)

        print("\n" + "=" * 140)
        print("Estadísticas básicas para el dataframe:")
        print(df.describe().to_string())

        print("\n" + "=" * 140)
        print("Valores Faltantes por columna:")
        missing = df.isnull().sum()
        print(missing[missing > 0])

        print("\n" + "=" * 140)
        print("Generación de gráficas del dataset:")

        # ── Grupos de variables ────────────────────────────────────────
        # Variables cuyo dtype ya es numérico en el CSV crudo
        numeric_raw    = df.select_dtypes(include=[np.number]).columns.tolist()
        # Variables que contienen números pero con símbolos ($ o %)
        mixed_num      = ['discount_offered_pct', 'monthly_income_usd']
        # Todas las que necesitamos tratar como numéricas para graficar
        continuous_vars = ['age', 'avg_session_minutes', 'satisfaction_score',
                           'monthly_income_usd', 'discount_offered_pct']
        discrete_vars   = ['trial_length_days', 'days_active_trial', 'sessions_count',
                           'features_used', 'support_tickets', 'emails_opened',
                           'plan_page_views', 'last_activity_gap_days']
        binary_vars     = ['webinar_attended', 'payment_method_on_file',
                           'referred_friend', 'converted_to_paid_plan']
        categorical_vars = ['country', 'gender', 'device_type', 'acquisition_channel',
                            'city_tier', 'preferred_plan_before_conversion']
        key_numeric_vars = ['age', 'days_active_trial', 'sessions_count',
                            'avg_session_minutes', 'features_used', 'emails_opened',
                            'plan_page_views', 'satisfaction_score',
                            'monthly_income_usd', 'discount_offered_pct']
        outlier_vars    = ['sessions_count', 'avg_session_minutes', 'features_used',
                           'monthly_income_usd', 'support_tickets', 'age']
        target          = 'converted_to_paid_plan'

        
        # GRÁFICA 01 — Variable objetivo: distribución de clases
        counts = df[target].value_counts().sort_index()
        labels = [f'{idx} – {"Convertido" if idx == 1 else "No Convertido"}'
                  for idx in counts.index]
        colors = ['#E74C3C', '#2ECC71']

        fig, axes = plt.subplots(1, 2, figsize=(12, 5))
        fig.suptitle('Distribución de la Variable Objetivo (datos crudos)',
                     fontsize=14, fontweight='bold')

        axes[0].bar(labels, counts.values, color=colors, edgecolor='white')
        axes[0].set_title('Conteo de registros')
        axes[0].set_ylabel('Cantidad')
        for i, v in enumerate(counts.values):
            axes[0].text(i, v + 3, str(v), ha='center', fontweight='bold')

        axes[1].pie(counts.values, labels=labels, colors=colors,
                    autopct='%1.1f%%', startangle=140,
                    wedgeprops={'edgecolor': 'white'})
        axes[1].set_title('Proporción (%)')

        plt.tight_layout()
        self.__save(fig, os.path.join(output_path, '01_dist_variable_objetivo.png'))

        
        # GRÁFICA 02 — Valores nulos por columna
        null_pct = (df.isnull().sum() / len(df) * 100).sort_values(ascending=False)

        fig, ax = plt.subplots(figsize=(14, 5))
        bars = ax.bar(null_pct.index, null_pct.values,
                      color=['#E74C3C' if v > 0 else '#AED6F1' for v in null_pct.values],
                      edgecolor='white')
        ax.set_title('Porcentaje de Valores Nulos por Columna (datos crudos)',
                     fontsize=13, fontweight='bold')
        ax.set_ylabel('% nulos')
        ax.set_ylim(0, max(null_pct.values) * 1.25 + 1)
        plt.xticks(rotation=45, ha='right', fontsize=8)
        for bar, v in zip(bars, null_pct.values):
            if v > 0:
                ax.text(bar.get_x() + bar.get_width() / 2,
                        v + 0.1, f'{v:.1f}%', ha='center', fontsize=8)
        plt.tight_layout()
        self.__save(fig, os.path.join(output_path, '02_valores_nulos.png'))

        
        # GRÁFICA 03 — Tipos de datos por columna
        dtype_counts = df.dtypes.astype(str).value_counts()

        fig, axes = plt.subplots(1, 2, figsize=(14, 5))
        fig.suptitle('Tipos de Datos en el Dataset (datos crudos)',
                     fontsize=13, fontweight='bold')

        # Barras
        axes[0].bar(dtype_counts.index, dtype_counts.values,
                    color='#5DADE2', edgecolor='white')
        axes[0].set_title('Conteo de columnas por tipo')
        axes[0].set_ylabel('Número de columnas')
        for i, v in enumerate(dtype_counts.values):
            axes[0].text(i, v + 0.05, str(v), ha='center', fontweight='bold')

        # Tabla con dtype de cada columna
        col_types = df.dtypes.reset_index()
        col_types.columns = ['Columna', 'Dtype']
        axes[1].axis('off')
        tbl = axes[1].table(
            cellText=col_types.values,
            colLabels=col_types.columns,
            loc='center', cellLoc='left'
        )
        tbl.auto_set_font_size(False)
        tbl.set_fontsize(7.5)
        tbl.scale(1, 1.3)
        axes[1].set_title('Dtype por columna', fontsize=10, fontweight='bold')

        plt.tight_layout()
        self.__save(fig, os.path.join(output_path, '03_tipos_de_datos.png'))

        
        # GRÁFICA 04 — Distribución de variables continuas
        # (conversión numérica local solo para graficar)
        ncols = 3
        nrows = -(-len(continuous_vars) // ncols)
        fig, axes = plt.subplots(nrows, ncols, figsize=(16, nrows * 4))
        axes = axes.flatten()
        fig.suptitle('Distribución de Variables Continuas (datos crudos)',
                     fontsize=14, fontweight='bold')

        for idx, var in enumerate(continuous_vars):
            ax = axes[idx]
            data = self.__to_num(df[var]).dropna()
            n_raw = len(df[var])
            n_parsed = len(data)
            n_failed = n_raw - n_parsed

            ax.hist(data, bins=30, color='steelblue', alpha=0.7, edgecolor='white',
                    density=True)
            try:
                data.plot.kde(ax=ax, color='darkred', linewidth=2)
            except Exception:
                pass
            ax.axvline(data.mean(),   color='orange', linestyle='--', linewidth=1.5,
                       label=f'Media: {data.mean():.1f}')
            ax.axvline(data.median(), color='green',  linestyle='-.', linewidth=1.5,
                       label=f'Mediana: {data.median():.1f}')
            ax.set_title(var, fontsize=11, fontweight='bold')
            ax.legend(fontsize=7)
            if n_failed:
                ax.set_xlabel(f'⚠ {n_failed} valores no parseables ignorados',
                              fontsize=7, color='red')

        for j in range(len(continuous_vars), len(axes)):
            axes[j].set_visible(False)

        plt.tight_layout(rect=(0, 0, 1, 0.95))
        self.__save(fig, os.path.join(output_path, '04_dist_continuas.png'))

        
        # GRÁFICA 05 — Distribución de variables discretas
        ncols = 3
        nrows = -(-len(discrete_vars) // ncols)
        fig, axes = plt.subplots(nrows, ncols, figsize=(18, nrows * 4))
        axes = axes.flatten()
        fig.suptitle('Distribución de Variables Discretas (datos crudos)',
                     fontsize=14, fontweight='bold')

        for idx, var in enumerate(discrete_vars):
            ax = axes[idx]
            data = self.__to_num(df[var]).dropna()
            ax.hist(data, bins=min(30, int(data.nunique())),
                    color='teal', alpha=0.7, edgecolor='white')
            stats_txt = (f'n={len(data)}\nμ={data.mean():.1f}'
                         f'\nσ={data.std():.1f}\nMd={data.median():.0f}')
            ax.text(0.97, 0.97, stats_txt, transform=ax.transAxes,
                    fontsize=8, va='top', ha='right',
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
            ax.set_title(var, fontsize=11, fontweight='bold')

        for j in range(len(discrete_vars), len(axes)):
            axes[j].set_visible(False)

        plt.tight_layout(rect=(0, 0, 1, 0.95))
        self.__save(fig, os.path.join(output_path, '05_dist_discretas.png'))

        
        # GRÁFICA 06 — Variables binarias
        fig, axes = plt.subplots(1, len(binary_vars), figsize=(16, 5))
        fig.suptitle('Distribución de Variables Binarias (datos crudos)',
                     fontsize=14, fontweight='bold')

        for idx, var in enumerate(binary_vars):
            ax = axes[idx]
            counts_bin = df[var].value_counts().sort_index()
            bars = ax.bar(counts_bin.index.astype(str), counts_bin.values,
                          color=['#E74C3C', '#2ECC71'], edgecolor='white', width=0.5)
            ax.set_title(var, fontsize=10, fontweight='bold')
            ax.set_xlabel('Valor')
            ax.set_ylabel('Cantidad')
            for bar in bars:
                ax.text(bar.get_x() + bar.get_width() / 2,
                        bar.get_height() + 2,
                        str(int(bar.get_height())),
                        ha='center', fontsize=9)

        plt.tight_layout(rect=(0, 0, 1, 0.93))
        self.__save(fig, os.path.join(output_path, '06_dist_binarias.png'))

        
        # GRÁFICA 07 — Variables categóricas: conteo (datos crudos,
        # se verán todas las inconsistencias de mayúsculas/tildes)
        ncols = 3
        nrows = -(-len(categorical_vars) // ncols)
        fig, axes = plt.subplots(nrows, ncols, figsize=(22, nrows * 5))
        axes = axes.flatten()
        fig.suptitle('Distribución de Variables Categóricas (datos crudos – sin normalizar)',
                     fontsize=13, fontweight='bold')

        for idx, var in enumerate(categorical_vars):
            ax = axes[idx]
            top_n = 20
            counts_cat = df[var].value_counts().head(top_n)
            counts_cat.plot(kind='barh', ax=ax, color='#5DADE2', edgecolor='white')
            ax.set_title(f'{var}  (top {top_n} valores)',
                         fontsize=10, fontweight='bold')
            ax.set_xlabel('Cantidad')
            ax.invert_yaxis()
            # Cantidad total de categorías únicas
            n_unique = df[var].nunique()
            ax.set_ylabel(f'{n_unique} categorías únicas', fontsize=8)

        for j in range(len(categorical_vars), len(axes)):
            axes[j].set_visible(False)

        plt.tight_layout(rect=(0, 0, 1, 0.95))
        self.__save(fig, os.path.join(output_path, '07_dist_categoricas.png'))

        
        # GRÁFICA 08 — Tasa de conversión por variable categórica
        ncols = 3
        nrows = -(-len(categorical_vars) // ncols)
        fig, axes = plt.subplots(nrows, ncols, figsize=(22, nrows * 5))
        axes = axes.flatten()
        fig.suptitle('Tasa de Conversión por Variable Categórica (datos crudos)',
                     fontsize=13, fontweight='bold')

        for idx, var in enumerate(categorical_vars):
            ax = axes[idx]
            top_vals = df[var].value_counts().head(15).index
            sub = df[df[var].isin(top_vals)]
            conv_rate = (sub.groupby(var)[target].mean() * 100).sort_values(ascending=False)
            conv_rate.plot(kind='barh', ax=ax, color='#E67E22', edgecolor='white')
            ax.set_title(f'Conversión por {var}', fontsize=10, fontweight='bold')
            ax.set_xlabel('Tasa de conversión (%)')
            ax.invert_yaxis()
            for i, v in enumerate(conv_rate):
                ax.text(v + 0.3, i, f'{v:.1f}%', va='center', fontsize=8)

        for j in range(len(categorical_vars), len(axes)):
            axes[j].set_visible(False)

        plt.tight_layout(rect=(0, 0, 1, 0.95))
        self.__save(fig, os.path.join(output_path, '08_categoricas_vs_objetivo.png'))

        
        # GRÁFICA 09 — Boxplots numéricos vs variable objetivo
        ncols = 5
        nrows = -(-len(key_numeric_vars) // ncols)
        fig, axes = plt.subplots(nrows, ncols, figsize=(22, nrows * 5))
        axes = axes.flatten()
        fig.suptitle('Boxplots por Conversión (conversión numérica local para graficar)',
                     fontsize=13, fontweight='bold')

        # DataFrame temporal solo para esta gráfica
        box_df = df[[target]].copy()
        for var in key_numeric_vars:
            box_df[var] = self.__to_num(df[var])

        for idx, var in enumerate(key_numeric_vars):
            ax = axes[idx]
            sns.boxplot(data=box_df, x=target, y=var, ax=ax,
                        hue=target,
                        palette={0: '#E74C3C', 1: '#2ECC71'},
                        width=0.5, legend=False)
            ax.set_title(var, fontsize=9, fontweight='bold')
            ax.set_xlabel('Convertido')
            ax.set_ylabel('')
            ax.set_xticks([0, 1])
            ax.set_xticklabels(['No (0)', 'Sí (1)'])

        for j in range(len(key_numeric_vars), len(axes)):
            axes[j].set_visible(False)

        plt.tight_layout(rect=(0, 0, 1, 0.95))
        self.__save(fig, os.path.join(output_path, '09_boxplots_por_conversion.png'))

        
        # GRÁFICA 10 — Violin plots: variables clave vs objetivo
        top_vars = ['days_active_trial', 'sessions_count', 'plan_page_views',
                    'features_used', 'satisfaction_score', 'last_activity_gap_days']

        vio_df = df[[target]].copy()
        for var in top_vars:
            vio_df[var] = self.__to_num(df[var])

        fig, axes = plt.subplots(2, 3, figsize=(20, 10))
        axes = axes.flatten()
        fig.suptitle('Violin Plots: Variables Clave por Conversión (datos crudos)',
                     fontsize=14, fontweight='bold')

        for idx, var in enumerate(top_vars):
            ax = axes[idx]
            sns.violinplot(data=vio_df, x=target, y=var, ax=ax,
                           palette=['#E74C3C', '#2ECC71'],
                           inner='quartile', cut=0)
            ax.set_title(var, fontsize=11, fontweight='bold')
            ax.set_xlabel('Convertido')
            ax.set_ylabel('')
            ax.set_xticks([0, 1])
            ax.set_xticklabels(['No (0)', 'Sí (1)'])

        plt.tight_layout(rect=(0, 0, 1, 0.95))
        self.__save(fig, os.path.join(output_path, '10_violin_plots.png'))

        
        # GRÁFICA 11 — Detección de outliers con IQR
        ncols = 3
        nrows = -(-len(outlier_vars) // ncols)
        fig, axes = plt.subplots(nrows, ncols, figsize=(18, nrows * 5))
        axes = axes.flatten()
        fig.suptitle('Detección de Outliers – Método IQR (datos crudos)',
                     fontsize=14, fontweight='bold')

        for idx, var in enumerate(outlier_vars):
            ax = axes[idx]
            data = self.__to_num(df[var]).dropna()
            Q1, Q3 = data.quantile(0.25), data.quantile(0.75)
            IQR = Q3 - Q1
            lower, upper = Q1 - 1.5 * IQR, Q3 + 1.5 * IQR
            n_out = ((data < lower) | (data > upper)).sum()

            ax.hist(data, bins=30, color='steelblue', alpha=0.7, edgecolor='white')
            ax.axvline(lower, color='red', linestyle='--', linewidth=1.5,
                       label=f'Límite inf: {lower:.1f}')
            ax.axvline(upper, color='red', linestyle='--', linewidth=1.5,
                       label=f'Límite sup: {upper:.1f}')
            ax.set_title(f'{var}  |  Outliers detectados: {n_out}',
                         fontsize=10, fontweight='bold')
            ax.legend(fontsize=7)

        for j in range(len(outlier_vars), len(axes)):
            axes[j].set_visible(False)

        plt.tight_layout(rect=(0, 0, 1, 0.95))
        self.__save(fig, os.path.join(output_path, '11_deteccion_outliers.png'))

        
        # GRÁFICA 12 — Matriz de correlación
        # (usa solo columnas ya numéricas en el CSV; sin transformar)
        
        corr_df = df[numeric_raw].copy()
        corr_matrix = corr_df.corr()

        fig, ax = plt.subplots(figsize=(16, 13))
        mask = np.triu(np.ones_like(corr_matrix, dtype=bool))
        sns.heatmap(corr_matrix, mask=mask, annot=True, fmt='.2f',
                    cmap='coolwarm', center=0, linewidths=0.5,
                    annot_kws={'size': 7}, ax=ax)
        ax.set_title('Matriz de Correlación – columnas numéricas del CSV crudo\n'
                     '(monthly_income_usd y discount_offered_pct excluidas por tener $/%)',
                     fontsize=12, fontweight='bold')
        plt.xticks(rotation=45, ha='right', fontsize=8)
        plt.yticks(fontsize=8)
        plt.tight_layout()
        self.__save(fig, os.path.join(output_path, '12_matriz_correlacion.png'))