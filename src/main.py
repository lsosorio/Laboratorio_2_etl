from traceback import print_last

from src.clases.DataAnalysis import DataAnalysis

#import pandas as pd
#import numpy as np
#import matplotlib.pyplot as plt
#import seaborn as sns
#
#from sklearn.model_selection import train_test_split
#from sklearn.linear_model import LogisticRegression
#from sklearn.preprocessing import StandardScaler, LabelEncoder
#from sklearn.metrics import (accuracy_score, precision_score, recall_score,
#                             f1_score, confusion_matrix, classification_report,
#                             roc_auc_score, roc_curve)
#
#
## Tarea ETL
#
## --- 1.1 Carga de datos ---
#df_raw = pd.read_csv('./datasets/lab2_trial_conversion_users.csv')
#df_dict = pd.read_csv('./datasets/lab2_data_dictionary.csv')
#
#print("=" * 70)
#print("Analisis del dataset")
#print("=" * 70)
#
#
#print(f"\nDimensiones del dataset: {df_raw.shape}")
#print(f"Total de registros: {df_raw.shape[0]}")
#print(f"Total de columnas: {df_raw.shape[1]}")
#
## --- 1.2 Estructura y tipos de datos ---
#print("\n--- Tipos de datos originales ---")
#print(df_raw.dtypes.to_string())
#
#print("\n--- Primeras 5 filas ---")
#print(df_raw.head().to_string())
#
#print("\n--- Información general ---")
#print(df_raw.info())
#
## Analisis de los valores nulos
##null_counts = df_raw.isnull().sum()
##null_pct = (df_raw.isnull().sum() / len(df_raw) * 100).round(2)
##null_summary = pd.DataFrame({'Nulos': null_counts, 'Porcentaje(%)': null_pct})
##print(null_summary[null_summary['Nulos'] > 0])
#
#
#
### Convirtamos las fechas al mismo formato
#def parse_mixed_dates(series):
#    """Parsea fechas con formatos mixtos."""
#    parsed = pd.to_datetime(series, format='%Y-%m-%d', errors='coerce')
#
#    # Intentar formato DD/MM/YYYY
#    mask_null = parsed.isna()
#    if mask_null.any():
#        parsed_alt = pd.to_datetime(series[mask_null], format='%d/%m/%Y', errors='coerce')
#        parsed[mask_null] = parsed_alt
#
#    # Intentar formato MM-DD-YYYY
#    mask_null = parsed.isna()
#    if mask_null.any():
#        parsed_alt = pd.to_datetime(series[mask_null], format='%m-%d-%Y', errors='coerce')
#        parsed[mask_null] = parsed_alt
#
#    return parsed
#
### Funcion que compara los tipos de datos entre el dataframe original y el transformado, usando el diccionario de datos para referencia
#def compare_dataframes_dtypes(df_raw, df_clean, df_dic):
#    """
#    Compara los tipos de datos entre el DataFrame original y el transformado.
#    Incluye un ejemplo de valor de cada uno para mayor claridad.
#    """
#    records = []
#    dtype_mapping = {
#        'string': ['str'],
#        'string/date': ['datetime64[us]'],
#        'integer': ['int64', 'int32'],
#        'numeric': ['float64', 'int64'],
#        'categorical': ['object', 'category', "str"],
#        'binary': ['int64', 'int32', 'uint8'],
#        'numeric/string': ['float64', 'int64', 'int32'],
#    }
#    dict_types = df_dic.set_index('column_name')['data_type'].to_dict()
#
#    for col in df_raw.columns:
#        # Tomar un valor no nulo de ejemplo de cada dataframe
#        #ejemplo_raw = df_raw[col].dropna().iloc[0] if df_raw[col].notna().any() else 'N/A'
#        #ejemplo_clean = df_clean[col].dropna().iloc[0] if df_clean[col].notna().any() else 'N/A'
#
#        clean_dtype = str(df_clean[col].dtype) if col in df_clean.columns else 'NO EXISTE'
#        expected_type = dict_types.get(col, 'No definido')
#        acceptable_types = dtype_mapping.get(expected_type, [])
#        is_valid = clean_dtype in acceptable_types if acceptable_types else None
#
#        records.append({
#            'Columna': col,
#            'Dtype Dictionario': expected_type,
#            'Dtype Original': str(df_raw[col].dtype),
#            'Dtype Transformado': str(df_clean[col].dtype),
#            'Válido': '✅' if is_valid else '❌' if is_valid is False else '⚠️',
#            #'Ejemplo Original': ejemplo_raw,
#            #'Ejemplo Transformado': ejemplo_clean
#        })
#
#    comparison_df = pd.DataFrame(records)
#    return comparison_df
#
#
##hacemos una copia exacta del dataframe original
#df = df_raw.copy()
#
#print("=" * 140)
#print("Analisis del dataset Inicial comparando el dataframe original con el transformado (inicialmente son iguales)")
#comparison = compare_dataframes_dtypes(df_raw, df, df_dict)
#
#print(comparison.to_string())
#
#print("=" * 140)
#df['signup_date'] = parse_mixed_dates(df['signup_date'])
#df['trial_end_date'] = parse_mixed_dates(df['trial_end_date'])
#
#print("Resultado primera transformacion de los campos de fecha")
#comparison = compare_dataframes_dtypes(df_raw, df, df_dict)
#
#print(comparison.to_string())
#
#print("=" * 140)
#df['discount_offered_pct'] = df['discount_offered_pct'].astype(str).str.replace('%', '', regex=False)
#df['discount_offered_pct'] = pd.to_numeric(df['discount_offered_pct'], errors='coerce')
#print(f"discount_offered_pct - nulos: {df['discount_offered_pct'].isna().sum()}")
#
#df['monthly_income_usd'] = df['monthly_income_usd'].astype(str).str.replace('$', '', regex=False)
#df['monthly_income_usd'] = df['monthly_income_usd'].replace('<NA>', np.nan)
#df['monthly_income_usd'] = df['monthly_income_usd'].replace('nan', np.nan)
#df['monthly_income_usd'] = pd.to_numeric(df['monthly_income_usd'], errors='coerce')
#print(f"monthly_income_usd - nulos: {df['monthly_income_usd'].isna().sum()}")
#
#print("Resultados de la transformacion de los campos numericos ($)")
#comparison = compare_dataframes_dtypes(df_raw, df, df_dict)
#
#print(comparison.to_string())
#
#print()
#print("=" * 140)
#print("Valores nulos por columna")
#print(f"Total Registros: {len(df_raw)}")
#null_counts = df_raw.isnull().sum()
#null_pct = (df_raw.isnull().sum() / len(df_raw) * 100).round(2)
#null_summary = pd.DataFrame({'Nulos': null_counts, 'Porcentaje(%)': null_pct})
#print(null_summary[null_summary['Nulos'] > 0])
#
#print()
#print("=" * 140)
#print("Eliminacion valores duplicados")
#n_before = len(df)
#
## Eliminar duplicados exactos
#df = df.drop_duplicates()
#print(f"Duplicados exactos eliminados: {n_before - len(df)}")
#
## Para user_id duplicados, conservar el último registro
#n_before = len(df)
#df = df.drop_duplicates(subset='user_id', keep='last')
#print(f"Duplicados por user_id eliminados (se conserva último): {n_before - len(df)}")
#print(f"Registros después de deduplicación: {len(df)}")
#
#print()
#print("=" * 140)
#print("Valores nulos por columna despues de eliminar los duplicados")
#print(f"Total Registros: {len(df)}")
#null_counts = df.isnull().sum()
#null_pct = (df.isnull().sum() / len(df) * 100).round(2)
#null_summary = pd.DataFrame({'Nulos': null_counts, 'Porcentaje(%)': null_pct})
#print(null_summary[null_summary['Nulos'] > 0])
#
#print()
#print("=" * 140)
#print("Imputacion de valores nulos")
## Justificación: Se imputan con mediana para numéricas y moda para categóricas
## para no perder registros. Se documenta cada decisión.
#num_cols_to_impute = ['age', 'avg_session_minutes', 'satisfaction_score', 'monthly_income_usd'] # ,
#for col in num_cols_to_impute:
#    n_null = df[col].isna().sum()
#    if n_null > 0:
#        median_val = df[col].median()
#        df[col] = df[col].fillna(median_val)
#        print(f"  {col}: {n_null} nulos imputados con mediana ({median_val:.2f})")
#
#cat_cols_to_impute = ['country', 'device_type']
#for col in cat_cols_to_impute:
#    n_null = df[col].isna().sum()
#    if n_null > 0:
#        mode_val = df[col].mode()[0]
#        df[col] = df[col].fillna(mode_val)
#        print(f"  {col}: {n_null} nulos imputados con moda ('{mode_val}')")
#
#print("Valores nulos por columna y la imputacion de valores")
#print(f"Total Registros: {len(df)}")
#null_counts = df.isnull().sum()
#null_pct = (df.isnull().sum() / len(df) * 100).round(2)
#null_summary = pd.DataFrame({'Nulos': null_counts, 'Porcentaje(%)': null_pct})
#print(null_summary[null_summary['Nulos'] > 0])
#
#
#print()
#print("=" * 140)
#print("Eliminacion de valores nulos")
#n_before = len(df)
#df_temp = df.dropna()
#print(f"Registros eliminados por contener nulos: {n_before - len(df_temp)}")
#print(f"Registros después de eliminar nulos: {len(df_temp)}")
#
## Tratamiento de out outliers
#print()
#print("=" * 140)
#print("Tratamiento de outliers ---")
#def cap_outliers_iqr(series, factor=1.5):
#    """Capea outliers usando el método IQR."""
#    Q1 = series.quantile(0.25)
#    Q3 = series.quantile(0.75)
#    IQR = Q3 - Q1
#    lower = Q1 - factor * IQR
#    upper = Q3 + factor * IQR
#    n_lower = (series < lower).sum()
#    n_upper = (series > upper).sum()
#    capped = series.clip(lower=lower, upper=upper)
#
#    return capped, n_lower, n_upper, lower, upper
#
#
#outlier_cols = ['sessions_count', 'avg_session_minutes', 'features_used',
#                'support_tickets', 'monthly_income_usd']
#
#for col in outlier_cols:
#    df[col], n_low, n_up, lb, ub = cap_outliers_iqr(df[col])
#    if n_low + n_up > 0:
#        print(f"  {col}: {n_low} outliers inferiores, {n_up} outliers superiores "
#              f"(rango: [{lb:.1f}, {ub:.1f}])")
#
#
#print()
#print("=" * 140)
#print("Construccion de variables derivadas")
#
## --- Ratio de sesiones por día activo ---
## Interpretación: Intensidad de uso diaria
#df['sessions_per_active_day'] = np.where(
#    df['days_active_trial'] > 0,
#    df['sessions_count'] / df['days_active_trial'],
#    0
#)
#print("1. sessions_per_active_day: Ratio de sesiones por día activo")
#
## --- Ratio de días activos sobre duración del trial ---
## Interpretación: Qué tan consistente fue el uso durante el trial
#df['activity_rate'] = df['days_active_trial'] / df['trial_length_days']
#print("2. activity_rate: Proporción de días activos sobre total del trial")
#
## --- Engagement total (sesiones * duración promedio) ---
#df['total_engagement_minutes'] = df['sessions_count'] * df['avg_session_minutes']
#print("3. total_engagement_minutes: Tiempo total estimado de engagement")
#
#df['features_per_session'] = np.where(
#    df['sessions_count'] > 0,
#    df['features_used'] / df['sessions_count'],
#    0
#)
#print("4. features_per_session: Funcionalidades usadas por sesión")
#
## Interpretación: Si el usuario estuvo activo cerca del cierre, mayor intención
#df['recency_score'] = np.where(
#    df['last_activity_gap_days'] > 0,
#    1 / (1 + df['last_activity_gap_days']),
#    1
#)
#print("5. recency_score: Cercanía de última actividad al fin del trial")
#
#
#print("--- Estadísticas de variables derivadas ---")
#new_features = ['sessions_per_active_day', 'activity_rate', 'total_engagement_minutes','features_per_session', 'recency_score']
#print(df[new_features].describe().to_string())
#
#print()
#print("=" * 140)
#print("--- Preparacion del modelo ---")
#exclude_cols = ['user_id', 'signup_date', 'trial_end_date', 'selected_plan', 'converted_to_paid_plan']
#
## Variables numéricas
#numeric_features = df.select_dtypes(include=[np.number]).columns.tolist()
#numeric_features = [c for c in numeric_features if c not in exclude_cols]
#
#print(numeric_features)
#
## Variables categóricas a codificar
#categorical_features = ['country', 'gender', 'device_type', 'acquisition_channel',
#                        'city_tier', 'preferred_plan_before_conversion']
#
#print("Variables numéricas seleccionadas:")
#print(numeric_features)
#print(f"\nVariables categóricas a codificar:")
#print(categorical_features)
##
#df_model = df.copy()
##
#df_model = pd.get_dummies(df_model, columns=categorical_features, drop_first=True, dtype=int)
#print(df_model.head().to_string())
#
#y = df_model['converted_to_paid_plan']
## X = df_model.drop(columns=exclude_cols, errors='ignore')
#X = df_model.filter(items=['days_active_trial', 'sessions_count', 'avg_session_minutes', 'features_used', 'payment_method_on_file', 'discount_offered_pct', 'plan_page_views', 'last_activity_gap_days'])
#
#
#print(f"\nDimensiones de X: {X.shape}")
#print(f"Dimensiones de y: {y.shape}")
#print(f"\nDistribución de y:")
#print(y.value_counts())
#print(f"Tasa de conversión: {y.mean() * 100:.2f}%")
#
#print(f"\nNulos en X: {X.isnull().sum().sum()}")
#
## Separación 60% train, 20% test, 20% validation con estratificación
#X_train, X_temp, y_train, y_temp = train_test_split(
#    X, y, test_size=0.40, random_state=42, stratify=y
#)
#
#X_test, X_val, y_test, y_val = train_test_split(
#    X_temp, y_temp, test_size=0.50, random_state=42, stratify=y_temp
#)
#
#print(f"Conjunto de entrenamiento: {X_train.shape[0]} registros ({X_train.shape[0] / len(X) * 100:.1f}%)")
#print(f"Conjunto de prueba:        {X_test.shape[0]} registros ({X_test.shape[0] / len(X) * 100:.1f}%)")
#print(f"Conjunto de validación:    {X_val.shape[0]} registros ({X_val.shape[0] / len(X) * 100:.1f}%)")
#
#print(f"\nDistribución de la variable objetivo por conjunto:")
#print(f"  Train:      {y_train.mean() * 100:.2f}% conversión")
#print(f"  Test:       {y_test.mean() * 100:.2f}% conversión")
#print(f"  Validation: {y_val.mean() * 100:.2f}% conversión")
#
#scaler = StandardScaler()
#X_train_scaled = pd.DataFrame(
#    scaler.fit_transform(X_train), columns=X_train.columns, index=X_train.index
#)
#X_test_scaled = pd.DataFrame(
#    scaler.transform(X_test), columns=X_test.columns, index=X_test.index
#)
#X_val_scaled = pd.DataFrame(
#    scaler.transform(X_val), columns=X_val.columns, index=X_val.index
#)
#print("\nEscalado (StandardScaler) aplicado - fit en train, transform en test y validation")
#
#
#model = LogisticRegression(
#    max_iter=1000,
#    random_state=42,
#    class_weight='balanced',  # Manejar desbalance de clases
#    solver='lbfgs'
#)
#
#model.fit(X_train_scaled, y_train)
#print("Modelo entrenado exitosamente")
#print(f"  Solver: lbfgs")
#print(f"  class_weight: balanced (para manejar desbalance)")
#print(f"  max_iter: 1000")
#
## --- 6.1 Interpretación de coeficientes ---
#print("\n--- Top 15 variables más importantes (por magnitud del coeficiente) ---")
#coef_df = pd.DataFrame({
#    'Variable': X_train.columns,
#    'Coeficiente': model.coef_[0]
#}).sort_values('Coeficiente', ascending=False)
#
#print("\nTop 10 positivos (favorecen conversión):")
#print(coef_df.head(10).to_string(index=False))
#
#print("\nTop 10 negativos (reducen conversión):")
#print(coef_df.tail(10).to_string(index=False))
#
#print("\n" + "=" * 140)
#print("EVALUACIÓN DEL MODELO")
#print("=" * 140)
#
#def evaluate_model(model, X, y, set_name):
#    """Evalúa el modelo en un conjunto de datos y reporta métricas."""
#    y_pred = model.predict(X)
#    y_prob = model.predict_proba(X)[:, 1]
#
#    acc = accuracy_score(y, y_pred)
#    prec = precision_score(y, y_pred)
#    rec = recall_score(y, y_pred)
#    f1 = f1_score(y, y_pred)
#    auc = roc_auc_score(y, y_prob)
#
#    print(f"\n{'=' * 50}")
#    print(f"  Métricas en {set_name}")
#    print(f"{'=' * 50}")
#    print(f"  Accuracy:  {acc:.4f}")
#    print(f"  Precision: {prec:.4f}")
#    print(f"  Recall:    {rec:.4f}")
#    print(f"  F1-Score:  {f1:.4f}")
#    print(f"  ROC-AUC:   {auc:.4f}")
#
#    print(f"\n  Matriz de Confusión:")
#    cm = confusion_matrix(y, y_pred)
#    print(f"  {'':>20} Predicho=0  Predicho=1")
#    print(f"  {'Real=0':>20}  {cm[0][0]:>8}  {cm[0][1]:>8}")
#    print(f"  {'Real=1':>20}  {cm[1][0]:>8}  {cm[1][1]:>8}")
#
#    print(f"\n  Reporte de Clasificación:")
#    print(classification_report(y, y_pred, target_names=['No Convertido', 'Convertido']))
#
#    return {'accuracy': acc, 'precision': prec, 'recall': rec, 'f1': f1, 'roc_auc': auc}
#
#
## Evaluar en los tres conjuntos
#metrics_train = evaluate_model(model, X_train_scaled, y_train, "ENTRENAMIENTO (60%)")
#metrics_test = evaluate_model(model, X_test_scaled, y_test, "PRUEBA (20%)")
#metrics_val = evaluate_model(model, X_val_scaled, y_val, "VALIDACIÓN (20%)")
#
#print("\n" + "=" * 70)
#print("COMPARACIÓN DE MÉTRICAS ENTRE CONJUNTOS")
#print("=" * 70)
#
#comparison = pd.DataFrame({
#    'Entrenamiento': metrics_train,
#    'Prueba': metrics_test,
#    'Validación': metrics_val
#}).round(4)
#print(comparison)
#
## --- 7.2 Curva ROC ---
#print("\n--- Generando curva ROC ---")
#
#fig, axes = plt.subplots(1, 3, figsize=(18, 5))
#
#for ax, (X_set, y_set, name) in zip(axes, [
#    (X_train_scaled, y_train, 'Entrenamiento'),
#    (X_test_scaled, y_test, 'Prueba'),
#    (X_val_scaled, y_val, 'Validación')
#]):
#    y_prob = model.predict_proba(X_set)[:, 1]
#    fpr, tpr, _ = roc_curve(y_set, y_prob)
#    auc_val = roc_auc_score(y_set, y_prob)
#
#    ax.plot(fpr, tpr, label=f'ROC (AUC = {auc_val:.3f})', linewidth=2)
#    ax.plot([0, 1], [0, 1], 'k--', alpha=0.5)
#    ax.set_xlabel('Tasa de Falsos Positivos')
#    ax.set_ylabel('Tasa de Verdaderos Positivos')
#    ax.set_title(f'Curva ROC - {name}')
#    ax.legend(loc='lower right')
#
#plt.tight_layout()
#plt.savefig('roc_curves.png', dpi=150, bbox_inches='tight')
#plt.show()
#print("Curva ROC guardada como 'roc_curves.png'")
#
#ig, ax = plt.subplots(figsize=(10, 8))
#top_n = 20
#top_coefs = coef_df.head(top_n )
#bottom_coefs = coef_df.tail(top_n)
#plot_coefs = pd.concat([top_coefs, bottom_coefs])
#
#colors = ['#2ecc71' if c > 0 else '#e74c3c' for c in plot_coefs['Coeficiente']]
#ax.barh(range(len(plot_coefs)), plot_coefs['Coeficiente'], color=colors)
#ax.set_yticks(range(len(plot_coefs)))
#ax.set_yticklabels(plot_coefs['Variable'], fontsize=9)
#ax.set_xlabel('Coeficiente')
#ax.set_title('Top Variables por Coeficiente en Regresión Logística')
#ax.axvline(x=0, color='black', linewidth=0.5)
#
#plt.tight_layout()
#plt.savefig('feature_importance.png', dpi=150, bbox_inches='tight')
#plt.show()
#print("Gráfico de importancia de variables guardado como 'feature_importance.png'")
#
#
#
#print("\n--- Boxplot: recency_score ---")
#
#plot_df = df[['recency_score', 'converted_to_paid_plan']].dropna().copy()
#
#plt.figure(figsize=(8, 6))
#sns.boxplot(
#    data=plot_df,
#    x = 'converted_to_paid_plan',
#    y='recency_score',
#    color='steelblue',
#    width=0.4
#)
#
#plt.title('Boxplot de last_activity_gap_days')
#plt.ylabel('last_activity_gap_days')
#plt.xlabel('')
#plt.tight_layout()
#plt.savefig('boxplot_last_activity_gap_days.png', dpi=150, bbox_inches='tight')
#plt.show()
#
#print("Gráfico guardado como 'boxplot_last_activity_gap_days.png'")




#print()
#print("=" * 140)
#continuous_vars = [
#    'age', 'avg_session_minutes', 'satisfaction_score', 'monthly_income_usd'
#]
#
## Variables numéricas discretas (conteos / enteros)
#discrete_vars = [
#    'trial_length_days', 'days_active_trial', 'sessions_count',
#    'features_used', 'support_tickets', 'emails_opened',
#    'plan_page_views', 'last_activity_gap_days', 'discount_offered_pct'
#]
#
## Variables binarias
#binary_vars = [
#    'webinar_attended', 'payment_method_on_file',
#    'referred_friend', 'converted_to_paid_plan'
#]
#
#fig, axes = plt.subplots(2, 2, figsize=(14, 10))
#fig.suptitle('Distribución de Variables Numéricas Continuas', fontsize=16, fontweight='bold')
#
#for idx, var in enumerate(continuous_vars):
#    ax = axes[idx // 2, idx % 2]
#    data = df[var].dropna()
#
#    # Histograma con KDE
#    ax.hist(data, bins=30, color='steelblue', alpha=0.7, edgecolor='white', density=True, label='Histograma')
#    data.plot.kde(ax=ax, color='darkred', linewidth=2, label='KDE')
#
#    # Líneas de referencia: media y mediana
#    mean_val = data.mean()
#    median_val = data.median()
#    ax.axvline(mean_val, color='orange', linestyle='--', linewidth=1.5, label=f'Media: {mean_val:.1f}')
#    ax.axvline(median_val, color='green', linestyle='-.', linewidth=1.5, label=f'Mediana: {median_val:.1f}')
#
#    ax.set_title(var, fontsize=13, fontweight='bold')
#    ax.set_xlabel('')
#    ax.legend(fontsize=8)
#
#plt.tight_layout(rect=[0, 0, 1, 0.95])
#plt.savefig('dist_continuas.png', dpi=150, bbox_inches='tight')
#plt.show()
#
## =============================================================================
## 4. GRÁFICO 2: HISTOGRAMAS DE VARIABLES DISCRETAS
## =============================================================================
#
#fig, axes = plt.subplots(3, 3, figsize=(18, 14))
#fig.suptitle('Distribución de Variables Numéricas Discretas', fontsize=16, fontweight='bold')
#
#for idx, var in enumerate(discrete_vars):
#    ax = axes[idx // 3, idx % 3]
#    data = df[var].dropna()
#
#    ax.hist(data, bins=min(30, int(data.nunique())), color='teal', alpha=0.7, edgecolor='white')
#
#    # Estadísticas en el gráfico
#    stats_text = f'n={len(data)}\nμ={data.mean():.1f}\nσ={data.std():.1f}\nMd={data.median():.0f}'
#    ax.text(0.95, 0.95, stats_text, transform=ax.transAxes,
#            fontsize=8, verticalalignment='top', horizontalalignment='right',
#            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
#
#    ax.set_title(var, fontsize=12, fontweight='bold')
#    ax.set_xlabel('')
#
#plt.tight_layout(rect=[0, 0, 1, 0.95])
#plt.savefig('dist_discretas.png', dpi=150, bbox_inches='tight')
#plt.show()
#
## =============================================================================
## 9. GRÁFICO 7: DETECCIÓN DE OUTLIERS CON IQR
## =============================================================================
#
#fig, axes = plt.subplots(2, 3, figsize=(18, 10))
#fig.suptitle('Detección de Outliers (IQR) en Variables Clave', fontsize=16, fontweight='bold')
#
#outlier_vars = ['sessions_count', 'avg_session_minutes', 'features_used',
#                'monthly_income_usd', 'support_tickets', 'age']
#
#for idx, var in enumerate(outlier_vars):
#    ax = axes[idx // 3, idx % 3]
#    data = df[var].dropna()
#
#    Q1 = data.quantile(0.25)
#    Q3 = data.quantile(0.75)
#    IQR = Q3 - Q1
#    lower = Q1 - 1.5 * IQR
#    upper = Q3 + 1.5 * IQR
#    n_outliers = ((data < lower) | (data > upper)).sum()
#
#    # Histograma
#    ax.hist(data, bins=30, color='steelblue', alpha=0.7, edgecolor='white')
#    ax.axvline(lower, color='red', linestyle='--', linewidth=1.5, label=f'Límite inf: {lower:.1f}')
#    ax.axvline(upper, color='red', linestyle='--', linewidth=1.5, label=f'Límite sup: {upper:.1f}')
#
#    ax.set_title(f'{var}\n(Outliers: {n_outliers})', fontsize=11, fontweight='bold')
#    ax.legend(fontsize=7)
#
#plt.tight_layout(rect=[0, 0, 1, 0.95])
#plt.savefig('deteccion_outliers.png', dpi=150, bbox_inches='tight')
#plt.show()
#
## =============================================================================
## 5. GRÁFICO 3: BOXPLOTS COMPARATIVOS (por variable objetivo)
## =============================================================================
#
#key_numeric_vars = [
#    'age', 'days_active_trial', 'sessions_count', 'avg_session_minutes',
#    'features_used', 'emails_opened', 'plan_page_views',
#    'satisfaction_score', 'monthly_income_usd', 'discount_offered_pct'
#]
#
#fig, axes = plt.subplots(2, 5, figsize=(22, 10))
#fig.suptitle('Distribución por Conversión (converted_to_paid_plan)', fontsize=16, fontweight='bold')
#
#for idx, var in enumerate(key_numeric_vars):
#    ax = axes[idx // 5, idx % 5]
#    sns.boxplot(
#        data=df, x='converted_to_paid_plan', y=var, ax=ax,
#        hue='converted_to_paid_plan',
#        palette={0: '#E74C3C', 1: '#2ECC71'}, width=0.5, legend=False
#    )
#    ax.set_title(var, fontsize=10, fontweight='bold')
#    ax.set_xlabel('Convertido')
#    ax.set_ylabel('')
#
#plt.tight_layout(rect=[0, 0, 1, 0.95])
#plt.savefig('boxplots_por_conversion.png', dpi=150, bbox_inches='tight')
#plt.show()
#
## =============================================================================
## 6. GRÁFICO 4: VIOLIN PLOTS PARA VARIABLES CLAVE
## =============================================================================
#
#top_vars = ['days_active_trial', 'sessions_count', 'plan_page_views', 'features_used']
#
#fig, axes = plt.subplots(1, 4, figsize=(20, 6))
#fig.suptitle('Violin Plots: Variables Clave por Conversión', fontsize=16, fontweight='bold')
#
#for idx, var in enumerate(top_vars):
#    ax = axes[idx]
#    sns.violinplot(
#        data=df, x='converted_to_paid_plan', y=var, ax=ax,
#        palette={0: '#E74C3C', 1: '#2ECC71'}, inner='quartile', cut=0
#    )
#    ax.set_title(var, fontsize=12, fontweight='bold')
#    ax.set_xlabel('Convertido')
#    ax.set_ylabel('')
#
#plt.tight_layout(rect=[0, 0, 1, 0.93])
#plt.savefig('violin_plots.png', dpi=150, bbox_inches='tight')
#plt.show()
#





# =====================================================================
# Laboratorio No. 2 - ETL y Regresión Logística
# Curso: ETL (Extract, Transform, Load)
# Institución: Universidad Autónoma
# Estudiante: Luis Santiago Osorio Ortiz
# =====================================================================

#import pandas as pd
#import numpy as np
#import statsmodels.api as sm
#from sklearn.model_selection import train_test_split
#from sklearn.linear_model import LogisticRegression
#from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix, roc_auc_score
#from sklearn.preprocessing import StandardScaler
#import matplotlib.pyplot as plt
#
#
## =====================================================================
## 1. EXTRACCIÓN (Extract)
## =====================================================================
#print("Iniciando pipeline ETL...")
## Carga del dataset original
#df = pd.read_csv('../datasets/raw/lab2_trial_conversion_users.csv')
#print(f"Registros iniciales cargados: {len(df)}")
#
## =====================================================================
## 2. TRANSFORMACIÓN: LIMPIEZA DE DATOS (Calidad del ETL)
## =====================================================================
## A. Tratamiento de Duplicados (Eliminar los 45 registros trampa)
#df = df.drop_duplicates(subset=['user_id'], keep='last')
#
## B. Corrección de Formatos
#df['device_type'] = df['device_type'].str.lower().str.strip()
#df['discount_offered_pct'] = df['discount_offered_pct'].astype(str).str.replace('%', '', regex=False).astype(float)
#df['monthly_income_usd'] = (df['monthly_income_usd']
#                            .astype(str)
#                            .str.replace('$', '', regex=False)
#                            .str.replace('<NA>', 'nan', regex=False)
#                            .astype(float))
#
## C. Imputación de Valores Nulos (Mediana para numéricas, 'desconocido' para categóricas)
#cols_num_nulas = ['age', 'avg_session_minutes', 'satisfaction_score', 'monthly_income_usd']
#for col in cols_num_nulas:
#    df[col] = df[col].fillna(df[col].median())
#
#df['device_type'] = df['device_type'].fillna('desconocido')
#df['country'] = df['country'].fillna('desconocido')
#
## D. Gestión de Outliers (Método de Rango Intercuartílico - IQR)
#columnas_con_outliers = ['avg_session_minutes', 'monthly_income_usd', 'sessions_count']
#for col in columnas_con_outliers:
#    Q1 = df[col].quantile(0.25)
#    Q3 = df[col].quantile(0.75)
#    IQR = Q3 - Q1
#
#    limite_inferior = Q1 - 1.5 * IQR
#    limite_superior = Q3 + 1.5 * IQR
#
#    df[col] = np.where(df[col] > limite_superior, limite_superior, df[col])
#    df[col] = np.where(df[col] < limite_inferior, limite_inferior, df[col])
#
## =====================================================================
## 3. TRANSFORMACIÓN: FEATURE ENGINEERING
## =====================================================================
#df['sessions_per_active_day'] = np.where(df['days_active_trial'] > 0,
#                                         df['sessions_count'] / df['days_active_trial'], 0)
#df['total_minutes_used'] = df['sessions_count'] * df['avg_session_minutes']
#df['activity_intensity'] = df['features_used'] * df['days_active_trial']
#df['high_commercial_intent'] = ((df['plan_page_views'] > 1) & (df['payment_method_on_file'] == 1)).astype(int)
#
## =====================================================================
## 4. PREPARACIÓN PARA EL MODELADO
## =====================================================================
##features_to_use = [
##    'days_active_trial', 'sessions_count', 'avg_session_minutes',
##    'features_used', 'payment_method_on_file', 'discount_offered_pct',
##    'plan_page_views', 'last_activity_gap_days', 'age', 'satisfaction_score',
##    'monthly_income_usd', 'sessions_per_active_day', 'total_minutes_used',
##    'activity_intensity', 'high_commercial_intent'
##]
#features_to_use = [
#    'days_active_trial', 'sessions_count', 'avg_session_minutes',
#   'features_used', 'payment_method_on_file', 'discount_offered_pct',
#   'plan_page_views', 'last_activity_gap_days'#, 'sessions_per_active_day', 'total_minutes_used',
#   #'activity_intensity', 'high_commercial_intent'
# #'days_active_trial' , 'payment_method_on_file' , 'discount_offered_pct', 'last_activity_gap_days'
##'trial_length_days', 'age', 'country', 'gender', 'device_type',
##       'acquisition_channel', 'city_tier', 'preferred_plan_before_conversion',
##       'days_active_trial', 'sessions_count', 'avg_session_minutes',
##       'features_used', 'support_tickets', 'emails_opened', 'webinar_attended',
##       'payment_method_on_file', 'referred_friend', 'discount_offered_pct',
##       'plan_page_views', 'last_activity_gap_days', 'satisfaction_score',
##       'monthly_income_usd', 'sessions_per_active_day', 'total_minutes_used',
##       'activity_intensity', 'high_commercial_intent'
#]
##excludeColumns = ['user_id', 'signup_date', 'trial_end_date', 'selected_plan', 'converted_to_paid_plan', 'country', 'gender', 'device_type', 'acquisition_channel','city_tier', 'preferred_plan_before_conversion']
#target = 'converted_to_paid_plan'
#
#X =  df[features_to_use]
#y = df[target]
#
## División de datos (60/20/20)
#X_temp, X_test, y_temp, y_test = train_test_split(X, y, test_size=0.20, random_state=42, stratify=y)
#X_train, X_val, y_train, y_val = train_test_split(X_temp, y_temp, test_size=0.25, random_state=42, stratify=y_temp)
#
## Escalado manteniendo el formato de DataFrame de Pandas para statsmodels
#scaler = StandardScaler()
#X_train_scaled = pd.DataFrame(scaler.fit_transform(X_train), columns=X_train.columns, index=X_train.index)
#X_val_scaled = pd.DataFrame(scaler.transform(X_val), columns=X_val.columns, index=X_val.index)
#X_test_scaled = pd.DataFrame(scaler.transform(X_test), columns=X_test.columns, index=X_test.index)
#
#print(scaler.fit_transform(X_train))
#
## =====================================================================
## 5A. INFERENCIA ESTADÍSTICA (Statsmodels)
## =====================================================================
#print("=== RESUMEN ESTADÍSTICO DE VARIABLES (STATSMODELS) ===")
## Statsmodels exige agregar explícitamente la constante
#X_train_sm = sm.add_constant(X_train_scaled)
## Ajustar el modelo estadístico
#modelo_sm = sm.Logit(y_train, X_train_sm)
#resultados_sm = modelo_sm.fit(disp=0)  # disp=0 oculta el texto de las iteraciones
#print(resultados_sm.summary())
#print("\n" + "=" * 70 + "\n")
#
## =====================================================================
## 5B. MODELO PREDICTIVO Y BALANCEADO (Scikit-Learn)
## =====================================================================
## Regresión logística con class_weight='balanced' para mejorar el Recall
#model_sklearn = LogisticRegression(random_state=42, max_iter=1000, )
#model_sklearn.fit(X_train_scaled, y_train)
#
## =====================================================================
## 6. EVALUACIÓN Y MÉTRICAS FINALES
## =====================================================================
#y_test_pred = model_sklearn.predict(X_test_scaled)
#y_test_prob = model_sklearn.predict_proba(X_test_scaled)[:, 1]
#
#print("=== MÉTRICAS PREDICTIVAS EN EL CONJUNTO DE PRUEBA (20%) ===")
#print(f"Accuracy (Exactitud):  {accuracy_score(y_test, y_test_pred):.4f}")
#print(f"Precision (Precisión): {precision_score(y_test, y_test_pred):.4f}")
#print(f"Recall (Sensibilidad): {recall_score(y_test, y_test_pred):.4f}")
#print(f"F1-Score:              {f1_score(y_test, y_test_pred):.4f}")
#print(f"ROC-AUC:               {roc_auc_score(y_test, y_test_prob):.4f}")
#
#print("\n=== MATRIZ DE CONFUSIÓN ===")
#cm = confusion_matrix(y_test, y_test_pred)
#print(f"Verdaderos Negativos (No pagan, modelo acierta): {cm[0][0]}")
#print(f"Falsos Positivos     (No pagan, modelo falla):   {cm[0][1]}")
#print(f"Falsos Negativos     (Pagan, modelo falla):      {cm[1][0]}")
#print(f"Verdaderos Positivos (Pagan, modelo acierta):    {cm[1][1]}")



# path de los diferentes archivos
path_data_raw = "../datasets/raw/lab2_trial_conversion_users.csv"
path_data_clean = "../datasets/cleaned/lab2_trial_conversion_users_clean.csv"
path_data_dictionary = "../datasets/raw/lab2_data_dictionary.csv"

data_analysis = DataAnalysis(path_data_raw)

data_analysis.EDA()