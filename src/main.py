import pandas as pd
import statsmodels.api as sm
import numpy as np

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (accuracy_score, precision_score, recall_score,
                             f1_score, confusion_matrix, roc_auc_score)
from sklearn.preprocessing import StandardScaler
from src.clases.DataAnalysis import DataAnalysis
from src.clases.DataTransformer import DataTransformer


# ── Paths ────────
path_data_raw        = "../datasets/raw/lab2_trial_conversion_users.csv"
path_data_clean      = "../datasets/clean/lab2_trial_conversion_users_clean.csv"
path_data_dictionary = "../datasets/raw/lab2_data_dictionary.csv"

# ── ETL ──────────
data_analysis = DataAnalysis(path_data_raw)
data_transform = DataTransformer(path_data_raw, path_data_clean, path_data_dictionary)

data_analysis.EDA()
data_transform.transform()
data_transform.guardar_data_frame_clean()
df = data_transform.get_dataframe_clean()

# ── Preparación de features ────────────────────────────────────────────────────
exclude_cols = ['user_id', 'signup_date', 'trial_end_date', 'selected_plan', 'converted_to_paid_plan']
target = 'converted_to_paid_plan'

categorical_features = ['country', 'gender', 'device_type', 'acquisition_channel', 'city_tier', 'preferred_plan_before_conversion']

df = pd.get_dummies(df, columns=categorical_features, drop_first=True, dtype=int)

X = df.drop(columns=exclude_cols, errors='ignore')
y = df[target]

# ── División 60 / 20 / 20 ──────────────────────────────────────────────────────
X_temp,  X_test, y_temp,  y_test = train_test_split(X, y, test_size=0.20, random_state=42, stratify=y)
X_train, X_val,  y_train, y_val  = train_test_split(X_temp, y_temp, test_size=0.25, random_state=42, stratify=y_temp)

# ── Escalado ─────
scaler = StandardScaler()
X_train_scaled = pd.DataFrame(scaler.fit_transform(X_train), columns=X_train.columns, index=X_train.index)
X_val_scaled   = pd.DataFrame(scaler.transform(X_val),   columns=X_val.columns,   index=X_val.index)
X_test_scaled  = pd.DataFrame(scaler.transform(X_test),  columns=X_test.columns,  index=X_test.index)


# ── Helper de métricas ─────────────────────────────────────────────────────────
def evaluar_modelo(modelo, X, y, nombre_conjunto: str):
    y_pred = modelo.predict(X)
    y_prob = modelo.predict_proba(X)[:, 1]
    print(f"\n=== MÉTRICAS EN {nombre_conjunto} ===")
    print(f"Accuracy:  {accuracy_score(y, y_pred):.4f}")
    print(f"Precision: {precision_score(y, y_pred):.4f}")
    print(f"Recall:    {recall_score(y, y_pred):.4f}")
    print(f"F1-Score:  {f1_score(y, y_pred):.4f}")
    print(f"ROC-AUC:   {roc_auc_score(y, y_prob):.4f}")
    cm = confusion_matrix(y, y_pred)
    print(f"\nMatriz de Confusión:")
    print(f"Verdaderos Negativos (No pagan, modelo acierta): {cm[0][0]}")
    print(f"Falsos Positivos     (No pagan, modelo falla):   {cm[0][1]}")
    print(f"Falsos Negativos     (Pagan, modelo falla):      {cm[1][0]}")
    print(f"Verdaderos Positivos (Pagan, modelo acierta):    {cm[1][1]}")


# ── 5A. Inferencia estadística (Statsmodels) ───────────────────────────────────
print("=== RESUMEN ESTADÍSTICO (STATSMODELS) ===")
X_train_sm = sm.add_constant(X_train_scaled)
modelo_sm  = sm.Logit(y_train, X_train_sm)
resultados_sm = modelo_sm.fit(disp=0)
print(resultados_sm.summary())
print("\n" + "=" * 70 + "\n")

# ── 5B. Modelo predictivo (Scikit-Learn) ───────────────────────────────────────
print("\n" + "=" * 70 + "\n")
print("Regresion logistica no balanceada")
model_sklearn = LogisticRegression(random_state=42, max_iter=1000,)
model_sklearn.fit(X_train_scaled, y_train)

# ── 6. Evaluación 
evaluar_modelo(model_sklearn, X_val_scaled,  y_val,  "VALIDACIÓN (20%) - Modelo 1")
evaluar_modelo(model_sklearn, X_test_scaled, y_test, "PRUEBA (20%) - Modelo 1")


# ── 5B. Modelo predictivo (Scikit-Learn) ───────────────────────────────────────
print("\n" + "=" * 70 + "\n")
print("Regresion logistica balanceada")
model_sklearn = LogisticRegression(random_state=42, max_iter=1000, class_weight='balanced')
model_sklearn.fit(X_train_scaled, y_train)

# 6. Evaluación
evaluar_modelo(model_sklearn, X_val_scaled,  y_val,  "VALIDACIÓN (20%) - Modelo 2")
evaluar_modelo(model_sklearn, X_test_scaled, y_test, "PRUEBA (20%) - Modelo 2")


# Modelo 3:
print("\n" + "=" * 70 + "\n")
print("Regresion logistica — Features seleccionadas (balanceado)")

features_to_use = [
    'last_activity_gap_days',
    'satisfaction_score',
    'referred_friend',
    'monthly_income_usd',
    'discount_offered_pct',
    'payment_method_on_file',
    'age',
    'gender',
    'preferred_plan_before_conversion'
]

# Para las categóricas ya codificadas con get_dummies, buscamos todas
# las columnas que empiecen con el prefijo del feature original
cols_modelo3 = []
for feat in features_to_use:
    if feat in X_train_scaled.columns:
        cols_modelo3.append(feat)
    else:
        encoded = [c for c in X_train_scaled.columns if c.startswith(feat + '_')]
        cols_modelo3.extend(encoded)

print(f"Columnas seleccionadas ({len(cols_modelo3)}): {cols_modelo3}\n")

X_train_m3 = X_train_scaled[cols_modelo3]
X_val_m3   = X_val_scaled[cols_modelo3]
X_test_m3  = X_test_scaled[cols_modelo3]

model3 = LogisticRegression(random_state=42, max_iter=1000, class_weight='balanced')
model3.fit(X_train_m3, y_train)

evaluar_modelo(model3, X_val_m3,  y_val,  "VALIDACIÓN (20%) — Modelo 3")
evaluar_modelo(model3, X_test_m3, y_test, "PRUEBA (20%) — Modelo 3")

