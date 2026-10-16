import pandas as pd
import matplotlib.pyplot as plt

ruta = "/home/max/eda_local/"

# Figura 1: demora por aeropuerto
df_aeropuerto = pd.read_csv(
    ruta + "demora_por_aeropuerto.csv"
)

df_aeropuerto = df_aeropuerto.sort_values(
    "tasa_demora_porcentaje",
    ascending=True
)

plt.figure(figsize=(10, 6))
plt.barh(
    df_aeropuerto["Origin"],
    df_aeropuerto["tasa_demora_porcentaje"]
)
plt.xlabel("Tasa de vuelos demorados (%)")
plt.ylabel("Aeropuerto de origen")
plt.title("Tasa de demora por aeropuerto")
plt.tight_layout()
plt.savefig(
    "/home/max/figura_1_demora_aeropuerto.png",
    dpi=200
)
plt.close()

# Figura 2: demora por hora
df_hora = pd.read_csv(
    ruta + "demora_por_hora.csv"
)

df_hora = df_hora.sort_values("scheduled_hour")

plt.figure(figsize=(10, 6))
plt.plot(
    df_hora["scheduled_hour"],
    df_hora["tasa_demora_porcentaje"],
    marker="o"
)
plt.xlabel("Hora programada de salida")
plt.ylabel("Tasa de vuelos demorados (%)")
plt.title("Tasa de demora según hora programada")
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(
    "/home/max/figura_2_demora_hora.png",
    dpi=200
)
plt.close()

# Figura 3: demora por visibilidad
df_visibilidad = pd.read_csv(
    ruta + "demora_por_visibilidad.csv"
)

plt.figure(figsize=(10, 6))
plt.bar(
    df_visibilidad["visibility_group"],
    df_visibilidad["tasa_demora_porcentaje"]
)
plt.xlabel("Grupo de visibilidad")
plt.ylabel("Tasa de vuelos demorados (%)")
plt.title("Tasa de demora según visibilidad")
plt.xticks(rotation=25)
plt.tight_layout()
plt.savefig(
    "/home/max/figura_3_demora_visibilidad.png",
    dpi=200
)
plt.close()

print("FIGURAS GENERADAS CORRECTAMENTE")
print("/home/max/figura_1_demora_aeropuerto.png")
print("/home/max/figura_2_demora_hora.png")
print("/home/max/figura_3_demora_visibilidad.png")
