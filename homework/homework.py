# pylint: disable=import-outside-toplevel
import os
import glob
import zipfile
import pandas as pd
from datetime import datetime
    
def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta files/input/ en varios archivos csv.zip comprimidos para ahorrar espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv. Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months
    """
    
    input_dir = 'files/input/'
    output_dir = 'files/output/'
    os.makedirs(output_dir, exist_ok=True)

    # 1. Leer CSV dentro de cada ZIP sin descomprimir
    all_zips = glob.glob(os.path.join(input_dir, '*.csv.zip'))
    dfs = []
    for zip_path in all_zips:
        with zipfile.ZipFile(zip_path) as z:
            inner = z.namelist()[0]
            with z.open(inner) as f:
                dfs.append(pd.read_csv(f))
    data = pd.concat(dfs, ignore_index=True)

    # 2. client.csv
    client = data[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']].copy()
    client['job'] = client['job'].str.replace('.', '', regex=False).str.replace('-', '_', regex=False)
    client['education'] = client['education'].str.replace('.', '_', regex=False).replace('unknown', pd.NA)
    client['credit_default'] = (client['credit_default'] == 'yes').astype(int)
    client['mortgage'] = (client['mortgage'] == 'yes').astype(int)
    client.to_csv(os.path.join(output_dir, 'client.csv'), index=False)

    # 3. campaign.csv
    camp = data[['client_id', 'number_contacts', 'contact_duration', 'previous_campaign_contacts', 'previous_outcome', 'campaign_outcome', 'day', 'month']].copy()
    camp['previous_outcome'] = (camp['previous_outcome'] == 'success').astype(int)
    camp['campaign_outcome'] = (camp['campaign_outcome'] == 'yes').astype(int)
    camp['last_contact_date'] = camp.apply(
        lambda r: datetime.strptime(f"{int(r['day'])} {r['month']} 2022", "%d %b %Y")
                              .strftime('%Y-%m-%d'),
        axis=1
    )
    camp = camp.drop(columns=['day', 'month'])
    camp.to_csv(os.path.join(output_dir, 'campaign.csv'), index=False)

    # 4. economics.csv
    econ = data[['client_id', 'cons_price_idx', 'euribor_three_months']].copy()
    econ.to_csv(os.path.join(output_dir, 'economics.csv'), index=False)

if __name__ == "__main__":
    clean_campaign_data()