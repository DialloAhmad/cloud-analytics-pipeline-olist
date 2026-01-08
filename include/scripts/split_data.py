import pandas as pd
import numpy as np
import os
import shutil

# Config
INPUT_DIR = 'C:/Users/Ahmad Diallo/Documents/cloud_native_pipeline/olist_data/'  
OUTPUT_BASE_DIR = 'C:/Users/Ahmad Diallo/Documents/cloud_native_pipeline/olist_data/split_data/' 
NUM_CHUNKS = 10 # Nombre de split

# Chemins des fichiers sources
orders_path = os.path.join(INPUT_DIR, 'olist_orders_dataset.csv')
items_path = os.path.join(INPUT_DIR, 'olist_order_items_dataset.csv')

def setup_directories():
    """Création des dossiers de sortie"""
    if os.path.exists(OUTPUT_BASE_DIR):
        shutil.rmtree(OUTPUT_BASE_DIR)
    
    os.makedirs(os.path.join(OUTPUT_BASE_DIR, 'orders'))
    os.makedirs(os.path.join(OUTPUT_BASE_DIR, 'order_items'))
    print(f"Dossiers créés dans '{OUTPUT_BASE_DIR}/'")

def main():
    print("Chargement des datasets...")
    df_orders = pd.read_csv(orders_path)
    df_items = pd.read_csv(items_path)

    # Trier les commandes par date pour simuler une vrai chronologie
    # On se base sur une datetime pour être sûr du tri
    df_orders['order_purchase_timestamp'] = pd.to_datetime(df_orders['order_purchase_timestamp'])
    df_orders = df_orders.sort_values(by='order_purchase_timestamp')
    
    print(f"Total Orders: {len(df_orders)}")
    print(f"Total Items: {len(df_items)}")

    # Diviser les COMMANDES en 10 parties égales
    orders_chunks = np.array_split(df_orders, NUM_CHUNKS)

    print("\nDébut du découpage synchronisé...")

    for i, chunk_orders in enumerate(orders_chunks):
        batch_id = i + 1
        
        # Récupérer la liste des ID de commandes de ce lot
        current_order_ids = chunk_orders['order_id'].unique()
        
        # Filtrage des ITEMS qui appartiennent à ces commandes
        chunk_items = df_items[df_items['order_id'].isin(current_order_ids)]
        
        # Définir les noms de fichiers splités avec un ajoute un padding (01, 02) 
        filename_orders = f"orders_batch_{batch_id:02d}.csv"
        filename_items = f"items_batch_{batch_id:02d}.csv"
        
        path_orders = os.path.join(OUTPUT_BASE_DIR, 'orders', filename_orders)
        path_items = os.path.join(OUTPUT_BASE_DIR, 'order_items', filename_items)
        
        # Sauvegarde des fichiers
        chunk_orders.to_csv(path_orders, index=False)
        chunk_items.to_csv(path_items, index=False)
        
        print(f"   [Batch {batch_id:02d}] Orders: {len(chunk_orders):<6} | Items: {len(chunk_items):<6} -> Sauvegardé")

    print(f"\nTerminé ! Les fichiers sont dans '{OUTPUT_BASE_DIR}/'")

if __name__ == "__main__":
    setup_directories()
    main()