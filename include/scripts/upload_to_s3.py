import boto3
import os
import json
import time

# Config 
BUCKET_NAME = 'bucket' 
LOCAL_SPLIT_DIR = 'include/data'
STATE_FILE = 'upload_state.json'

# S3 Prefixes
S3_PREFIX_ORDERS = 'olist/orders/'
S3_PREFIX_ITEMS = 'olist/order_items/'

def load_state():
    """Lecture du dernier batch traité depuis le fichier json."""
    if not os.path.exists(STATE_FILE):
        return 0 # Si le fichier n'existe pas, on commence à 0
    with open(STATE_FILE, 'r') as f:
        data = json.load(f)
        return data.get('last_batch', 0)

def save_state(batch_id):
    """Sauvegarde le numéro du batch qu'on vient de traiter."""
    with open(STATE_FILE, 'w') as f:
        json.dump({'last_batch': batch_id}, f)

def upload_file(s3_client, local_path, s3_key):
    """Envoie un fichier sur S3."""
    try:
        print(f"   ⬆️  Upload de {os.path.basename(local_path)} vers {s3_key}...")
        s3_client.upload_file(local_path, BUCKET_NAME, s3_key)
        return True
    except Exception as e:
        print(f"Erreur upload : {e}")
        return False

def main():
    s3 = boto3.client('s3')
    
    # Où en sommes-nous ?
    last_batch = load_state()
    current_batch = last_batch + 1
    
    # Format du nom de fichier attendu (ex: orders_batch_01.csv)
    filename_orders = f"orders_batch_{current_batch:02d}.csv"
    filename_items = f"items_batch_{current_batch:02d}.csv"
    
    path_orders = os.path.join(LOCAL_SPLIT_DIR, 'orders', filename_orders)
    path_items = os.path.join(LOCAL_SPLIT_DIR, 'order_items', filename_items)
    
    # Vérifier si les fichiers existent sinon, terminer
    if not os.path.exists(path_orders):
        print(f"Plus aucun fichier à traiter ! (Dernier batch : {last_batch})")
        return

    print(f"🚀 Traitement du Batch #{current_batch:02d}...")

    # Upload des Orders
    s3_key_orders = f"{S3_PREFIX_ORDERS}{filename_orders}"
    if upload_file(s3, path_orders, s3_key_orders):
        
        # Upload des Items (Seulement si Orders a réussi)
        s3_key_items = f"{S3_PREFIX_ITEMS}{filename_items}"
        upload_file(s3, path_items, s3_key_items)
        
        # Mise à jour de l'état du tracker
        save_state(current_batch)
        print(f"Batch #{current_batch:02d} terminé avec succès.\n")
        
    else:
        print("L'upload a échoué, on ne met pas à jour l'état pour réessayer la prochaine fois.")

if __name__ == "__main__":
    main()