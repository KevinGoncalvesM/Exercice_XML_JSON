import os
import json
import logging

#Fonction qui permet de notifier en logs que plusisurs JSON on le même nom
def warning_dup(fichiers):
    set_fichiers = set() # Permet d'éviter tout doublon
    doublons = set()
    for fichier in fichiers:
        if fichier in set_fichiers:
            doublons.add(fichier)
        else:
            set_fichiers.add(fichier)

    for fichier in doublons:
        logging.warning(f"Plusieurs exemplaires détectés pour le fichier : {fichier}")

#permet de voir si le JSON est valide (on parse le JSON)
def is_valid_json(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            json.load(f)
        return True
    except (ValueError, json.JSONDecodeError) as e:
        logging.error(f"Erreur JSON dans '{file_path}': {e}")
        return False

#Mise en place des logs
logging.basicConfig(
    filename="../journaux.log",
    level = logging.INFO, #On prend que a partir de INFO pas avant (debug)
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Début du test 1 - Vérification des fichiers JSON \n")

path = "../data/out" #chemin pour accéder aux fichiers JSON
files = os.listdir(path) #lecture des fichiers
logging.info(f"Le nombre de fichiers trouvés dans {path} est {len(files)}")

warning_dup(files) #Appel de la fonction pour mettre en log les fichiers dupliqués
#
for file in files:
    file_path = os.path.join(path, file)  # Chemin complet du fichier
    path_issue = "../data/out_pb"
    if file.endswith(".json"):  # Vérifier que c'est bien un fichier JSON
        if is_valid_json(file_path):
            print(f"{file} -> fichier JSON valide.")
            logging.info(f"{file} -> fichier JSON valide")
        else:
            print(f"{file} est invalide")
            if not os.path.exists(path_issue):
                os.mkdir(path_issue)
            os.rename(file_path, os.path.join(path_issue, file))  # Déplacement en cas d'erreur
    else:
        print(f"{file} n'est pas un JSON")
        logging.error(f"{file} n'est pas un JSON")
        if not os.path.exists(path_issue):
            os.mkdir(path_issue)
        os.rename(file_path, os.path.join(path_issue, file))