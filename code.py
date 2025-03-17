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

#Permet de deéterminer si le format demandé est conforme avec ceux des fichiers JSON
def is_valid_format(file_path, file, format_dict):
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if len(data) != len(format_dict):
        logging.error(f"Le fichier {file} ne correspond pas au format attendu, pas le même nombre de paramètre.")
        return False

    for key, value in data.items():
        if key not in format_dict:
            logging.error(f"Clé '{key}' dans le fichier {file}.")
            return False
        elif type(value) != format_dict[key]:
            logging.error(f"Le type de la valeur pour la clé '{key}' dans {file} est incorrect : attendu {format_dict[key]}, mais trouvé {type(value)}.")
            return False

    keys_in_file = list(data.keys())
    keys_in_reference = list(format_dict.keys())

    if keys_in_reference != keys_in_file:
        logging.error(f"L'ordre des clés dans le fichier {file} est incorrect")
        return False

    logging.info(f"Le fichier {file} est conforme au format attendu.")
    return True


def test1(path) :
    logging.info("\nDébut du test 1 - Vérification des fichiers JSON ")

    if not os.path.exists(path):
        logging.error(f"Le dossier {path} n'existe pas")
        exit(1)
    files = os.listdir(path) #lecture des fichiers
    if not files:
        logging.error(f"Le dossier {path} est vide")
        exit(1)
    logging.info(f"Le nombre de fichiers trouvés dans {path} est {len(files)}")

    warning_dup(files) #Appel de la fonction pour mettre en log les fichiers dupliqués
    #
    for file in files:
        file_path = os.path.join(path, file)  # Chemin complet du fichier
        path_issue = "../data/out_pb"
        if file.endswith(".json"):  # Vérifier que c'est bien un fichier JSON
            if is_valid_json(file_path):
                logging.info(f"{file} -> fichier JSON valide")
            else:
                if not os.path.exists(path_issue):
                    os.mkdir(path_issue)
                os.rename(file_path, os.path.join(path_issue, file))  # Déplacement en cas d'erreur
        else:
            logging.error(f"{file} n'est pas un JSON")
            if not os.path.exists(path_issue):
                os.mkdir(path_issue)
            os.rename(file_path, os.path.join(path_issue, file))

def test2(path):
    logging.info("\nDébut du test 2 - Vérification de la structure du JSON ")

    files = os.listdir(path) #lecture des fichiers après le test 1
    if not files:
        logging.error(f"Le dossier {path} est vide")
        exit(1)

    format_reference = {"id" : int, "timestamp" : int, "direction" : str, "content" : str, "contact" : str} #Format de base attendu

    for file in files:
        file_path = os.path.join(path, file)  # Chemin complet du fichier
        path_issue = "../data/out_pb"

        if not is_valid_format(file_path, file, format_reference):
            if not os.path.exists(path_issue):
                os.mkdir(path_issue)
            os.rename(file_path, os.path.join(path_issue, file))


#Mise en place des logs
logging.basicConfig(
    filename="../journaux.log",
    level = logging.INFO, #On prend que a partir de INFO pas avant (debug)
    format="%(asctime)s - %(levelname)s - %(message)s"
)

path = "../data/out"  # chemin pour accéder aux fichiers JSON

test1(path)
test2(path)