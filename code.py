import os
import json
import logging
import sqlite3
import datetime
import base64

#Fonction qui permet de notifier en logs que plusisurs JSON ont le même nom
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
def is_valid_json(file_path, file):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            json.load(f)
        return True
    except:
        logging.error(f"Fichier JSON invalide : {file}")
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

def request_db(path_db, file_db, table_name) :
    db_path = os.path.join(path_db, file_db[0]) #connexion à la base de données

    try:
        connection = sqlite3.connect(db_path)
        cursor = connection.cursor()
    except:
        logging.error(f"Erreur de connexion à la base de données")
        exit(1)

    try:
        #ici on fait une requete sur la table que l'on souhaite
        cursor.execute(f"SELECT * FROM {table_name};")
        lignes = cursor.fetchall()

        return lignes

    except:
        logging.error(f"Erreur lors de l'exécution de la requête SQL, la table {table_name} n'existe pas ")

    finally:
        connection.close()


#Ici la requête est ciblée
def request_db_opti(path_db, file_db, table_name,attribut, valeur) :
    db_path = os.path.join(path_db, file_db[0]) #connexion à la base de données

    try:
        connection = sqlite3.connect(db_path)
        cursor = connection.cursor()
    except:
        logging.error(f"Erreur de connexion à la base de données")
        exit(1)

    try:
        #ici on fait une requete sur la table que l'on souhaite
        query = f"SELECT * FROM {table_name} WHERE {attribut} = ?;"
        cursor.execute(query, (valeur,)) #on passe valeur en tuple pour eviter une erreur de requête

        lignes = cursor.fetchall()

        return lignes

    except:
        logging.error(f"Erreur lors de l'exécution de la requête SQL, la table {table_name} n'existe pas ")
        return None

    finally:
        connection.close()


def id_msg_verif(data, file, max_id, id_message_json):

    if data["id"] < 1 or data["id"] > max_id: #on analyse la valeur de id_message (entre 0 et max value) et si dup
        logging.error(f"L'id {data['id']} est impossible dans {file}")
        return False

    if data["id"] in id_message_json: #on vérifie si l'id est déjà dans le set (doublon)
        logging.warning(f"{file} : Attention, l'id_message {data['id']} est un doublon dans le fichier {id_message_json[data["id"]] }")
    else:
        id_message_json[data["id"]] = file
    return True


def timestamp_verif(data, file):
    annee_courante = datetime.datetime.now().year

    timestamp_date = datetime.datetime.fromtimestamp(data["timestamp"])
    timestamp_year = timestamp_date.year

    if timestamp_year< annee_courante-20 or timestamp_year > annee_courante: #on analyse la valeur de id_message (entre 0 et max value) et si dup
        logging.error(f"Le timestamp {data['timestamp']} dans {file} est incorrect")
        return False

    return True

def direction_verif(data, file):
    if data["direction"] != "originating" and data["direction"] != "destinating":
        logging.error(f"La direction : {data['direction']} dans {file} est incorrect")
        return False

    return True

def base_64_verif(data, file):
    try:
        base64.b64decode(data["content"])
    except:
        logging.error(f"L'encodage Base64 {data['content']} dans {file} est invalide")
        return False

    return True

def contact_verif(data, file, nom):
    if data["contact"] not in nom:
        logging.error(f"Le nom {data['contact']} n'existe pas dans la base de donnée")
        return False
    return True

def comparaison_ligne(data_json, data_msg_db, dict_contact_db, fichier):

    is_false = 0

    id = data_json["id"]-1

    if data_json["timestamp"] != data_msg_db[id][1]:
        logging.error(f"{fichier} : Le timestamp {data_json['timestamp']} ne correspond pas au timestamp de la bdd : {data_msg_db[id][1]}")
        is_false += 1

    if data_json["direction"] != data_msg_db[id][2]:
        logging.error(f"{fichier} : La direction {data_json['direction']} ne correspond pas à la direction de la bdd : {data_msg_db[id][2]}")
        is_false += 1

    encoded_msg = base64.b64encode(data_msg_db[id][3].encode("utf-8")).decode("utf-8")

    if data_json["content"] != encoded_msg:
        logging.error(f"{fichier} : Le contenu {data_json['content']} ne correspond pas au contenu de la BDD : {encoded_msg}")
        is_false += 1

    if data_json["contact"] != dict_contact_db[data_msg_db[id][4]]:
        logging.error(f"{fichier} : Le nom {data_json['contact']} ne correspond pas au contact de la bdd : {dict_contact_db[data_msg_db[id][4]]}")
        is_false += 1

    if is_false > 0:
        return False

    logging.info((f"Le fichier {fichier} est définitvement correct"))
    return True


def comparaison_ligne_opti(data_json, data_msg_db, data_contact_db, fichier): #Ici on compare deux listes

    is_false = 0

    if data_json["timestamp"] != data_msg_db[0][1]:
        logging.error(f"{fichier} : Le timestamp {data_json['timestamp']} ne correspond pas au timestamp de la bdd : {data_msg_db[0][1]}")
        is_false += 1

    if data_json["direction"] != data_msg_db[0][2]:
        logging.error(f"{fichier} : La direction {data_json['direction']} ne correspond pas à la direction de la bdd : {data_msg_db[0][2]}")
        is_false += 1

    encoded_msg = base64.b64encode(data_msg_db[0][3].encode("utf-8")).decode("utf-8")

    if data_json["content"] != encoded_msg:
        logging.error(f"{fichier} : Le contenu {data_json['content']} ne correspond pas au contenu de la BDD : {encoded_msg}")
        is_false += 1

    if data_contact_db[0][0] != data_msg_db[0][-1]:
        logging.error(f"{fichier} : Le id_nom {data_contact_db[0][0]} ne correspond pas au contact de la bdd : {data_msg_db[0][-1]}")
        is_false += 1

    if is_false > 0:
        return False

    logging.info((f"Le fichier {fichier} est définitvement correct"))
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
            if is_valid_json(file_path, file):
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
        file_path = os.path.join(path, file)
        path_issue = "../data/out_pb"

        if not is_valid_format(file_path, file, format_reference):
            if not os.path.exists(path_issue):
                os.mkdir(path_issue)
            os.rename(file_path, os.path.join(path_issue, file))

def test3(path_json, path_db):
    logging.info("\nDébut du test 3a - Vérification de la validité des données ")

    #Vérification des dossier
    #IN
    if not os.path.exists(path_db):
        logging.error(f"Le dossier {path_db} n'existe pas")
        exit(1)
    file_db = os.listdir(path_db)

    #OUT
    files_json = os.listdir(path_json) #lecture des fichiers après le test 1
    if not files_json:
        logging.error(f"Le dossier {path_json} est vide")
        exit(1)

    #Recupération des données de la BDD
    requete_contact_db = request_db(path_db, file_db, "contact")
    requete_sqlite_sequence = request_db(path_db, file_db, "sqlite_sequence")

    nb_messages = requete_sqlite_sequence[0][1]
    nb_contact = requete_sqlite_sequence[1][1]

    nom = [name[1] for name in requete_contact_db]

    id_message_json = {} #permet de déterminer quel sont les id_message en doublon

    path_issue = "../data/out_pb"


    for file_json in files_json:
        file_to_drop = [] #fichiers qui n'ont pas respectés une ou plusieurs conditions

        file_path_json = os.path.join(path_json, file_json)
        path_issue_json = "../data/out_pb"

        with open(file_path_json, "r", encoding="utf-8") as f:
            data = json.load(f)

        #Section ID_message
        if not id_msg_verif(data, file_json, nb_messages, id_message_json):
            file_to_drop.append(file_json)

        #Section timestamp
        if not timestamp_verif(data, file_json):
            if file_json not in file_to_drop:
                file_to_drop.append(file_json)

        #Section direction
        if not direction_verif(data, file_json):
            if file_json not in file_to_drop:
                file_to_drop.append(file_json)

        #Section Encodage base64
        if not base_64_verif(data, file_json):
            if file_json not in file_to_drop:
                file_to_drop.append(file_json)

        # Section Contact
        if not contact_verif(data, file_json, nom):
            if file_json not in file_to_drop:
                file_to_drop.append(file_json)

        if file_to_drop:
            if not os.path.exists(path_issue):
                os.mkdir(path_issue)
            os.rename(file_path_json, os.path.join(path_issue, file_json))
        else:
            logging.info((f"Le fichier {file_json} passe le test 3.a "))


def test4(path_json, path_db):
    logging.info("\nDébut du test 3b - Vérification de la mise en relation des données ")

    #Vérification des dossier
    #IN
    if not os.path.exists(path_db):
        logging.error(f"Le dossier {path_db} n'existe pas")
        exit(1)
    file_db = os.listdir(path_db)

    #OUT
    files_json = os.listdir(path_json) #lecture des fichiers après le test 1
    if not files_json:
        logging.error(f"Le dossier {path_json} est vide")
        exit(1)

    requet_messages_db = request_db(path_db, file_db, "messages")
    requete_contact_db = request_db(path_db, file_db, "contact")

    db_contact = dict(requete_contact_db)

    for file_json in files_json:

        file_path_json = os.path.join(path_json, file_json)
        path_issue_json = "../data/out_pb"

        with open(file_path_json, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not comparaison_ligne(data, requet_messages_db, db_contact, file_json):
            if not os.path.exists(path_issue_json):
                os.mkdir(path_issue_json)
            os.rename(file_path_json, os.path.join(path_issue_json, file_json))

# Il s'agit d'une amélioration du test 4 qui ne charge pas toute la base de données afin que le code puisse être utilisable
# même pour une grande base de données
# Ici on effectue une requête WHERE pour récupérer l'id du message et on compare directement cette valeur avec la nouvelle
# Ici on oublie le test 3 car la base de données est trop grande
def test_3_4_opti(path_json, path_db):
    logging.info("\nDébut du test 3b - Vérification de la mise en relation des données ")

    #Vérification des dossier
    #IN
    if not os.path.exists(path_db):
        logging.error(f"Le dossier {path_db} n'existe pas")
        exit(1)
    file_db = os.listdir(path_db)

    #OUT
    files_json = os.listdir(path_json) #lecture des fichiers après le test 1
    if not files_json:
        logging.error(f"Le dossier {path_json} est vide")
        exit(1)

    for file_json in files_json:

        file_path_json = os.path.join(path_json, file_json)
        path_issue_json = "../data/out_pb"

        with open(file_path_json, "r", encoding="utf-8") as f:
            data = json.load(f)

        id = data["id"]
        name = data["contact"]

        #Ici les requêtes sons ciblées
        requet_messages_db = request_db_opti(path_db, file_db, "messages", "id", id)
        requete_contact_db = request_db_opti(path_db, file_db, "contact", "name", name)

        if requet_messages_db is None or requet_messages_db == [] or requete_contact_db is None or requete_contact_db == []: #ici on regarde si la requete est possible et si elle ne renvoie pas rien
            logging.error(f"Échec de la requête SQL pour le fichier '{file_json}'. ID: {id}, Nom: '{name}'. Aucune correspondance trouvée dans la base de données.")
            if not os.path.exists(path_issue_json):
                os.mkdir(path_issue_json)
            os.rename(file_path_json, os.path.join(path_issue_json, file_json))
        else:
            if not comparaison_ligne_opti(data, requet_messages_db, requete_contact_db, file_json):
                if not os.path.exists(path_issue_json):
                    os.mkdir(path_issue_json)
                os.rename(file_path_json, os.path.join(path_issue_json, file_json))



def resultat(chemin_out, chemin_out_pb):
    if not os.path.exists(chemin_out):
        logging.error(f"Le dossier {chemin_out} n'existe pas")
        exit(1)
    files_out = os.listdir(chemin_out)

    if not files_out:
        print("Aucun fichier validé")
    else:
        print("Les fichiers ayant validé les tests sont :")
        for file_out in files_out:
            print(f"{file_out}")

    if not os.path.exists(chemin_out_pb):
        print("Aucun fichier erroné")
    else:
        files_out_pb = os.listdir(chemin_out_pb)

        print("Les fichiers n'ayant pas validé les tests sont :")
        for file_out_pb in files_out_pb:
            print(f"{file_out_pb}")


#Mise en place des logs
logging.basicConfig(
    filename="../journaux.log",
    level = logging.INFO, #On prend que a partir de INFO pas avant (debug)
    format="%(asctime)s - %(levelname)s - %(message)s"
)

path_out = "../data/out"  # chemin pour accéder aux fichiers JSON
path_out_pb = "../data/out_pb"  # chemin pour accéder aux fichiers JSON
path_in = "../data/in"

test1(path_out)
test2(path_out)
#test3(path_out, path_in)
#test4(path_out, path_in)
test_3_4_opti(path_out, path_in)
resultat(path_out, path_out_pb)