# MySQL2SQLite

MySQL2SQLite est une application en ligne de commande permettant de migrer les données d'une base de données MySQL vers une base SQLite.

Dans la version actuelle est transféré la struture des tables, les index, et les données.
Dans une version future, les triggers seront également migrés.


Utilisation :

--sqliteFile : chemin complet vers le fichier SQLite à générer
-- sqlitePassword : mot de passe pour le fichier SQLite
--mysqlUser  : nom de l'utilisateur pour l'accès MYSql
--mysqlPassword : mot de passe pour l'accès à MySQL
--mysqlDatabase : nom de la base de de données MySQL
--mysqlHost : adresse du serveur MySQL
--schemaOnly : 1 pour ne migrer que la structure de la base 
--logQuery : 1 pour afficher toutes les requêtes dans la sortie de la console

Exemple :
mysql2sqlite --sqliteFile=d:\sqlite.db --mysqlUser=root --mysqlPassword=0000 --mysqlDatabase=pureplayer --mysqlHost=localhost --schemaOnly=0


