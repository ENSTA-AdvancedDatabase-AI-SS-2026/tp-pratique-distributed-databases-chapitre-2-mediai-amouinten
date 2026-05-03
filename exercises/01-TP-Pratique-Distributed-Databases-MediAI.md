# TP Pratique – Chapitre 2 : Distributed Databases
## Cas d'étude : MediAI – Plateforme de santé intelligente distribuée
### ENSTA 3A – Filière AI & Systèmes de Santé

---

> **Nom :** OUINTEN  
> **Prénom :** Mohammed Amine  
> **Date :** 28-04-2026  
> **Note :** ___ / 100

---

## 🌍 Contexte : La plateforme MediAI

MediAI est une startup de e-santé qui déploie une plateforme d'IA médicale sur **4 sites géographiques** :

| Site | Localisation | Rôle | Workers Citus |
|------|-------------|------|---------------|
| **HQ** | Paris, France | Coordinator (nœud maître) | `citus_master` |
| **Site EU-S** | Tunis, Tunisie | Patients Afrique du Nord | `citus_worker1` |
| **Site NA** | Montréal, Canada | Patients Amérique du Nord | `citus_worker2` |
| **Site APAC** | Tokyo, Japon | Patients Asie-Pacifique | `citus_worker3` |

La plateforme stocke :
- 📋 **Patients** : données démographiques
- 🏥 **MedicalRecords** : résultats d'examens + scores IA
- 🤖 **TrainingData** : features pour entraîner les modèles d'IA médicale
- 💳 **Transactions** : paiements et remboursements

---

## ⚙️ Partie 1 – Mise en place du cluster Citus (10 pts)

### 1.1 – Lancement du cluster Docker

Exécutez les commandes suivantes dans votre terminal :

```bash
# Démarrer les 4 conteneurs (1 coordinator + 3 workers)
docker-compose up -d

# Vérifier que les 4 conteneurs sont UP
docker ps

# Se connecter au coordinator
docker exec -it citus_master psql -U postgres -d mediAI
```

📸 **Capture d'écran attendue** : résultat de `docker ps` montrant les 4 conteneurs en état `Up`
> 
> ```
> CONTAINER ID   IMAGE                  COMMAND                  CREATED        STATUS        PORTS                    NAMES
> xxxxxxxxxxxx   citusdata/citus:12.1   "docker-entrypoint.s…"   2 minutes ago  Up 2 minutes  0.0.0.0:5432->5432/tcp   citus_master
> xxxxxxxxxxxx   citusdata/citus:12.1   "docker-entrypoint.s…"   2 minutes ago  Up 2 minutes  5432/tcp                 citus_worker1
> xxxxxxxxxxxx   citusdata/citus:12.1   "docker-entrypoint.s…"   2 minutes ago  Up 2 minutes  5432/tcp                 citus_worker2
> xxxxxxxxxxxx   citusdata/citus:12.1   "docker-entrypoint.s…"   2 minutes ago  Up 2 minutes  5432/tcp                 citus_worker3
> ```

---

### 1.2 – Enregistrement des workers

Une fois connecté au coordinator, enregistrez les 3 workers :

```sql
-- Enregistrer les workers dans le cluster
SELECT citus_add_node('citus_worker1', 5432);
SELECT citus_add_node('citus_worker2', 5432);
SELECT citus_add_node('citus_worker3', 5432);
```

**Question 1.2.a** : Quelle est la différence entre un **coordinator** et un **worker** dans Citus ?

> **ma réponse :**
> le **coordinator** (nœud maître) est le point d'entrée unique du cluster : il reçoit toutes les requêtes SQL des clients, les analyse, les décompose en sous-requêtes, et les distribue aux workers appropriés.il maintient les métadonnées de distribution (quels shards sont sur quels workers) dans les tables système Citus (pg_dist_node, pg_dist_shard, etc.). Il agrège ensuite les résultats et les renvoie au client. Il ne stocke pas les données utilisateur.
Les workers sont les nœuds de stockage et de calcul: ils hébergent physiquement les shards (fragments) des tables distribuées et exécutent les sous-requêtes qui leur sont déléguées par le coordinator. Chaque worker est une instance PostgreSQL + Citus indépendante. Dans notre cluster : citus_worker1 = Tunis, citus_worker2 = Montréal, citus_worker3 = Tokyo.
> _______________________________________________

**Question 1.2.b** : Vérifiez que les 3 workers sont bien enregistrés avec la requête ci-dessous. Combien de lignes obtenez-vous ?

```sql
SELECT nodeid, nodename, nodeport, isactive
FROM pg_dist_node
ORDER BY nodeid;
```

> **Résultat et réponse :**
>
> ```
>  nodeid |    nodename    | nodeport | isactive
> --------+----------------+----------+----------
>       1 | citus_worker1  |     5432 | t
>       2 | citus_worker2  |     5432 | t
>       3 | citus_worker3  |     5432 | t
> (3 rows)
> ```
>
> On obtient **3 lignes**, une par worker, toutes avec `isactive = t` (true). Cela confirme que les 3 workers (Tunis, Montréal, Tokyo) sont bien enregistrés et actifs dans le cluster.


---

### 1.3 – Chargement du schéma et des données

```bash
# Charger le schéma
docker exec -it citus_master psql -U postgres -d mediAI -f /data/schema-mediAI.sql

# Initialiser la distribution Citus
docker exec -it citus_master psql -U postgres -d mediAI -f /data/init-cluster.sql

# Insérer les données de test
docker exec -it citus_master psql -U postgres -d mediAI -f /data/seed-mediAI.sql
```

**Vérification** :

```sql
-- Vérifier le nombre de lignes par table
SELECT 'Patients'       AS table_name, COUNT(*) AS nb_lignes FROM Patients
UNION ALL
SELECT 'MedicalRecords',               COUNT(*)              FROM MedicalRecords
UNION ALL
SELECT 'TrainingData',                 COUNT(*)              FROM TrainingData
UNION ALL
SELECT 'Transactions',                 COUNT(*)              FROM Transactions;
```

> **Résultat attendu et observé :**
> 
> | table_name | nb_lignes attendu | nb_lignes observé |
> |---|---|---|
> | Patients | 20 | 20 |
> | MedicalRecords | 14 | 14 |
> | TrainingData | 13 | 13 |
> | Transactions | 18 | 18 |

---

## 🗂️ Partie 2 – Fragmentation (30 pts)

### 2.1 – Fragmentation Horizontale : `TrainingData` par `siteOrigin` (10 pts)

La **fragmentation horizontale** divise une table en sous-ensembles de **lignes** selon un critère.

#### Rappel théorique

Soit la table `TrainingData(idData, idRecord, siteOrigin, featureVector, label, quality)`.

La règle de fragmentation est :

```
F_Paris    = σ(siteOrigin = 'Paris')    (TrainingData)
F_Tunis    = σ(siteOrigin = 'Tunis')    (TrainingData)
F_Montreal = σ(siteOrigin = 'Montreal') (TrainingData)
F_Tokyo    = σ(siteOrigin = 'Tokyo')    (TrainingData)
```

#### ✏️ Exercice 2.1.a – Créer les fragments comme des vues SQL

Complétez les vues suivantes (remplacez les `___`) :

```sql
-- Fragment Paris
CREATE OR REPLACE VIEW TrainingData_Paris AS
    SELECT * FROM TrainingData
    WHERE siteOrigin = 'Paris';        -- ← compléter

-- Fragment Tunis
CREATE OR REPLACE VIEW TrainingData_Tunis AS
    SELECT * FROM TrainingData
    WHERE siteOrigin = 'Tunis';           -- ← compléter

-- Fragment Montréal
CREATE OR REPLACE VIEW TrainingData_Montreal AS
    SELECT * FROM TrainingData
    WHERE siteOrigin = 'Montreal';         -- ← compléter

-- Fragment Tokyo
CREATE OR REPLACE VIEW TrainingData_Tokyo AS
    SELECT * FROM TrainingData
    WHERE siteOrigin = 'Tokyo';                   -- ← compléter
```


#### ✏️ Exercice 2.1.b – Vérifier la completeness (complétude)

La **complétude** garantit que tout tuple de la table globale appartient à au moins un fragment. Vérifiez-la :

```sql
-- Compter les lignes par fragment
SELECT siteOrigin, COUNT(*) AS nb_lignes
FROM TrainingData
GROUP BY siteOrigin
ORDER BY siteOrigin;

-- Le total doit égaler la table globale
SELECT COUNT(*) AS total_global FROM TrainingData;
```

**Question 2.1.b** : La propriété de complétude est-elle respectée ? Justifiez.

> **Réponse :**
>
> Oui, la propriété de **complétude** est respectée. En exécutant les requêtes :
> ```sql
> SELECT siteOrigin, COUNT(*) AS nb_lignes FROM TrainingData GROUP BY siteOrigin ORDER BY siteOrigin;
> -- Montreal: 3, Paris: 4, Tokyo: 3, Tunis: 3 → total = 13
>
> SELECT COUNT(*) AS total_global FROM TrainingData;
> -- total = 13
> ```
> La somme des lignes de chaque fragment (3 + 4 + 3 + 3 = 13) est égale au nombre total de lignes de la table globale (13). Tout tuple appartient à exactement un fragment (les fragments sont disjoints et leur union recouvre la table entière). La complétude est donc vérifiée.

#### ✏️ Exercice 2.1.c – Distribution Citus effective

Vérifiez comment Citus a réellement distribué les données :

```sql
-- Voir les shards de TrainingData
SELECT s.shardid, p.nodename, p.nodeport,
       s.shardminvalue, s.shardmaxvalue
FROM pg_dist_shard s
JOIN pg_dist_shard_placement p ON s.shardid = p.shardid
WHERE s.logicalrelid = 'TrainingData'::regclass
ORDER BY s.shardid;
```

> **Capture de la requête shards :** (résultat indicatif)
> ```
>  shardid | nodename      | nodeport | shardminvalue | shardmaxvalue
> ---------+---------------+----------+---------------+---------------
>   102008 | citus_worker1 |     5432 | -2147483648   | -715827883
>   102009 | citus_worker2 |     5432 | -715827882    | 715827882
>   102010 | citus_worker3 |     5432 | 715827883     | 2147483647
> ```
 
**Question 2.1.c** : Sur quel(s) worker(s) les données du site "Tokyo" sont-elles stockées ?
 
> **Réponse :**
> Citus distribue les données en appliquant une fonction de hachage sur la clé de distribution (`siteOrigin`). La valeur hash de `'Tokyo'` détermine dans quel shard range elle tombe. D'après la distribution par hash, les données Tokyo se trouvent sur le worker dont le range de hash couvre `hash('Tokyo')`. En pratique avec 3 workers et une distribution uniforme, les données Tokyo se retrouvent sur **un seul worker** (par exemple `citus_worker3` / Tokyo). La requête sur `pg_dist_shard_placement` permet de l'identifier précisément.
 
---

### 2.2 – Fragmentation Verticale : `MedicalRecords` (10 pts)

La **fragmentation verticale** divise une table en sous-ensembles de **colonnes** selon leur usage.

#### Rappel théorique

```
R(idRecord, idPatient, country, date, examType, result, aiModelUsed, aiScore, aiVersion)

Fragment A – Données cliniques (médecins) :
  FA = Π(idRecord, idPatient, country, date, examType, result) (MedicalRecords)

Fragment B – Données IA (data scientists) :
  FB = Π(idRecord, idPatient, country, aiModelUsed, aiScore, aiVersion) (MedicalRecords)
```

**Condition** : `idRecord` doit apparaître dans les deux fragments → propriété de **reconstructibilité**.

#### ✏️ Exercice 2.2.a – Identifier les groupes d'utilisateurs

**Question** : Pourquoi séparer les données cliniques des données IA ? Donnez 2 raisons.

> 1. _______________________________________________  
> 2. _______________________________________________

#### ✏️ Exercice 2.2.b – Les vues sont déjà créées dans le schéma, testez-les

```sql
-- Tester le fragment clinique
SELECT * FROM MedicalRecords_Clinical LIMIT 5;

-- Tester le fragment IA
SELECT * FROM MedicalRecords_AI LIMIT 5;

-- Reconstruction de la table originale (JOIN sur idRecord)
SELECT fc.idRecord, fc.idPatient, fc.date, fc.examType, fc.result,
       fi.aiModelUsed, fi.aiScore, fi.aiVersion
FROM MedicalRecords_Clinical fc
JOIN MedicalRecords_AI fi ON fc.idRecord = fi.idRecord
LIMIT 5;
```

📸 **Capture d'écran** : résultat de la reconstruction

> **Collez votre capture ici :**
> 
> ```
> [VOTRE CAPTURE]
> ```

#### ✏️ Exercice 2.2.c – Créer une vraie fragmentation verticale physique

Créez deux tables séparées qui implémentent physiquement les fragments :

```sql
-- Table Fragment A : Données cliniques
CREATE TABLE MedRec_Clinical (
    idRecord    INTEGER,
    idPatient   INTEGER,
    country     VARCHAR(100),
    date        DATE,
    examType    VARCHAR(100),
    result      TEXT
);

-- TODO : Créez la TABLE MedRec_AI avec les colonnes appropriées
-- Votre code ici :
CREATE TABLE MedRec_AI (
    ___                -- ← compléter avec les bonnes colonnes
);

-- Peupler les tables depuis MedicalRecords
INSERT INTO MedRec_Clinical
    SELECT idRecord, idPatient, country, date, examType, result
    FROM MedicalRecords;

-- TODO : Écrire l'INSERT pour MedRec_AI
-- Votre code ici :
INSERT INTO MedRec_AI
    SELECT ___ FROM MedicalRecords;   -- ← compléter
```

> **Votre code SQL :**
> 
> ```sql
> 
> ```

---

### 2.3 – Fragmentation Hybride : `Transactions` (10 pts)

La **fragmentation hybride** combine fragmentation horizontale ET verticale.

#### Schéma de la fragmentation hybride MediAI

```
Table Transactions (idTrans, idPatient, country, date, type, amount, currency, status)

Étape 1 – Fragmentation Horizontale par country :
  H_France   = σ(country = 'France')  (Transactions)
  H_Tunisia  = σ(country = 'Tunisia') (Transactions)
  H_Canada   = σ(country = 'Canada')  (Transactions)
  H_Japan    = σ(country = 'Japan')   (Transactions)

Étape 2 – Fragmentation Verticale sur chaque fragment H :
  Sur H_France → V1 : données financières    (idTrans, idPatient, date, amount, currency)
               → V2 : données de gestion     (idTrans, idPatient, type, status)
```

#### ✏️ Exercice 2.3.a – Compléter le schéma hybride

Dessinez (ou décrivez textuellement) le schéma complet des 8 fragments qui résultent de la fragmentation hybride (4 pays × 2 colonnes).

> **Votre réponse :**
> 
> | Fragment | country | Colonnes |
> |----------|---------|----------|
> | F_FR_FIN | France  | idTrans, idPatient, date, amount, currency |
> | F_FR_MGT | France  | ___ |
> | F_TN_FIN | Tunisia | ___ |
> | F_TN_MGT | Tunisia | ___ |
> | F_CA_FIN | Canada  | ___ |
> | F_CA_MGT | Canada  | ___ |
> | F_JP_FIN | Japan   | ___ |
> | F_JP_MGT | Japan   | ___ |

#### ✏️ Exercice 2.3.b – Implémentation SQL des fragments hybrides

Créez les 8 fragments comme des vues SQL (exemple pour France donné, à vous pour les autres) :

```sql
-- ── France ──────────────────────────────────────────────────
CREATE OR REPLACE VIEW Trans_FR_Financial AS
    SELECT idTrans, idPatient, date, amount, currency
    FROM Transactions
    WHERE country = 'France';

CREATE OR REPLACE VIEW Trans_FR_Management AS
    SELECT idTrans, idPatient, type, status
    FROM Transactions
    WHERE country = 'France';

-- ── Tunisia ─────────────────────────────────────────────────
-- TODO : Créez les 2 vues pour la Tunisia
-- Votre code ici :
___

-- ── Canada ──────────────────────────────────────────────────
-- TODO : Créez les 2 vues pour le Canada
-- Votre code ici :
___

-- ── Japan ───────────────────────────────────────────────────
-- TODO : Créez les 2 vues pour le Japon
-- Votre code ici :
___
```

> **Votre code SQL complet :**
> 
> ```sql
> 
> ```

#### ✏️ Exercice 2.3.c – Reconstruction

Écrivez la requête SQL qui reconstruit la table `Transactions` complète à partir des fragments France :

```sql
-- Reconstruction France : joindre F_FR_FIN et F_FR_MGT
SELECT fin.idTrans, fin.idPatient, fin.date, fin.amount, fin.currency,
       ___, ___          -- ← ajouter les colonnes de MGT
FROM Trans_FR_Financial fin
JOIN Trans_FR_Management mgt ON ___ = ___;  -- ← condition de jointure
```

> **Votre requête complétée :**
> 
> ```sql
> 
> ```

---

## 🔍 Partie 3 – Requêtes distribuées (30 pts)

### 3.1 – Requête de profil patient complet (10 pts)

#### Contexte

Un médecin parisien demande le profil complet d'un patient : données démographiques + derniers examens + score IA.

#### ✏️ Exercice 3.1.a – Écrire la requête

```sql
-- Q1 : Profil complet du patient Mohamed Benali
SELECT
    p.name,
    p.age,
    p.city,
    p.country,
    mr.date,
    mr.examType,
    mr.result,
    mr.aiModelUsed,
    mr.aiScore
FROM Patients p
JOIN MedicalRecords mr ON p.idPatient = mr.idPatient
                       AND p.country  = mr.country
WHERE p.name = 'Mohamed Benali'
ORDER BY mr.date DESC;
```

**Exécutez cette requête et collez le résultat :**

> ```
> [VOTRE RÉSULTAT]
> ```

#### ✏️ Exercice 3.1.b – Analyser le plan d'exécution distribué

```sql
-- Analyser le plan d'exécution
EXPLAIN (VERBOSE, FORMAT TEXT)
SELECT p.name, p.age, mr.date, mr.examType, mr.aiScore
FROM Patients p
JOIN MedicalRecords mr ON p.idPatient = mr.idPatient AND p.country = mr.country
WHERE p.name = 'Mohamed Benali';
```

📸 **Capture d'écran** : résultat de EXPLAIN

> **Collez votre capture ici :**
> 
> ```
> [VOTRE CAPTURE]
> ```

**Question 3.1.b** : Identifiez dans le plan d'exécution :
- Le type de JOIN utilisé : _______________
- Sur quel(s) worker(s) la requête s'exécute-t-elle : _______________
- Pourquoi la co-localisation (`country` comme clé commune) est-elle avantageuse ici ?

> _______________________________________________

---

### 3.2 – Requête agrégée multi-sites (10 pts)

#### Contexte

L'équipe data science veut comparer les **performances des modèles IA** par site géographique.

#### ✏️ Exercice 3.2.a – Écrire la requête

```sql
-- Q2 : Performance moyenne des modèles IA par site
SELECT
    p.siteOrigin            AS site,
    mr.aiModelUsed          AS modele_ia,
    COUNT(mr.idRecord)      AS nb_examens,
    ROUND(AVG(mr.aiScore)::numeric, 4) AS score_moyen,
    ROUND(MIN(mr.aiScore)::numeric, 4) AS score_min,
    ROUND(MAX(mr.aiScore)::numeric, 4) AS score_max
FROM MedicalRecords mr
JOIN Patients p ON mr.idPatient = p.idPatient
               AND mr.country   = p.country
WHERE mr.aiScore IS NOT NULL
GROUP BY p.siteOrigin, mr.aiModelUsed
ORDER BY p.siteOrigin, score_moyen DESC;
```

**Exécutez et interprétez les résultats :**

> ```
> [VOTRE RÉSULTAT]
> ```

**Question 3.2.a** : Quel modèle IA obtient le meilleur score moyen ? Sur quel site ?

> _______________________________________________

#### ✏️ Exercice 3.2.b – Requête avec filtre sur les données à risque

```sql
-- Q3 : Patients avec score IA élevé (>0.95) tous sites confondus
SELECT
    p.name,
    p.country,
    mr.examType,
    mr.aiModelUsed,
    mr.aiScore,
    CASE
        WHEN mr.aiScore >= 0.99 THEN '🔴 Critique'
        WHEN mr.aiScore >= 0.97 THEN '🟠 Élevé'
        WHEN mr.aiScore >= 0.95 THEN '🟡 Modéré'
        ELSE                        '🟢 Normal'
    END AS niveau_alerte
FROM MedicalRecords mr
JOIN Patients p ON mr.idPatient = p.idPatient
               AND mr.country   = p.country
WHERE mr.aiScore > 0.95
ORDER BY mr.aiScore DESC;
```

**Exécutez et analysez :**

> ```
> [VOTRE RÉSULTAT]
> ```

**Question 3.2.b** : Cette requête s'exécute-t-elle sur un seul worker ou plusieurs ? Pourquoi ?

> _______________________________________________

---

### 3.3 – Requête financière cross-site (10 pts)

#### ✏️ Exercice 3.3.a – Chiffre d'affaires par pays et type

```sql
-- Q4 : Chiffre d'affaires par pays (transactions committed uniquement)
SELECT
    country,
    currency,
    type,
    COUNT(*)            AS nb_transactions,
    SUM(amount)         AS total_amount,
    AVG(amount)         AS avg_amount
FROM Transactions
WHERE status = 'committed'
  AND amount > 0           -- exclure les remboursements
GROUP BY country, currency, type
ORDER BY country, total_amount DESC;
```

> ```
> [VOTRE RÉSULTAT]
> ```

#### ✏️ Exercice 3.3.b – Écrire votre propre requête

Écrivez une requête originale qui combine au moins **2 tables** et utilise une **agrégation** sur les données MediAI. Justifiez son intérêt métier.

> **Intérêt métier :** _______________________________________________

> **Votre requête SQL :**
> 
> ```sql
> -- Votre requête ici
> 
> ```

> **Résultat :**
> 
> ```
> [VOTRE RÉSULTAT]
> ```

---

## 🔐 Partie 4 – Transactions distribuées : Two-Phase Commit (30 pts)

### 4.1 – Contexte et rappel théorique (5 pts)

Le **Two-Phase Commit (2PC)** garantit qu'une transaction distribuée est **atomique** : soit elle est validée sur **tous les nœuds**, soit elle est annulée sur **tous les nœuds**.

```
           COORDINATOR
               │
      ┌────────┴────────┐
      │    Phase 1      │
      │  PREPARE ──→    │
      │  ←── READY      │
      │  ←── READY      │
      │    Phase 2      │
      │  COMMIT ──→     │
      └─────────────────┘
```

**Question 4.1** : Décrivez dans vos propres mots les deux phases du 2PC. Que se passe-t-il si un worker répond `ABORT` en Phase 1 ?

> **Phase 1 (Prepare) :**
> 
> _______________________________________________

> **Phase 2 (Commit) :**
> 
> _______________________________________________

> **Si un worker répond ABORT :**
> 
> _______________________________________________

---

### 4.2 – Simulation d'un 2PC en SQL PostgreSQL (15 pts)

#### Scénario

Un patient japonais (`Yuki Tanaka`, idPatient=16) consulte en urgence depuis Paris. La transaction doit :
1. Créer un enregistrement médical → sur le **worker Tokyo** (son site d'origine)
2. Créer une transaction financière → sur le **worker Paris** (lieu de la consultation)

**Ces deux opérations doivent être atomiques.**

#### ✏️ Exercice 4.2.a – Phase 1 : PREPARE (sur le coordinator)

```sql
-- ── Démarrer la transaction distribuée ──────────────────────
BEGIN;

-- Opération 1 : Nouveau dossier médical pour Yuki Tanaka
INSERT INTO MedicalRecords (idPatient, country, date, examType, result, aiModelUsed, aiScore, aiVersion)
VALUES (16, 'Japan', NOW()::DATE, 'Consultation urgence', 'Bilan général - patient en déplacement',
        'DiagNet-3', 0.8934, 'v3.2');

-- Opération 2 : Transaction financière associée (en France cette fois)
INSERT INTO Transactions (idPatient, country, date, type, amount, currency, status)
VALUES (16, 'Japan', NOW(), 'consultation', 15000, 'JPY', 'pending');

-- ── Phase 1 : Préparer la transaction (2PC) ─────────────────
-- Le coordinator demande à tous les workers de se préparer
PREPARE TRANSACTION 'mediAI_urgence_yuki_2024';
```

📸 **Capture d'écran** : exécution du PREPARE TRANSACTION

> **Collez votre capture ici :**
> 
> ```
> [VOTRE CAPTURE]
> ```

#### ✏️ Exercice 4.2.b – Vérifier les transactions préparées

```sql
-- Voir les transactions en attente de validation (prepared)
SELECT gid, prepared, owner, database
FROM pg_prepared_xacts;
```

**Question 4.2.b** : Que contient la colonne `gid` ? À quoi sert-elle dans le protocole 2PC ?

> _______________________________________________

#### ✏️ Exercice 4.2.c – Phase 2 : COMMIT ou ROLLBACK

**Scénario A : Tout s'est bien passé → COMMIT**

```sql
-- Phase 2a : Valider la transaction préparée
COMMIT PREPARED 'mediAI_urgence_yuki_2024';

-- Vérifier que les données sont bien insérées
SELECT idRecord, idPatient, date, examType, aiScore
FROM MedicalRecords
WHERE idPatient = 16
ORDER BY date DESC;
```

> ```
> [VOTRE RÉSULTAT]
> ```

**Scénario B : Un worker a échoué → ROLLBACK**

```sql
-- Simuler une nouvelle transaction pour tester le rollback
BEGIN;
INSERT INTO Transactions (idPatient, country, date, type, amount, currency, status)
VALUES (16, 'Japan', NOW(), 'consultation_test', 5000, 'JPY', 'pending');
PREPARE TRANSACTION 'mediAI_test_rollback';

-- Phase 2b : Annuler la transaction préparée (simule un échec)
ROLLBACK PREPARED 'mediAI_test_rollback';

-- Vérifier que la transaction a bien été annulée
SELECT COUNT(*) FROM Transactions WHERE type = 'consultation_test';
```

> ```
> [VOTRE RÉSULTAT]
> ```

---

### 4.3 – Gestion des défaillances (10 pts)

#### ✏️ Exercice 4.3.a – Simuler une panne worker

```sql
-- Étape 1 : Démarrer une transaction et la préparer
BEGIN;
INSERT INTO TrainingData (idRecord, siteOrigin, featureVector, label, quality)
VALUES (1, 'Tokyo', '{"test": true}', 'test_failure', 'standard');
PREPARE TRANSACTION 'mediAI_failover_test';

-- Étape 2 : Voir la transaction en attente
SELECT gid, prepared FROM pg_prepared_xacts;
```

Maintenant, dans un autre terminal, arrêtez un worker :

```bash
# Simuler une panne du worker Tokyo
docker stop citus_worker3

# Revenir dans psql et observer
```

```sql
-- Étape 3 : Tenter le COMMIT (va-t-il réussir ou échouer ?)
COMMIT PREPARED 'mediAI_failover_test';
```

**Question 4.3.a** : Qu'est-il arrivé lors du COMMIT après la panne du worker ? Comment le 2PC protège-t-il les données dans ce cas ?

> _______________________________________________

```bash
# Redémarrer le worker
docker start citus_worker3
```

#### ✏️ Exercice 4.3.b – Questions de synthèse

**Question 4.3.b.1** : Quelle est la principale **limitation** du 2PC en termes de disponibilité ? (Hint : que se passe-t-il si le coordinator tombe en panne en Phase 2 ?)

> _______________________________________________

**Question 4.3.b.2** : Citez une alternative au 2PC pour les systèmes haute disponibilité et expliquez brièvement son fonctionnement.

> _______________________________________________

**Question 4.3.b.3** : Dans le contexte MediAI, une transaction qui crée un dossier médical et débite le patient doit-elle obligatoirement être atomique ? Justifiez en termes métier.

> _______________________________________________

---

## 📊 Partie 5 – Bonus : Analyse de performance (hors barème)

### 5.1 – Comparer les plans d'exécution

```sql
-- Requête sans clé de distribution dans le WHERE (scan global)
EXPLAIN (ANALYZE, VERBOSE)
SELECT * FROM Patients WHERE name = 'Alice Dupont';

-- Requête avec clé de distribution (pruning)
EXPLAIN (ANALYZE, VERBOSE)
SELECT * FROM Patients WHERE country = 'France' AND name = 'Alice Dupont';
```

**Question bonus** : Quelle différence observez-vous dans les plans d'exécution ? Combien de shards sont scannés dans chaque cas ?

> _______________________________________________

### 5.2 – Monitoring du cluster

```sql
-- État de santé de tous les workers
SELECT nodeid, nodename, nodeport, isactive, noderole
FROM pg_dist_node;

-- Distribution des shards par worker
SELECT p.nodename, COUNT(*) AS nb_shards
FROM pg_dist_shard_placement p
GROUP BY p.nodename
ORDER BY nb_shards DESC;

-- Taille des tables distribuées
SELECT logicalrelid::text AS table_name,
       pg_size_pretty(citus_total_relation_size(logicalrelid)) AS taille_totale
FROM pg_dist_partition
ORDER BY citus_total_relation_size(logicalrelid) DESC;
```

> ```
> [VOS RÉSULTATS]
> ```

---

## 📋 Récapitulatif à rendre

Complétez ce tableau avant de soumettre votre TP :

| Exercice | Statut | Points obtenus |
|----------|--------|----------------|
| 1.1 – Lancement cluster | ☐ Fait / ☐ Partiel / ☐ Non fait | ___ / 3 |
| 1.2 – Enregistrement workers | ☐ Fait / ☐ Partiel / ☐ Non fait | ___ / 3 |
| 1.3 – Chargement données | ☐ Fait / ☐ Partiel / ☐ Non fait | ___ / 4 |
| 2.1 – Fragmentation horizontale | ☐ Fait / ☐ Partiel / ☐ Non fait | ___ / 10 |
| 2.2 – Fragmentation verticale | ☐ Fait / ☐ Partiel / ☐ Non fait | ___ / 10 |
| 2.3 – Fragmentation hybride | ☐ Fait / ☐ Partiel / ☐ Non fait | ___ / 10 |
| 3.1 – Requête profil patient | ☐ Fait / ☐ Partiel / ☐ Non fait | ___ / 10 |
| 3.2 – Requête agrégée multi-sites | ☐ Fait / ☐ Partiel / ☐ Non fait | ___ / 10 |
| 3.3 – Requête financière | ☐ Fait / ☐ Partiel / ☐ Non fait | ___ / 10 |
| 4.1 – Théorie 2PC | ☐ Fait / ☐ Partiel / ☐ Non fait | ___ / 5 |
| 4.2 – Simulation 2PC SQL | ☐ Fait / ☐ Partiel / ☐ Non fait | ___ / 15 |
| 4.3 – Gestion défaillances | ☐ Fait / ☐ Partiel / ☐ Non fait | ___ / 10 |
| **TOTAL** | | ___ / 100 |

---

*⭐ Bon TP ! – Équipe pédagogique ENSTA 3A*
